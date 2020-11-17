package personal.leo.debezium_to_kudu.kudu;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import personal.leo.debezium_to_kudu.common.Task;
import personal.leo.debezium_to_kudu.config.props.KuduProps;
import personal.leo.debezium_to_kudu.constants.OperationType;
import personal.leo.debezium_to_kudu.constants.PayloadKeys;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class KuduSyncer implements AutoCloseable {

    private final KuduSession session;
    private final KuduTable kuduTable;

    /**
     * TODO 需要非常明确topic和kuduTable的关系,否则可能出现数据错乱,需要拿kudu表名与topics.regex进行校验
     */
    private final String kuduTableName;
    @Getter
    private final int maxBatchSize;
    private final boolean logEnabled;
    private final Map<String, ColumnSchema> kuduColumnNameMapKuduColumn;
    private final SimpleDateFormat dateFormat;
    private final String tableIncludeList;
    private final TimeZone timeZone;

    public KuduSyncer(KuduProps kuduProps, Task task) throws KuduException {
        kuduTableName = task.getKuduTableName();
        tableIncludeList = task.getTableIncludeList();

        maxBatchSize = kuduProps.getMaxBatchSize() + 10;//随便加几个size,防止kudu 报 超出maxBatchSize的错误
        logEnabled = kuduProps.isLogEnabled();

        timeZone = TimeZone.getTimeZone(kuduProps.getZoneId());
        dateFormat = new SimpleDateFormat(DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern());
        dateFormat.setTimeZone(timeZone);

        kuduTable = KuduManager.getTable(kuduTableName);

        session = KuduManager.getKuduClient().newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(maxBatchSize);

        kuduColumnNameMapKuduColumn = kuduTable.getSchema().getColumns().stream().collect(Collectors.toMap(columnSchema -> columnSchema.getName().toLowerCase(), Function.identity()));
        log.info("KuduSyncer : " + toString());
    }


    public Operation createOperation(Struct after, Struct before, Struct payload) {
        final Operation operation;
        final String op = payload.getString(PayloadKeys.op);
        final OperationType operationType = OperationType.of(op);

        final Struct fields;
        switch (operationType) {
            case CREATE:
            case UPDATE:
                fields = after;
                operation = kuduTable.newUpsert();
                break;
            case DELETE:
                fields = before;
                operation = kuduTable.newDelete();
                break;
            default:
                throw new RuntimeException("not supported:" + operationType);
        }

        boolean hasAddData = false;
        for (Field field : fields.schema().fields()) {
            final String srcColumnName = field.name().toLowerCase();
            final Object srcColumnValue = fields.get(field);
            final ColumnSchema kuduColumn = kuduColumnNameMapKuduColumn.get(srcColumnName);
            if (kuduColumn == null) {
//                throw new RuntimeException("no column found for : " + srcColumnName);
//                TODO 发现不存在的列,可能源库出现变更,需要发邮件通知
            } else {
                fillRow(kuduColumn, srcColumnValue, operation.getRow());
//                row.addObject(kuduColumn.getName(), srcColumnValue);
                if (!hasAddData) {
                    hasAddData = true;
                }
            }
        }

        if (hasAddData) {
            return operation;
        } else {
            throw new RuntimeException("no column value be set,please confirm the topics are match the kudu table: " + kuduTableName);
        }
    }

    public boolean accept(String srcTableId) {
        return Pattern.matches(tableIncludeList, srcTableId);
    }

    /**
     * copy from org.apache.kudu.client.PartialRow.addObject(int, java.lang.Object)
     */
    private void fillRow(ColumnSchema kuduColumn, Object srcColumnValue, PartialRow row) {
        final String kuduColumnName = kuduColumn.getName();
        final Type kuduColumnType = kuduColumn.getType();
        if (srcColumnValue == null) {
            row.addObject(kuduColumnName, null);
            return;
        }

        final String value = String.valueOf(srcColumnValue);

        switch (kuduColumnType) {
            case BOOL:
                row.addBoolean(kuduColumnName, Boolean.parseBoolean(value));
                break;
            case INT8:
                row.addByte(kuduColumnName, Byte.parseByte(value));
                break;
            case INT16:
                row.addShort(kuduColumnName, Short.parseShort(value));
                break;
            case INT32:
                row.addInt(kuduColumnName, Integer.parseInt(value));
                break;
            case INT64:
                row.addLong(kuduColumnName, Long.parseLong(value));
                break;
            case UNIXTIME_MICROS:
//                TODO date类型转换会出现1970-01-01
                try {
                    row.addTimestamp(kuduColumnName, new Timestamp(Long.parseLong(value)));
                } catch (NumberFormatException e) {
                    try {
                        final Date date = dateFormat.parse(value);
                        row.addTimestamp(kuduColumnName, new Timestamp(date.getTime()));
                    } catch (ParseException ex) {
                        throw new RuntimeException("parse date error:" + value);
                    }
                }
                break;
            case FLOAT:
                row.addFloat(kuduColumnName, Float.parseFloat(value));
                break;
            case DOUBLE:
                row.addDouble(kuduColumnName, Double.parseDouble(value));
                break;
            case STRING:
                row.addString(kuduColumnName, value);
                break;
            case BINARY:
                row.addBinary(kuduColumnName, value.getBytes(StandardCharsets.UTF_8));
                break;
            case DECIMAL:
                // required "decimal.handling.mode": "string"
                row.addDecimal(kuduColumnName, new BigDecimal(value));
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + kuduColumnType);
        }
    }


    public void syncAndClear(List<Operation> operations) throws KuduException {
        final StopWatch watch = StopWatch.createStarted();
        if (operations.isEmpty()) {
            return;
        }

        for (Operation operation : operations) {
            session.apply(operation);
        }

        final List<OperationResponse> resps = session.flush();
        if (resps.size() > 0) {
            for (OperationResponse resp : resps) {
                if (resp.hasRowError()) {
                    final RowError rowError = resp.getRowError();
                    final Status errorStatus = rowError.getErrorStatus();
                    if (errorStatus.isNotFound()) {
                        log.error("ignore not found error: " + rowError);
                    } else {
                        throw new RuntimeException("sync to kudu error: " + rowError);
                    }
                }
            }
        }
        watch.stop();
        if (logEnabled) {
            log.info("sync: " + operations.size() + ",to " + kuduTableName + ",spend: " + watch);
        }
        operations.clear();
    }


    @Override
    public void close() throws Exception {
        session.close();
    }
}
