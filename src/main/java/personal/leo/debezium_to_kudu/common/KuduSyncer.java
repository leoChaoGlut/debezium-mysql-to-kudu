package personal.leo.debezium_to_kudu.common;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
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
public class KuduSyncer {
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String[] datePatterns = {
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.getPattern(),
            DEFAULT_DATE_PATTERN,
    };
    private final KuduClient kuduClient;
    private final KuduSession session;
    private final KuduTable kuduTable;

    private final String masterAddresses;
    /**
     * TODO 需要非常明确topic和kuduTable的关系,否则可能出现数据错乱,需要拿kudu表名与topics.regex进行校验
     */
    private final String kuduTableName;
    @Getter
    private final int maxBatchSize;
    private final boolean logEnabled;
    private final Map<String, ColumnSchema> kuduColumnNameMapKuduColumn;
    private final SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_PATTERN);
    private final String srcTableIdRegex;

    public KuduSyncer(KuduProps kuduProps, Task task) throws KuduException {
        masterAddresses = kuduProps.getMasterAddresses();
        kuduTableName = task.getKuduTableName();
        srcTableIdRegex = task.getSrcTableIdRegex();

        maxBatchSize = kuduProps.getMaxBatchSize() + 10;//随便加几个size,防止kudu 报 超出maxBatchSize的错误
        logEnabled = kuduProps.isLogEnabled();

        final String zoneId = kuduProps.getZoneId();
        sdf.setTimeZone(TimeZone.getTimeZone(zoneId));

        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses).build();

        kuduTable = kuduClient.openTable(kuduTableName);

        session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(maxBatchSize);

        kuduColumnNameMapKuduColumn = kuduTable.getSchema().getColumns().stream().collect(Collectors.toMap(columnSchema -> columnSchema.getName().toLowerCase(), Function.identity()));
        log.info("KuduSyncer : " + toString());
    }


    public Operation createOperation(Struct payload) {
        final Struct after = payload.getStruct(PayloadKeys.after);
        final Struct before = payload.getStruct(PayloadKeys.before);

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
        return Pattern.matches(srcTableIdRegex, srcTableId);
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
                Timestamp timestamp;
                try {
                    timestamp = new Timestamp(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    try {
                        final Date date = DateUtils.parseDate(value, datePatterns);
                        final String convertedDateStr = sdf.format(date);
                        final Date convertedDate = DateUtils.parseDate(convertedDateStr, datePatterns);
                        timestamp = new Timestamp(convertedDate.getTime());
                    } catch (ParseException ex) {
                        throw new RuntimeException("parse date error:" + value);
                    }
                }

                row.addTimestamp(kuduColumnName, timestamp);
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
            OperationResponse resp = resps.get(0);
            if (resp.hasRowError()) {
                throw new RuntimeException("sync to kudu error:" + resp.getRowError());
            }
        }
        watch.stop();
        if (logEnabled) {
            log.info("sync: " + operations.size() + ",to " + kuduTableName + ",spend: " + watch);
        }
        operations.clear();
    }

    public void stop() throws KuduException {
        session.close();
        kuduClient.close();
    }

    @Override
    public String toString() {
        return "KuduSyncer{" +
                "masterAddresses='" + masterAddresses + '\'' +
                ", kuduTableName='" + kuduTableName + '\'' +
                ", maxBatchSize=" + maxBatchSize +
                ", logEnabled=" + logEnabled +
                ", kuduColumnNameMapKuduColumn=" + kuduColumnNameMapKuduColumn +
                '}';
    }
}
