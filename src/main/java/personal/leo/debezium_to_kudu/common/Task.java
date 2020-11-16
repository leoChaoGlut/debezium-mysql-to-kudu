package personal.leo.debezium_to_kudu.common;

import com.alibaba.fastjson.JSON;
import io.debezium.relational.history.AbstractDatabaseHistory;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import personal.leo.debezium_to_kudu.constants.DebeziumConnectorType;
import personal.leo.debezium_to_kudu.constants.DefaultValues;
import personal.leo.debezium_to_kudu.mapper.po.TaskPO;

import javax.validation.constraints.NotBlank;
import java.util.Map;
import java.util.Properties;

import static personal.leo.debezium_to_kudu.constants.Separator.DOT;

@Accessors(chain = true)
@Getter
@Setter
@EqualsAndHashCode(of = {"databaseHostname", "databasePort", "databaseUser"})
@ToString
public class Task {
    @NonNull
    private DebeziumConnectorType debeziumConnectorType;

    private int offsetFlushIntervalMs = DefaultValues.offsetFlushIntervalMs;
    private Class<? extends MemoryOffsetBackingStore> offsetStorage = DefaultValues.offsetStorage;
    private Class<? extends AbstractDatabaseHistory> databaseHistory = DefaultValues.databaseHistory;

    private int maxBatchSize = DefaultValues.maxBatchSize;
    private int maxQueueSize = DefaultValues.maxQueueSize;

    @NotBlank
    private String databaseHostname;
    private int databasePort = DefaultValues.databasePort;
    @NotBlank
    private String databaseUser;
    @NotBlank
    private String databasePassword;
    /**
     * 由数据库自增主键生成
     */
    private int databaseServerId;

    @NotBlank
    private String databaseIncludeList;
    @NotBlank
    private String tableIncludeList;
    @NotBlank
    private String kuduTableName;

    private Map<String, Object> extra;

    public String getDatabaseServerName() {
        return databaseHostname + DOT + databasePort + DOT + databaseUser;
    }

    public String id() {
        return getDatabaseServerName();
    }

    public Properties toProps() {
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", debeziumConnectorType.getConnectorClass());
        props.setProperty("offset.storage", debeziumConnectorType.getOffsetStorage());
        props.setProperty("database.history", debeziumConnectorType.getDatabaseHistory());

        props.setProperty("offset.flush.interval.ms", String.valueOf(offsetFlushIntervalMs));
        props.setProperty("max.batch.size", String.valueOf(maxBatchSize));
        props.setProperty("max.queue.size", String.valueOf(maxQueueSize));

        props.setProperty("database.hostname", databaseHostname);
        props.setProperty("database.port", String.valueOf(databasePort));
        props.setProperty("database.user", databaseUser);
        props.setProperty("database.password", databasePassword);

        props.setProperty("database.include.list", databaseIncludeList);
        props.setProperty("table.include.list", tableIncludeList);

        props.setProperty("database.server.id", String.valueOf(databaseServerId));
        props.setProperty("database.server.name", getDatabaseServerName());

        //snapshop和binlogreader时区有冲突,全量同步通过jdbc,不能通过initial模式,否则时间会乱
        props.setProperty("database.serverTimezone", DefaultValues.databaseServerTimezone);
        props.setProperty("snapshot.mode", DefaultValues.snapshotMode.getValue());
        props.setProperty("decimal.handling.mode", "string");

        return props;
    }


    public static Task of(TaskPO taskPO) {
        final Task task = JSON.parseObject(taskPO.getJson(), Task.class);
        task.setDatabaseServerId(taskPO.getTask_num());
        return task;
    }

    public enum State {
        ACTIVE, INACTIVE
    }


}
