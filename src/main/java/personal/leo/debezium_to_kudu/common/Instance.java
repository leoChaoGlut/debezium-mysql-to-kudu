package personal.leo.debezium_to_kudu.common;

import com.alibaba.fastjson.JSON;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.history.AbstractDatabaseHistory;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import personal.leo.debezium_to_kudu.constants.DebeziumConnectorType;
import personal.leo.debezium_to_kudu.constants.DefaultValues;
import personal.leo.debezium_to_kudu.mapper.po.InstancePO;

import javax.validation.constraints.NotBlank;
import java.util.Map;
import java.util.Properties;

import static personal.leo.debezium_to_kudu.constants.Separator.DOT;

@Accessors(chain = true)
@Getter
@Setter
@EqualsAndHashCode(of = {"databaseHostname", "databasePort", "databaseUser"})
@ToString
public class Instance {
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
    private int databaseServerId;

    private MySqlConnectorConfig.SnapshotMode snapshotMode = DefaultValues.snapshotMode;
    @NotBlank
    private String databaseIncludeList;
    @NotBlank
    private String tableIncludeList;

    private Map<String, Object> extra;

    public String getDatabaseServerName() {
        return databaseHostname + DOT + databasePort + DOT + databaseUser;
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
        props.setProperty("snapshot.mode", snapshotMode.getValue());

        return props;
    }


    public static Instance of(InstancePO instancePO) {
        final Instance instance = JSON.parseObject(instancePO.getJson(), Instance.class);
        instance.setDatabaseServerId(instancePO.getInstance_num());
        return instance;
    }

}
