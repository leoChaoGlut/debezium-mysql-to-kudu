package personal.leo.debezium_to_kudu.constants;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.history.AbstractDatabaseHistory;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import personal.leo.debezium_to_kudu.debezium.mysql.MysqlDatabaseHistory;
import personal.leo.debezium_to_kudu.debezium.mysql.MysqlOffsetBackingStore;

public interface DefaultValues {
    int offsetFlushIntervalMs = 3000;
    int maxBatchSize = 5120;
    int maxQueueSize = 20480;
    MySqlConnectorConfig.SnapshotMode snapshotMode = MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY;
    int databasePort = 3306;
    boolean logEnabled = false;
    Class<? extends MemoryOffsetBackingStore> offsetStorage = MysqlOffsetBackingStore.class;
    Class<? extends AbstractDatabaseHistory> databaseHistory = MysqlDatabaseHistory.class;

    String zoneId = "GMT-8";
    String databaseServerTimezone = "GMT";
}
