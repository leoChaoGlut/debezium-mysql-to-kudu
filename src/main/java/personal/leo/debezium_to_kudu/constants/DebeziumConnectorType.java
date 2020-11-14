package personal.leo.debezium_to_kudu.constants;

import io.debezium.connector.mysql.MySqlConnector;
import lombok.AllArgsConstructor;
import lombok.Getter;
import personal.leo.debezium_to_kudu.debezium.mysql.MysqlDatabaseHistory;
import personal.leo.debezium_to_kudu.debezium.mysql.MysqlOffsetBackingStore;

@Getter
@AllArgsConstructor
public enum DebeziumConnectorType {
    MYSQL(
            MySqlConnector.class.getName(),
            MysqlOffsetBackingStore.class.getName(),
            MysqlDatabaseHistory.class.getName()
    );

    private String connectorClass;
    private String offsetStorage;
    private String databaseHistory;

}
