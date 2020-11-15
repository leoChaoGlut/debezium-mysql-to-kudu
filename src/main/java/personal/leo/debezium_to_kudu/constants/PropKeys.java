package personal.leo.debezium_to_kudu.constants;

public interface PropKeys {
    String masterAddresses = "masterAddresses";
    String kuduTableName = "kuduTableName";
    String maxBatchSize = "maxBatchSize";
    String logEnabled = "logEnabled";


    String zoneId = "zoneId";

    String databaseHistoryConnectorId = "database.history.connector.id";
    String databaseServerName = "database.server.name";
}
