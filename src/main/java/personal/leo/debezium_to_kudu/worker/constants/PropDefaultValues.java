package personal.leo.debezium_to_kudu.worker.constants;

public interface PropDefaultValues {
    String maxBatchSize = "10000";
    String onlySyncValueChangedColumns = "false";
    String logEnabled = "false";
    String sendDataToKuduTableNameTopic = "false";


    String emailHostName = "smtp.163.com";
    String emailFrom = "ypshengxian@163.com";
    String emailUser = "ypshengxian@163.com";
    String emailPassword = "YBYFNAGEYXJAGQMO";
    String emailTo = "liaochao@ypshengxian.com";

    String includeSchemaChanges = "false";
    String zoneId = "GMT+16";
}
