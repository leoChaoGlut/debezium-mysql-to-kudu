package personal.leo.debezium_to_kudu.constants;

public interface PayloadKeys {
    String payload = "payload";
    String before = "before";
    String after = "after";
    String op = "op";
    String source = "source";
    String db = "db";
    String table = "table";

}
