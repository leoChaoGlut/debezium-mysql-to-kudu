package personal.leo.debezium_to_kudu.worker.constants;

public interface PayloadKeys {
    String payload = "payload";
    String before = "before";
    String after = "after";
    String op = "op";
}
