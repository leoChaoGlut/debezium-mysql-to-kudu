package personal.leo.debezium_to_kudu.constants;

public interface ZkPath {
    String root = "/debezium-to-kudu";
    String workers = root + "/workers";
    String instances = root + "/instances";
}
