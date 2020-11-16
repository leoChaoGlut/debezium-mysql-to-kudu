package personal.leo.debezium_to_kudu.kudu;

import lombok.Getter;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KuduManager {
    @Getter
    private static KuduClient kuduClient;

    private static final Map<String, KuduTable> kuduTableNameMapKuduTable = Collections.synchronizedMap(new HashMap<>());

    public static void init(String masterAddresses) {
        if (kuduClient == null) {
            kuduClient = new KuduClient.KuduClientBuilder(masterAddresses).build();
        } else {
            throw new RuntimeException("kudu client has been initialized");
        }
    }

    public static KuduTable getTable(String kuduTableName) throws KuduException {
        KuduTable kuduTable = kuduTableNameMapKuduTable.get(kuduTableName);
        if (kuduTable == null) {
            kuduTable = kuduClient.openTable(kuduTableName);
            kuduTableNameMapKuduTable.put(kuduTableName, kuduTable);
        }
        return kuduTable;
    }
}
