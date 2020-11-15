package personal.leo.debezium_to_kudu.common;

import org.apache.kudu.client.KuduException;
import personal.leo.debezium_to_kudu.config.props.KuduProps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KuduSyncerPool {
    private static final Map<String, KuduSyncer> kuduTableNameMapKuduSyncer = Collections.synchronizedMap(new HashMap<>());

    public static KuduSyncer get(KuduProps kuduProps, Task task) throws KuduException {
        KuduSyncer kuduSyncer = kuduTableNameMapKuduSyncer.get(task.getKuduTableName());
        if (kuduSyncer == null) {
            kuduSyncer = new KuduSyncer(kuduProps, task);
            kuduTableNameMapKuduSyncer.put(task.getKuduTableName(), kuduSyncer);
        }
        return kuduSyncer;
    }
}
