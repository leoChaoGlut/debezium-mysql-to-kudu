package personal.leo.debezium_to_kudu.common;

import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kudu.client.Operation;
import personal.leo.debezium_to_kudu.constants.PayloadKeys;
import personal.leo.debezium_to_kudu.kudu.KuduSyncer;
import personal.leo.debezium_to_kudu.utils.StructUtils;

import java.util.ArrayList;
import java.util.List;

import static personal.leo.debezium_to_kudu.constants.Separator.DOT;

@Slf4j
public class MsgConsumer implements DebeziumEngine.ChangeConsumer<SourceRecord> {

    private final List<Operation> operations;
    private final KuduSyncer kuduSyncer;

    public MsgConsumer(KuduSyncer kuduSyncer) {
        this.kuduSyncer = kuduSyncer;
        operations = new ArrayList<>(kuduSyncer.getMaxBatchSize());
    }

    @Override
    public void handleBatch(List<SourceRecord> records, DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {
        try {
            for (SourceRecord record : records) {
                final Struct payload = (Struct) record.value();
                if (payload == null) {
                    continue;
                }
                final Struct source = payload.getStruct(PayloadKeys.source);
                if (source == null) {
                    continue;
                }
                final String srcTableId = source.getString(PayloadKeys.db) + DOT + source.getString(PayloadKeys.table);
                final boolean accept = kuduSyncer.accept(srcTableId);
                if (accept) {
                    final Struct after = StructUtils.getStruct(payload, PayloadKeys.after);
                    final Struct before = StructUtils.getStruct(payload, PayloadKeys.before);
                    if (after == null && before == null) {
                        continue;
                    }
                    final Operation operation = kuduSyncer.createOperation(after, before, payload);
                    operations.add(operation);
                    if (operations.size() >= kuduSyncer.getMaxBatchSize()) {
                        kuduSyncer.syncAndClear(operations);
                    }
                }
            }

            if (!operations.isEmpty()) {
                kuduSyncer.syncAndClear(operations);
            }

            for (SourceRecord record : records) {
                committer.markProcessed(record);
            }

            committer.markBatchFinished();
        } catch (Exception e) {
            log.error("msgConsumer error", e);
            throw new RuntimeException(e);
        }
    }
}
