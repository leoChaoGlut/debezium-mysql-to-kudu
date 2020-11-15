package personal.leo.debezium_to_kudu.debezium.mysql;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import personal.leo.debezium_to_kudu.constants.PropKeys;
import personal.leo.debezium_to_kudu.mapper.OffsetMapper;
import personal.leo.debezium_to_kudu.mapper.po.Offset;
import personal.leo.debezium_to_kudu.spring.ApplicationContextHolder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static personal.leo.debezium_to_kudu.utils.MybatisUtils.assertOperationSuccess;

public class MysqlOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(MysqlOffsetBackingStore.class);
    private WorkerConfig config;

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        this.config = config;
    }

    @Override
    public synchronized void start() {
        super.start();
        log.info("start loading offset");
        load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        log.info("Stopped FileOffsetBackingStore");
    }

    @Override
    protected void save() {
        final OffsetMapper offsetMapper = ApplicationContextHolder.getBean(OffsetMapper.class);
        final String instanceId = (String) config.originals().get(PropKeys.databaseServerName);
        for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
            byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
            byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
            final Offset offset = new Offset()
                    .setTask_id(instanceId)
                    .setKey(key)
                    .setValue(value);
            assertOperationSuccess(() -> offsetMapper.upsert(offset));
        }
    }

    private void load() {
        final OffsetMapper offsetMapper = ApplicationContextHolder.getBean(OffsetMapper.class);
        final String instanceId = (String) config.originals().get(PropKeys.databaseServerName);
        final List<Offset> offsets = offsetMapper.select(instanceId);
        for (Offset offset : offsets) {
            ByteBuffer key = (offset.getKey() != null) ? ByteBuffer.wrap(offset.getKey()) : null;
            ByteBuffer value = (offset.getValue() != null) ? ByteBuffer.wrap(offset.getValue()) : null;
            data.put(key, value);
        }
    }
}
