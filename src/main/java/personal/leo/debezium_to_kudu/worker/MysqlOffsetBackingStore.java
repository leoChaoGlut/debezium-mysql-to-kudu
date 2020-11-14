package personal.leo.debezium_to_kudu.worker;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

@Slf4j
public class MysqlOffsetBackingStore extends MemoryOffsetBackingStore {

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
    }

    @Override
    public synchronized void start() {
        super.start();
//        log.info("Starting FileOffsetBackingStore with file {}", file);
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

    }

    private void load() {
//        try (SafeObjectInputStream is = new SafeObjectInputStream(Files.newInputStream(file.toPath()))) {
//            Object obj = is.readObject();
//            if (!(obj instanceof HashMap))
//                throw new ConnectException("Expected HashMap but found " + obj.getClass());
//            Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
//            data = new HashMap<>();
//            for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
//                ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
//                ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
//                data.put(key, value);
//            }
//        } catch (NoSuchFileException | EOFException e) {
//            // NoSuchFileException: Ignore, may be new.
//            // EOFException: Ignore, this means the file was missing or corrupt
//        } catch (IOException | ClassNotFoundException e) {
//            throw new ConnectException(e);
//        }
    }
}
