import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Common1Test {
    public static void main(String[] args) throws IOException {
        // Define the configuration for the Debezium Engine with MySQL connector...
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "D:\\git\\github\\debezium-to-kudu\\src\\test\\resources\\test\\offsets5.dat");
        props.setProperty("offset.flush.interval.ms", "3000");
        props.setProperty("max.batch.size", "5120");
        props.setProperty("max.queue.size", "20480");

        /* begin connector properties */
        props.setProperty("database.hostname", "hdp04");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", "1");
        props.setProperty("snapshot.mode", "schema_only");
        props.setProperty("database.include.list", "test");
        props.setProperty("table.include.list", "test\\.t10");
        props.setProperty("database.server.id", "1");
        props.setProperty("database.server.name", "my-app-connector1");
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename", "D:\\git\\github\\debezium-to-kudu\\src\\test\\resources\\test\\dbhistory5.dat");
        EmbeddedEngine.BuilderImpl builder = new EmbeddedEngine.BuilderImpl();
        final EmbeddedEngine engine = builder.using(props)
                .notifying((records, committer) -> {
                    for (SourceRecord record : records) {
                        final Struct struct = (Struct) (record.value());
                        final Struct after = struct.getStruct("after");
                        final Struct before = struct.getStruct("before");
                        for (Field field : after.schema().fields()) {
                            System.out.println(field + "=" + after.get(field));
                        }
//                        final byte[] key = keyConverter.fromConnectData("test", record.keySchema(), record.key());
//                        final byte[] value = keyConverter.fromConnectData("test", record.valueSchema(), record.value());
//                        System.out.println(new String(key, UTF_8));
//                        System.out.println(new String(value, UTF_8));
                        System.out.println("=================");
                    }
                    for (SourceRecord record : records) {
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .build();
        engine.stop();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
//// Create the engine with this configuration ...
//        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
//                .using(props)
//                .notifying((records, recordCommitter) -> {
//                    AtomicInteger i = new AtomicInteger(0);
//                    System.out.println(records.size());
//                    for (ChangeEvent<String, String> record : records) {
//                        final String value = record.value();
//                        final JSONObject jsonObject = JSON.parseObject(value);
//                        final JSONObject payload = jsonObject.getJSONObject("payload");
//                        final JSONObject after = payload.getJSONObject("after");
//                        if (after != null) {
//                            i.getAndIncrement();
//                            System.out.println(after);
//                            System.out.println("==================");
//                            if (i.get() > 1) {
////                                throw new RuntimeException("1");
//                            }
//                        }
//                    }
//                    for (ChangeEvent<String, String> record : records) {
//                        recordCommitter.markProcessed(record);
//                    }
//                    recordCommitter.markBatchFinished();
//                })
//                .build();
//        try {
//            ExecutorService executor = Executors.newSingleThreadExecutor();
//            executor.execute(engine);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            engine.close();
//        }
// Engine is stopped when the main code is finished
    }


}
