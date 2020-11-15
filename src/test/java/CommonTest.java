import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class CommonTest {
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

// Create the engine with this configuration ...
        final DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((records, recordCommitter) -> {
                    AtomicInteger i = new AtomicInteger(0);
                    System.out.println(records.size());
                    for (ChangeEvent<String, String> record : records) {
                        final String value = record.value();
                        final JSONObject jsonObject = JSON.parseObject(value);
                        final JSONObject payload = jsonObject.getJSONObject("payload");
                        final JSONObject after = payload.getJSONObject("after");
                        if (after != null) {
                            i.getAndIncrement();
                            System.out.println(after);
                            System.out.println("==================");
                            if (i.get() > 1) {
//                                throw new RuntimeException("1");
                            }
                        }
                    }
                    for (ChangeEvent<String, String> record : records) {
                        recordCommitter.markProcessed(record);
                    }
                    recordCommitter.markBatchFinished();
                })
                .build();
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            engine.close();
        }
// Engine is stopped when the main code is finished
    }

    @Test
    public void test() {
        String json = "{\"source\":{\"server\":\"my-app-connector\"},\"position\":{\"file\":\"master.000003\",\"pos\":337062046,\"snapshot\":true},\"ddl\":\"SET character_set_server=utf8, collation_server=utf8_general_ci;\"}\n";
        final JSONObject jsonObject = JSON.parseObject(json);
        System.out.println(jsonObject.getJSONObject("source").getString("server"));
    }

    @Test
    public void test1() {
        String regex = "^(test)\\.(t10_[0-9]+)$";
        final StopWatch watch = StopWatch.createStarted();
        System.out.println(Pattern.matches(regex, "test.t10_1"));
        System.out.println(Pattern.matches(regex, "test.t10_"));
        System.out.println(Pattern.matches(regex, "test.t10_11111111"));
        System.out.println(Pattern.matches(regex, "test1.t101"));
        System.out.println(Pattern.matches(regex, "test1.t111"));
        watch.stop();
        System.out.println(watch);
    }


}
