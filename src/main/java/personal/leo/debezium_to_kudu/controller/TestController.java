package personal.leo.debezium_to_kudu.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import personal.leo.debezium_to_kudu.debezium.mysql.MysqlDatabaseHistory;
import personal.leo.debezium_to_kudu.debezium.mysql.MysqlOffsetBackingStore;
import personal.leo.debezium_to_kudu.mapper.HistoryMapper;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("test")
public class TestController {
    @Autowired
    HistoryMapper historyMapper;

    @GetMapping
    public void test() {
        CompletableFuture.runAsync(() -> {
            // Define the configuration for the Debezium Engine with MySQL connector...
            final Properties props = new Properties();
            props.setProperty("name", "engine");
            props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
            props.setProperty("offset.storage", MysqlOffsetBackingStore.class.getName());
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
            props.setProperty("database.history", MysqlDatabaseHistory.class.getName());

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
                try {
                    engine.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @GetMapping("2")
    public int test2() {
        final int ss = historyMapper.count("ss");
        return ss;
    }
}
