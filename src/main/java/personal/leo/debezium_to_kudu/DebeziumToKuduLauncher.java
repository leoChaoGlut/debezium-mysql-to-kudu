package personal.leo.debezium_to_kudu;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DebeziumToKuduLauncher {
    public static void main(String[] args) {
        try {
            SpringApplication.run(DebeziumToKuduLauncher.class, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
