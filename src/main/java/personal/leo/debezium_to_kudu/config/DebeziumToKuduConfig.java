package personal.leo.debezium_to_kudu.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import personal.leo.debezium_to_kudu.config.props.EmailProps;
import personal.leo.debezium_to_kudu.config.props.KuduProps;

@Configuration
public class DebeziumToKuduConfig {

    @Bean
    @ConfigurationProperties("kudu")
    public KuduProps kuduProps() {
        return new KuduProps();
    }

    @Bean
    @ConfigurationProperties("email")
    public EmailProps emailProps() {
        return new EmailProps();
    }
}
