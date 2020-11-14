package personal.leo.debezium_to_kudu.config;

import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

@Configuration
public class TaskExecutorConfig {

    @Bean
    public TaskExecutor taskExecutor() {
        final int corePoolSize = Runtime.getRuntime().availableProcessors();
        return new TaskExecutorBuilder()
                .corePoolSize(corePoolSize)
                .maxPoolSize(corePoolSize * 100)
                .queueCapacity(corePoolSize * 200)
                .build()
                ;
    }

}
