package personal.leo.debezium_to_kudu.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.util.concurrent.ScheduledThreadPoolExecutor;

@Configuration
@EnableScheduling
public class ScheduleConfig implements SchedulingConfigurer {

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        final int schedulePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        taskRegistrar.setScheduler(new ScheduledThreadPoolExecutor(schedulePoolSize));
    }
}
