package personal.leo.debezium_to_kudu.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ZooKeeperConfig {

    @Value("${zkServers}")
    String zkServers;

    @Bean
    public CuratorFramework curator() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework curator = CuratorFrameworkFactory.newClient(zkServers, retryPolicy);
        curator.start();
        return curator;
    }

}
