package personal.leo.debezium_to_kudu.spring;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import personal.leo.debezium_to_kudu.constants.ZkPath;
import personal.leo.debezium_to_kudu.utils.CommonUtils;

@Slf4j
@Component
public class ApplicationReadyEventListener implements ApplicationListener<ApplicationReadyEvent> {
    @Autowired
    CuratorFramework curator;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        log.info("ApplicationReadyEvent: " + CommonUtils.getThisServerId());
        try {
            curator.create().withMode(CreateMode.EPHEMERAL).forPath(ZkPath.workers + "/" + CommonUtils.getThisServerId());
//            TODO 拿分布式锁,然后获取数据库里所有的instance,上传到zk
//            TODO 拿分布式锁,然后获取数据库里所有的instance,上传到zk
//            TODO 拿分布式锁,然后获取数据库里所有的instance,上传到zk
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
