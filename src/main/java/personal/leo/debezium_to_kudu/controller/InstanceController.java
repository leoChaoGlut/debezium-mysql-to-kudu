package personal.leo.debezium_to_kudu.controller;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.web.bind.annotation.*;
import personal.leo.debezium_to_kudu.common.Instance;
import personal.leo.debezium_to_kudu.constants.ZkPath;
import personal.leo.debezium_to_kudu.mapper.InstanceMapper;
import personal.leo.debezium_to_kudu.mapper.po.InstancePO;

@RestController
@RequestMapping("instance")
public class InstanceController {
    @Autowired
    PlatformTransactionManager txManager;
    @Autowired
    InstanceMapper instanceMapper;
    @Autowired
    CuratorFramework curator;

    @PostMapping("create")
    public InstancePO create(@RequestBody Instance instance) throws Exception {
//        TODO 校验非空
//        TODO 校验非空
//        TODO 校验非空
        final InstancePO instancePO = InstancePO.of(instance);

        final TransactionStatus status = txManager.getTransaction(new DefaultTransactionDefinition());

        try {
            instanceMapper.insert(instancePO);
            instance.setDatabaseServerId(instancePO.getInstance_num());

            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath(ZkPath.instances + "/" + instance.getDatabaseServerName());

            txManager.commit(status);
            return instancePO;
        } catch (Exception e) {
            txManager.commit(status);
            throw e;
        }
    }

    @GetMapping("get")
    public Instance get(String instanceId) {
        final InstancePO instancePO = instanceMapper.select(instanceId);
        return Instance.of(instancePO);
    }


}
