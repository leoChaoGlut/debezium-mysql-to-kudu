package personal.leo.debezium_to_kudu.worker;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import personal.leo.debezium_to_kudu.constants.ZkPath;

import javax.annotation.PostConstruct;

@Service
public class Worker {
    @Autowired
    CuratorFramework curator;

    @PostConstruct
    private void postConstruct() {
        final TreeCache treeCache = TreeCache.newBuilder(curator, ZkPath.instances).build();
        treeCache.getListenable().addListener((client, event) -> {
            final ChildData data = event.getData();
            if (data == null) {
                return;
            }
            final String path = data.getPath();
            if (StringUtils.equals(path, ZkPath.instances)) {
                return;
            }
            final String subPath = StringUtils.substringAfter(path, ZkPath.instances);
        });
    }
}
