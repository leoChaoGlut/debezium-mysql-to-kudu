package personal.leo.debezium_to_kudu.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;
import personal.leo.debezium_to_kudu.config.props.KuduProps;
import personal.leo.debezium_to_kudu.kudu.KuduManager;
import personal.leo.debezium_to_kudu.utils.CommonUtils;

@Component
public class ApplicationContextHolder implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    @Value("${server.port}")
    int serverPort;
    @Autowired
    KuduProps kuduProps;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        ApplicationContextHolder.applicationContext = applicationContext;
        CommonUtils.setPort(serverPort);
        KuduManager.init(kuduProps.getMasterAddresses());
    }

    public static <T> T getBean(Class<T> clazz) {
        return applicationContext.getBean(clazz);
    }

}
