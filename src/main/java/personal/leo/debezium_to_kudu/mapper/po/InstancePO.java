package personal.leo.debezium_to_kudu.mapper.po;

import com.alibaba.fastjson.JSON;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import personal.leo.debezium_to_kudu.common.Instance;

import java.util.Date;

@Accessors(chain = true)
@Getter
@Setter
@EqualsAndHashCode(of = "instance_id")
public class InstancePO {
    private int instance_num;
    private String instance_id;
    private String json;
    private Date create_time;

    public static InstancePO of(Instance instance) {
        return new InstancePO()
                .setInstance_num(instance.getDatabaseServerId())
                .setInstance_id(instance.getDatabaseServerName())
                .setJson(JSON.toJSONString(instance));
    }
}
