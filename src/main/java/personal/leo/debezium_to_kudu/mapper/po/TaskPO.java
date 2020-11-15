package personal.leo.debezium_to_kudu.mapper.po;

import com.alibaba.fastjson.JSON;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import personal.leo.debezium_to_kudu.common.Task;

import java.util.Date;

@Accessors(chain = true)
@Getter
@Setter
@EqualsAndHashCode(of = "task_id")
public class TaskPO {
    private int task_num;
    private String task_id;
    private String json;
    private Date create_time;
    private Task.State state;
    private String worker;
    private Date update_time;

    public static TaskPO of(Task task) {
        return new TaskPO()
                .setTask_num(task.getDatabaseServerId())
                .setTask_id(task.getDatabaseServerName())
                .setJson(JSON.toJSONString(task));
    }
}
