package personal.leo.debezium_to_kudu.mapper.po;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Date;

@Accessors(chain = true)
@Getter
@Setter
public class Offset {
    private String task_id;
    private byte[] key;
    private byte[] value;
    private Date create_time;
}
