package personal.leo.debezium_to_kudu.mapper.po;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Date;

@Accessors(chain = true)
@Getter
@Setter
public class History {
    private String instance_id;
    private String json;
    private Date create_time;
}
