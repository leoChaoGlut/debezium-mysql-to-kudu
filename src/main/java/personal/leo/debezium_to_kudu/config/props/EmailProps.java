package personal.leo.debezium_to_kudu.config.props;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

@Accessors(chain = true)
@Getter
@Setter
@ToString
public class EmailProps {
    private String hostName;
    private String from;
    private String user;
    private String password;
    private List<String> tos;
}
