package personal.leo.debezium_to_kudu.config.props;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import personal.leo.debezium_to_kudu.constants.DefaultValues;

@Accessors(chain = true)
@Getter
@Setter
public class KuduProps {
    private String masterAddresses;
    private int maxBatchSize = DefaultValues.maxBatchSize;
    private boolean logEnabled = DefaultValues.logEnabled;
    private String zoneId = DefaultValues.zoneId;
}
