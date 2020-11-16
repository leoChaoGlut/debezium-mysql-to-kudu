package personal.leo.debezium_to_kudu.constants;

/**
 * TODO 后续考虑从配置文件获取
 */
public interface WorkerConfig {
    int occupyTasksPeriodSec = 3;
    int updateUpdateTimePeriodSec = 3;
    int maxUpdateUpdateTimeRetryTimes = 10;
    int taskDeadThresholdSec = updateUpdateTimePeriodSec * maxUpdateUpdateTimeRetryTimes;
}
