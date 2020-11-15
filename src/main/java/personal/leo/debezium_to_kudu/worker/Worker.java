package personal.leo.debezium_to_kudu.worker;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import personal.leo.debezium_to_kudu.common.EmailService;
import personal.leo.debezium_to_kudu.common.KuduSyncer;
import personal.leo.debezium_to_kudu.common.Task;
import personal.leo.debezium_to_kudu.config.props.KuduProps;
import personal.leo.debezium_to_kudu.constants.PayloadKeys;
import personal.leo.debezium_to_kudu.mapper.TaskMapper;
import personal.leo.debezium_to_kudu.mapper.po.TaskPO;
import personal.leo.debezium_to_kudu.utils.CommonUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static personal.leo.debezium_to_kudu.constants.Separator.DOT;
import static personal.leo.debezium_to_kudu.constants.WorkerConfig.*;
import static personal.leo.debezium_to_kudu.utils.MybatisUtils.assertOperationSuccess;

@Slf4j
@Service
public class Worker {
    @Autowired
    TaskMapper taskMapper;
    @Autowired
    TaskExecutor taskExecutor;
    @Autowired
    EmailService emailService;
    @Autowired
    KuduProps kuduProps;

    private final Map<Task, EmbeddedEngine> taskMapEngine = Collections.synchronizedMap(new HashMap<>());


    @Scheduled(fixedDelay = occupyTasksPeriodSec * 1000)
    private void occupyTasks() {
        try {
            final List<TaskPO> taskPOS = taskMapper.selectUnoccupied(taskDeadThresholdSec);
            for (TaskPO taskPO : taskPOS) {
                try {
                    assertOperationSuccess(() -> taskMapper.occupy(taskPO.getTask_id(), taskPO.getWorker(), CommonUtils.getThisServerId()));

                    tryRunTask(taskPO);

                    TimeUnit.MILLISECONDS.sleep(500);//抢占成功后,sleep 一会儿,让其他worker抢占
                } catch (Exception e) {
                    log.error("occupyTask failed: ", e);
                }
            }
        } catch (Exception e) {
            log.error("occupyTasks error", e);
        }
    }


    @Scheduled(fixedDelay = updateUpdateTimePeriodSec * 1000)
    private void updateTasksUpdateTime() {
        try {
            final Map<Task, EmbeddedEngine> notRunningTaskMapEngine = new HashMap<>();

            taskMapEngine.forEach((task, engine) -> {
                if (engine.isRunning()) {
                    assertOperationSuccess(() -> taskMapper.updateUpdateTime(task.id()));
                } else {
                    notRunningTaskMapEngine.put(task, engine);
                }
            });

            notRunningTaskMapEngine.forEach(this::stopTask);

            stopInactiveTasks();
        } catch (Exception e) {
            log.error("updateTasksUpdateTime error", e);
        }
    }

    private void runTask(Task task) throws Exception {
        final Properties props = task.toProps();
        log.info("runTask: " + props);

        final Set<Task.KuduSink> kuduSinks = task.getKuduSinks();
        final Set<KuduSyncer> kuduSyncers = new HashSet<>();
        for (Task.KuduSink kuduSink : kuduSinks) {
            kuduSyncers.add(new KuduSyncer(kuduProps, kuduSink));
        }

        final DebeziumEngine.ChangeConsumer<SourceRecord> msgConsumer = (records, committer) -> {
            log.info("msg: " + records.size());
            for (SourceRecord record : records) {
                final Struct payload = (Struct) record.value();
                final Struct source = payload.getStruct(PayloadKeys.source);
                final String srcTableId = source.getString(PayloadKeys.db) + DOT + source.getString(PayloadKeys.table);
                final List<KuduSyncer> acceptedKuduSyncers = kuduSyncers.stream()
                        .filter(kuduSyncer -> kuduSyncer.accept(srcTableId))
                        .collect(Collectors.toList());
//                TODO
//                TODO
//                TODO
//                TODO
                System.out.println(payload);

            }
            if (records.size() > 5) {
                throw new RuntimeException("1");
            }
            for (SourceRecord record : records) {
                committer.markProcessed(record);
            }
            committer.markBatchFinished();
        };

        taskExecutor.execute(() -> {
            final EmbeddedEngine engine = new EmbeddedEngine.BuilderImpl()
                    .using(props)
                    .notifying(msgConsumer)
                    .build();
            try {
                taskMapEngine.put(task, engine);

                engine.run();
            } catch (Exception e) {
                log.error("engine run error", e);
                throw new RuntimeException(e);
            } finally {
                emailService.send("Task: " + task.id() + " is stop");
                stopTask(task, engine);
            }
        });
    }

    private void stopTask(String taskId) {
        final Map.Entry<Task, EmbeddedEngine> entry = findEntryByTaskId(taskId);
        if (entry != null) {
            entry.getValue().stop();
            taskMapEngine.remove(entry.getKey());
            taskMapper.setUpdateTimeToNull(taskId);
        }
    }

    private void stopTask(Task task, EmbeddedEngine engine) {

        taskMapper.setUpdateTimeToNull(task.id());
    }

    private void stopInactiveTasks() {
        final List<String> taskIds = taskMapEngine.keySet().stream()
                .map(Task::id)
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(taskIds)) {
            final String taskIdsStr = taskIds.stream()
                    .map(taskId -> "'" + taskId + "'")
                    .reduce((taskId1, taskId2) -> taskId1 + "," + taskId2)
                    .get();

            taskMapper.selectStateByPks(taskIdsStr)
                    .forEach(taskPO -> {
                        if (taskPO.getState() == Task.State.INACTIVE) {
                            stopTask(taskPO.getTask_id());
                        }
                    });
        }
    }

    private Map.Entry<Task, EmbeddedEngine> findEntryByTaskId(String taskId) {
        for (Map.Entry<Task, EmbeddedEngine> entry : taskMapEngine.entrySet()) {
            if (StringUtils.equals(entry.getKey().id(), taskId)) {
                return entry;
            }
        }
        return null;
    }

    private void tryRunTask(TaskPO taskPO) {
        final Task task = Task.of(taskPO);
        try {
            runTask(task);
        } catch (Exception e) {
            final EmbeddedEngine engine = taskMapEngine.get(task);
            if (engine != null) {
                engine.stop();
            }
            assertOperationSuccess(() -> taskMapper.setUpdateTimeToNull(task.id()));
            throw new RuntimeException(e);
        }
    }

}
