package personal.leo.debezium_to_kudu.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import personal.leo.debezium_to_kudu.common.Task;
import personal.leo.debezium_to_kudu.mapper.TaskMapper;
import personal.leo.debezium_to_kudu.mapper.po.TaskPO;
import personal.leo.debezium_to_kudu.worker.Worker;

import static personal.leo.debezium_to_kudu.utils.MybatisUtils.assertOperationSuccess;

@RestController
@RequestMapping("task")
public class TaskController {

    @Autowired
    TaskMapper taskMapper;
    @Autowired
    Worker worker;

    @PostMapping("create")
    public TaskPO create(@RequestBody Task task) {
//        TODO 校验非空
//        TODO 校验非空
//        TODO 校验非空
        final TaskPO taskPO = TaskPO.of(task);
        assertOperationSuccess(() -> taskMapper.insert(taskPO));
        return taskPO;
    }

    @GetMapping("get")
    public Task get(String taskId) {
        final TaskPO taskPO = taskMapper.selectByPk(taskId);
        return Task.of(taskPO);
    }

    @PostMapping("update")
    public TaskPO update(@RequestBody Task task) {
//        TODO 校验非空
//        TODO 校验非空
//        TODO 校验非空
        final TaskPO taskPO = TaskPO.of(task);
        assertOperationSuccess(() -> taskMapper.updateJson(taskPO));
//       TODO  KuduSyncerPool.delete(task); kuduSyncer的信息没有改变
//       TODO  KuduSyncerPool.delete(task); kuduSyncer的信息没有改变
//       TODO  KuduSyncerPool.delete(task); kuduSyncer的信息没有改变
//       TODO  KuduSyncerPool.delete(task);
//       TODO  KuduSyncerPool.delete(task);
        return taskPO;
    }

    @GetMapping("activate")
    public String activate(String taskId) {
        assertOperationSuccess(() -> taskMapper.activate(taskId));
        return "ok";
    }

    @GetMapping("deactivate")
    public String deactivate(String taskId) {
        assertOperationSuccess(() -> taskMapper.deactivate(taskId));
        return "ok";
    }


}
