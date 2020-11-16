package personal.leo.debezium_to_kudu.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import personal.leo.debezium_to_kudu.common.Task;
import personal.leo.debezium_to_kudu.common.TaskService;
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
    @Autowired
    TaskService taskService;

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

    @GetMapping("activate/all")
    public String activateAll(String kuduTableName) {
        assertOperationSuccess(() -> taskMapper.activateAll(kuduTableName));
        return "ok";
    }

    @GetMapping("deactivate/all")
    public String deactivateAll(String kuduTableName) {
        assertOperationSuccess(() -> taskMapper.deactivateAll(kuduTableName));
        return "ok";
    }

    @GetMapping("delete")
    public String delete(String taskId) {
        taskService.delete(taskId);
        return "ok";
    }


}
