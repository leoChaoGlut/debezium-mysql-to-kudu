package personal.leo.debezium_to_kudu.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import personal.leo.debezium_to_kudu.mapper.HistoryMapper;
import personal.leo.debezium_to_kudu.mapper.OffsetMapper;
import personal.leo.debezium_to_kudu.mapper.TaskMapper;

import static personal.leo.debezium_to_kudu.utils.MybatisUtils.assertOperationSuccess;

@Service
public class TaskService {
    @Autowired
    TaskMapper taskMapper;
    @Autowired
    OffsetMapper offsetMapper;
    @Autowired
    HistoryMapper historyMapper;

    @Transactional
    public void delete(String taskId) {
        assertOperationSuccess(() -> taskMapper.deleteByPk(taskId));
        assertOperationSuccess(() -> offsetMapper.deleteByTaskId(taskId));
        assertOperationSuccess(() -> historyMapper.deleteByTaskId(taskId));
    }
}
