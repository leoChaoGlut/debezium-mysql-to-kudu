package personal.leo.debezium_to_kudu.mapper;

import org.apache.ibatis.annotations.*;
import personal.leo.debezium_to_kudu.mapper.po.TaskPO;

import java.util.List;

@Mapper
public interface TaskMapper {
    @Insert("insert into task(task_id, json)\n" +
            "values (#{task_id}, #{json})\n")
    @Options(useGeneratedKeys = true, keyProperty = "task_num", keyColumn = "task_num")
    int insert(TaskPO taskPO);

    @Select("select *\n" +
            "from task\n" +
            "where task_id = #{taskId}")
    TaskPO selectByPk(@Param("taskId") String taskId);

    @Select("select task_id, state\n" +
            "from task\n" +
            "where task_id in (${taskIds})")
    List<TaskPO> selectStateByPks(@Param("taskIds") String taskIds);

    @Select("select *\n" +
            "from task\n" +
            "where state = 'ACTIVE'\n" +
            "  " +
            "and (\n" +
            "        " +
            "update_time is null\n" +
            "        or update_time < date_add(now(), interval -${taskDeadThresholdSec} second)\n" +
            "    )")
    List<TaskPO> selectUnoccupied(@Param("taskDeadThresholdSec") int taskDeadThresholdSec);

    @Update("<script>" +
            "update task\n" +
            "set worker      = #{newWorker},\n" +
            "    update_time = NOW()\n" +
            "where task_id = #{taskId}\n" +
            "   and state = 'ACTIVE'\n" +
            "<choose>" +
            "   <when test='oldWorker == null'>" +
            "       and worker is null" +
            "   </when>" +
            "   <otherwise>" +
            "       and worker = #{oldWorker}" +
            "   </otherwise>" +
            "</choose>" +
            "</script>"
    )
    int occupy(@Param("taskId") String taskId, @Param("oldWorker") String oldWorker, @Param("newWorker") String newWorker);

    @Update("update task\n" +
            "set update_time = NOW()\n" +
            "where task_id = #{taskId}")
    int updateUpdateTime(@Param("taskId") String taskId);

    @Update("update task\n" +
            "set update_time = null\n" +
            "where task_id = #{taskId}")
    int setUpdateTimeToNull(@Param("taskId") String taskId);

    @Update("update task\n" +
            "set json = #{json}\n" +
            "where task_id = #{task_id}"
    )
    int updateJson(TaskPO taskPO);

    @Update("update task\n" +
            "set state = 'ACTIVE'\n" +
            "where task_id = #{taskId}"
    )
    int activate(@Param("taskId") String taskId);

    @Update("update task\n" +
            "set state = 'INACTIVE'\n" +
            "where task_id = #{taskId}"
    )
    int deactivate(@Param("taskId") String taskId);
}
