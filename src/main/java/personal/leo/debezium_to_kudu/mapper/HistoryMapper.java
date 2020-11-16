package personal.leo.debezium_to_kudu.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import personal.leo.debezium_to_kudu.mapper.po.History;

import java.util.List;

@Mapper
public interface HistoryMapper {
    @Insert("insert into history(task_id, json)\n" +
            "values (#{taskId}, #{json})")
    int insert(@Param("taskId") String taskId, @Param("json") String json);

    @Select("select *\n" +
            "from history\n" +
            "where task_id = #{taskId}")
    List<History> select(@Param("taskId") String taskId);

    @Select("select count(*)\n" +
            "from history\n" +
            "where task_id = #{taskId}")
    int count(@Param("taskId") String taskId);

    @Select("delete\n" +
            "from history\n" +
            "where task_id = #{taskId}")
    int deleteByTaskId(@Param("taskId") String taskId);
}
