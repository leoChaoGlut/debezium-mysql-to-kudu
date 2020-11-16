package personal.leo.debezium_to_kudu.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import personal.leo.debezium_to_kudu.mapper.po.Offset;

import java.util.List;

@Mapper
public interface OffsetMapper {

    @Insert(
            "insert into offset(task_id, `key`, value)\n" +
                    "values (#{task_id}, #{key}, #{value})\n" +
                    "ON DUPLICATE KEY UPDATE `key` = #{key},\n" +
                    "                        value = #{value}"
    )
    int upsert(Offset offset);

    @Select("select *\n" +
            "from offset\n" +
            "where task_id = #{taskId}")
    List<Offset> select(@Param("taskId") String taskId);

    @Select("delete\n" +
            "from offset\n" +
            "where task_id = #{taskId}")
    int deleteByTaskId(@Param("taskId") String taskId);
}
