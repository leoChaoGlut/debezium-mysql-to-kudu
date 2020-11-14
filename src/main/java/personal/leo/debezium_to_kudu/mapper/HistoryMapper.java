package personal.leo.debezium_to_kudu.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import personal.leo.debezium_to_kudu.mapper.po.History;

import java.util.List;

@Mapper
public interface HistoryMapper {
    @Insert("insert into history(instance_id,json) values(#{instanceId},#{json})")
    int insert(@Param("instanceId") String instanceId, @Param("json") String json);

    @Select("select * from history where instance_id = #{instanceId}")
    List<History> select(@Param("instanceId") String instanceId);

    @Select("select count(*) from history where instance_id = #{instanceId}")
    int count(@Param("instanceId") String instanceId);

}
