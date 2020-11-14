package personal.leo.debezium_to_kudu.mapper;

import org.apache.ibatis.annotations.*;
import personal.leo.debezium_to_kudu.mapper.po.InstancePO;

@Mapper
public interface InstanceMapper {
    @Insert("insert into instance(instance_id,json) values(#{instance_id},#{json})")
    @Options(useGeneratedKeys = true, keyProperty = "instance_num", keyColumn = "instance_num")
    int insert(InstancePO instancePO);

    @Select("select * from instance where instance_id = #{instanceId}")
    InstancePO select(@Param("instanceId") String instanceId);
}
