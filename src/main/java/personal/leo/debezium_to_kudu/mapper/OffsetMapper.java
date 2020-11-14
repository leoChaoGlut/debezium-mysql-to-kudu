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
            "insert into offset(instance_id,`key`,value) values(#{instance_id},#{key},#{value}) " +
                    "ON DUPLICATE KEY UPDATE `key` = #{key}, value = #{value}"
    )
    int upsert(Offset offset);

    @Select("select * from offset where instance_id = #{instanceId}")
    List<Offset> select(@Param("instanceId") String instanceId);
}
