package personal.leo.debezium_to_kudu.utils;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.Set;
import java.util.stream.Collectors;

public class StructUtils {
    public static Struct getStruct(Struct struct, String fieldName) {
        final Set<String> fieldNames = struct.schema().fields().stream().map(Field::name).collect(Collectors.toSet());
        if (fieldNames.contains(fieldName)) {
            return struct.getStruct(fieldName);
        } else {
            return null;
        }
    }
}
