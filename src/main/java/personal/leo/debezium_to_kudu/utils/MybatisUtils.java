package personal.leo.debezium_to_kudu.utils;

import java.util.function.Supplier;

public class MybatisUtils {

    public static void assertOperationSuccess(Supplier<Integer> supplier) {
        final Integer count = supplier.get();
        if (count <= 0) {
            throw new RuntimeException("sql operation failed: " + count);
        }
    }
}
