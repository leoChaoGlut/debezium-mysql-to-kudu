package personal.leo.debezium_to_kudu.utils;

import lombok.Getter;
import lombok.Setter;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class CommonUtils {
    @Setter
    @Getter
    private static int port;

    public static String getIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    public static String getThisServerId() {
        if (port <= 0) {
            throw new RuntimeException("port has not initialized: " + port);
        }
        return getIp() + ":" + port;
    }


}
