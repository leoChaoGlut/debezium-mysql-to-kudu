package personal.leo.debezium_to_kudu.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.SimpleEmail;

import java.util.Arrays;

@Slf4j
public class EmailService {

    private final String hostName;
    private final String from;
    private final String user;
    private final String password;
    private final String[] to;

    public EmailService(String hostName, String from, String user, String password, String[] to) {
        this.hostName = hostName;
        this.from = from;
        this.user = user;
        this.password = password;
        this.to = to;
    }

    public void send(String msg) {
        log.info("send email: " + toString());
        try {
            Email email = new SimpleEmail();
            email.setHostName(hostName);
            email.setSmtpPort(25);
            email.setAuthenticator(new DefaultAuthenticator(user, password));
            email.setFrom(from);
            email.setSubject("Kafka Connect Kudu failed");
            email.setMsg(msg);
            email.addTo(to);
            email.send();
        } catch (Exception e) {
            log.error("send email failed", e);
        }
    }

    @Override
    public String toString() {
        return "EmailService{" +
                "hostName='" + hostName + '\'' +
                ", from='" + from + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", to=" + Arrays.toString(to) +
                '}';
    }
}
