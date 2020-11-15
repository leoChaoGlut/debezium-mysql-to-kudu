package personal.leo.debezium_to_kudu.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.SimpleEmail;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import personal.leo.debezium_to_kudu.config.props.EmailProps;

@Slf4j
@Service
public class EmailService {

    @Autowired
    EmailProps emailProps;

    @Async
    public void send(String msg) {
        log.info("send email: " + emailProps);
        try {
            Email email = new SimpleEmail();
            email.setHostName(emailProps.getHostName());
            email.setSmtpPort(25);
            email.setAuthenticator(new DefaultAuthenticator(emailProps.getUser(), emailProps.getPassword()));
            email.setFrom(emailProps.getFrom());
            email.setSubject("Debezium to kudu: task failed");
            email.setMsg(msg);
            email.addTo(emailProps.getTos().toArray(new String[0]));
            email.send();
        } catch (Exception e) {
            log.error("send email failed", e);
        }
    }

}
