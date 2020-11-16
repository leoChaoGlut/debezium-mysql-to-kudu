import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kudu.client.*;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class Common1Test {
    @Test
    public void test() {
        final String value = "2020-11-13T08:01:07Z";
        final int val = 1604925042;
        final int val2 = 1604925043;
//        final int val3=1605487104157;
        int nanoSeconds = 0; // no fractional seconds
        final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(val, nanoSeconds), ZoneOffset.UTC);
        final Date date = new Date();
        System.out.println(date.getTime());
        System.out.println(zonedDateTime);
    }

    @Test
    public void testkudu() throws Exception {
        final String value = "2020-11-08T17:01:01Z";
        final TimeZone timeZone = TimeZone.getTimeZone("GMT+16");
//        final FastDateFormat dateFormat = FastDateFormat.getInstance(DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.getPattern(), timeZone);
        final SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormatUtils.ISO_8601_EXTENDED_DATETIME_FORMAT.getPattern());
        dateFormat.setTimeZone(timeZone);
        final Date date = dateFormat.parse(value);
        System.out.println(date);
        String masterAddr = "test01,test02,test03";
        KuduClient client = new KuduClient.KuduClientBuilder(masterAddr).build();
        final KuduTable table = client.openTable("presto.test.t10");
        final KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        final Upsert upsert = table.newUpsert();
        final PartialRow row = upsert.getRow();
        row.addInt("id", 8);
//        row.addLong("t1", 1604925043000L);
        row.addTimestamp("t1", new Timestamp(1604925043000L));
        row.addTimestamp("t2", new Timestamp(date.getTime()));
//        row.addLong("t2", 1604925044);
        session.apply(upsert);
        final List<OperationResponse> flush = session.flush();
        if (flush.size() > 0) {
            OperationResponse response = flush.get(0);
            if (response.hasRowError()) {
                System.out.println("update list is :" + response.getRowError().getMessage());
            }
        }
        session.close();
    }

}
