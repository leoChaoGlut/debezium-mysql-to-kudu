package personal.leo.debezium_to_kudu.debezium.mysql;

import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import personal.leo.debezium_to_kudu.constants.PropKeys;
import personal.leo.debezium_to_kudu.mapper.HistoryMapper;
import personal.leo.debezium_to_kudu.mapper.po.History;
import personal.leo.debezium_to_kudu.spring.ApplicationContextHolder;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class MysqlDatabaseHistory extends AbstractDatabaseHistory {
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        try {
            final HistoryMapper historyMapper = ApplicationContextHolder.getBean(HistoryMapper.class);
            final String instanceId = config.getString(PropKeys.databaseHistoryConnectorId);
            String json = writer.write(record.document());
            historyMapper.insert(instanceId, json);
        } catch (IOException e) {
            throw new RuntimeException("storeRecord error", e);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        final HistoryMapper historyMapper = ApplicationContextHolder.getBean(HistoryMapper.class);
        final String instanceId = config.getString(PropKeys.databaseHistoryConnectorId);
        final List<History> histories = historyMapper.select(instanceId);
        try {
            for (History history : histories) {
                records.accept(new HistoryRecord(reader.read(history.getJson())));
            }
        } catch (IOException e) {
            throw new RuntimeException("recoverRecords error", e);
        }
    }

    @Override
    public boolean exists() {
        return storageExists();
    }

    @Override
    public boolean storageExists() {
        final HistoryMapper historyMapper = ApplicationContextHolder.getBean(HistoryMapper.class);
        final String instanceId = config.getString(PropKeys.databaseHistoryConnectorId);
        final int count = historyMapper.count(instanceId);
        return count > 0;
    }
}
