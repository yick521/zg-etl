package com.zhugeio.etl.common.sink;

import com.zhugeio.etl.common.model.EventAttrRow;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * EventAttrRow JSON 序列化器
 *
 * 直接调用 EventAttrRow.toJson()，无需 ObjectMapper
 */
public class EventAttrRowSerializer implements DorisRecordSerializer<EventAttrRow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EventAttrRowSerializer.class);

    private final String database;
    private final String tablePrefix;

    public EventAttrRowSerializer(String database, String tablePrefix) {
        this.database = database;
        this.tablePrefix = tablePrefix;
    }

    @Override
    public void initial() {
    }

    @Override
    public DorisRecord serialize(EventAttrRow row) throws IOException {
        if (row == null || row.getAppId() == null) {
            return null;
        }

        String tableName = database + "." + tablePrefix + "_" + row.getAppId();
        // 直接调用 toJson()
        return DorisRecord.of(tableName, row.toJson().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public DorisRecord flush() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}