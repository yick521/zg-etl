package com.zhugeio.etl.common.sink;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.zhugeio.etl.common.model.EventAttrRow;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * EventAttrRow 专用序列化器
 *
 * 将 EventAttrRow 的 List<String> columns 转换为 Doris 表结构对应的 JSON
 *
 * Doris 表结构 (242列):
 * - 列 0-39: 基础字段
 * - 列 40-139: cus1-cus100
 * - 列 140-239: type1-type100
 * - 列 240-241: eid, yw
 */
public class EventAttrRowSerializer implements DorisRecordSerializer<EventAttrRow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EventAttrRowSerializer.class);

    private static final String NULL_VALUE = "\\N";

    // Doris 列名 (按顺序)
    private static final String[] BASIC_COLUMNS = {
            "zg_id", "session_id", "uuid", "event_id",
            "begin_day_id", "begin_date", "begin_time_id",
            "device_id", "user_id", "event_name",
            "platform", "network", "mccmnc", "useragent",
            "website", "current_url", "referrer_url",
            "channel", "app_version", "ip", "ip_str",
            "country", "area", "city",
            "os", "ov", "bs", "bv",
            "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
            "duration", "utc_date",
            "attr1", "attr2", "attr3", "attr4", "attr5"
    };

    private final String database;
    private final String tablePrefix;
    private transient ObjectMapper objectMapper;

    public EventAttrRowSerializer(String database, String tablePrefix) {
        this.database = database;
        this.tablePrefix = tablePrefix;
    }

    @Override
    public void initial() {
        initObjectMapper();
    }

    private void initObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            // 注册自定义序列化器
            SimpleModule module = new SimpleModule();
            module.addSerializer(EventAttrRow.class, new EventAttrRowJsonSerializer());
            objectMapper.registerModule(module);
        }
    }

    @Override
    public DorisRecord serialize(EventAttrRow record) throws IOException {
        if (record == null) {
            return null;
        }

        initObjectMapper();

        Integer appId = record.getAppId();
        if (appId == null) {
            LOG.warn("EventAttrRow appId is null, skipping");
            return null;
        }

        String tableName = database + "." + tablePrefix + "_" + appId;
        String json = objectMapper.writeValueAsString(record);

        return DorisRecord.of(tableName, json.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public DorisRecord flush() {
        return null;
    }

    /**
     * EventAttrRow 自定义 JSON 序列化器
     */
    private static class EventAttrRowJsonSerializer extends JsonSerializer<EventAttrRow> {

        @Override
        public void serialize(EventAttrRow row, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {

            List<String> columns = row.getColumns();

            gen.writeStartObject();

            // 写入基础字段 (列 0-39)
            for (int i = 0; i < BASIC_COLUMNS.length && i < columns.size(); i++) {
                String value = columns.get(i);
                writeField(gen, BASIC_COLUMNS[i], value, i);
            }

            // 写入自定义属性 cus1-cus100 (列 40-139)
            for (int i = 1; i <= 100; i++) {
                int colIndex = 39 + i;  // 40-139
                if (colIndex < columns.size()) {
                    String value = columns.get(colIndex);
                    if (!isNull(value)) {
                        gen.writeStringField("cus" + i, value);
                    }
                }
            }

            // 写入类型字段 type1-type100 (列 140-239)
            for (int i = 1; i <= 100; i++) {
                int colIndex = 139 + i;  // 140-239
                if (colIndex < columns.size()) {
                    String value = columns.get(colIndex);
                    if (!isNull(value)) {
                        gen.writeStringField("type" + i, value);
                    }
                }
            }

            // 写入分区字段 eid, yw (列 240-241)
            if (columns.size() > 240) {
                String eid = columns.get(240);
                if (!isNull(eid)) {
                    gen.writeStringField("eid", eid);
                }
            }
            if (columns.size() > 241) {
                String yw = columns.get(241);
                if (!isNull(yw)) {
                    gen.writeStringField("yw", yw);
                }
            }

            gen.writeEndObject();
        }

        /**
         * 写入字段，根据 Doris 列类型进行适当转换
         */
        private void writeField(JsonGenerator gen, String fieldName, String value, int colIndex)
                throws IOException {

            if (isNull(value)) {
                return;  // 跳过 null 值，让 Doris 使用默认值
            }

            // 根据列索引判断数据类型
            // BIGINT 列: zg_id(0), session_id(1), event_id(3), begin_date(5), device_id(7), user_id(8), ip(19), duration(33), utc_date(34)
            // INT 列: begin_day_id(4), begin_time_id(6), mccmnc(12), ov(25), bv(27)
            // SMALLINT 列: platform(10), network(11)
            // VARCHAR 列: 其他

            switch (colIndex) {
                case 0:  // zg_id - BIGINT
                case 1:  // session_id - BIGINT
                case 3:  // event_id - BIGINT
                case 5:  // begin_date - BIGINT
                case 7:  // device_id - BIGINT
                case 8:  // user_id - BIGINT
                case 19: // ip - BIGINT
                case 33: // duration - BIGINT
                case 34: // utc_date - BIGINT
                    // 写入字符串，Doris 会自动转换
                    gen.writeStringField(fieldName, value);
                    break;

                case 4:  // begin_day_id - INT
                case 6:  // begin_time_id - INT
                case 12: // mccmnc - INT
                case 25: // ov - INT (操作系统版本)
                case 27: // bv - INT (浏览器版本)
                case 10: // platform - SMALLINT
                case 11: // network - SMALLINT
                    gen.writeStringField(fieldName, value);
                    break;

                default:
                    // VARCHAR 类型
                    gen.writeStringField(fieldName, value);
                    break;
            }
        }

        private boolean isNull(String value) {
            return value == null || value.isEmpty() || NULL_VALUE.equals(value);
        }
    }
}