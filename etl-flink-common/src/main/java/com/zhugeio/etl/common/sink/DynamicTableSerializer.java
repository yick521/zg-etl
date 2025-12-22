package com.zhugeio.etl.common.sink;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 动态表名序列化器
 *
 * 支持根据记录内容动态路由到不同的 Doris 表
 *
 * 使用示例:
 * <pre>
 * // UserRow 写入 b_user_{appId}
 * DynamicTableSerializer<UserRow> serializer = new DynamicTableSerializer<>(
 *     "dwd", "b_user", UserRow::getAppId
 * );
 *
 * // EventAttrRow 写入 b_user_event_attr_{appId}
 * DynamicTableSerializer<EventAttrRow> serializer = new DynamicTableSerializer<>(
 *     "dwd", "b_user_event_attr", EventAttrRow::getAppId
 * );
 * </pre>
 */
public class DynamicTableSerializer<T> implements DorisRecordSerializer<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableSerializer.class);

    private transient ObjectMapper objectMapper;

    private final String database;
    private final String tablePrefix;
    private final SerializableFunction<T, Integer> appIdExtractor;
    private final JsonInclude.Include includeStrategy;

    public DynamicTableSerializer(String database, String tablePrefix,
                                  SerializableFunction<T, Integer> appIdExtractor) {
        this(database, tablePrefix, appIdExtractor, JsonInclude.Include.NON_NULL);
    }

    public DynamicTableSerializer(String database, String tablePrefix,
                                  SerializableFunction<T, Integer> appIdExtractor,
                                  JsonInclude.Include includeStrategy) {
        this.database = database;
        this.tablePrefix = tablePrefix;
        this.appIdExtractor = appIdExtractor;
        this.includeStrategy = includeStrategy;
    }

    private void initObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(includeStrategy);
        }
    }

    @Override
    public DorisRecord serialize(T record) throws IOException {
        if (record == null) {
            return null;
        }

        initObjectMapper();

        // 1. 提取 appId
        Integer appId = appIdExtractor.apply(record);
        if (appId == null) {
            LOG.warn("Record appId is null, skipping: {}", record);
            return null;
        }

        // 2. 构建动态表名: database.tablePrefix_appId
        String tableName = database + "." + tablePrefix + "_" + appId;

        // 3. 序列化数据为 JSON
        String json = objectMapper.writeValueAsString(record);

        // 添加日志 - 打印序列化后的 JSON
        LOG.info("[DynamicTableSerializer] table={}, record={}, json={}", tableName, record, json);

        // 4. 使用 DorisRecord.of(tableName, data) 指定目标表
        return DorisRecord.of(tableName, json.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void initial() {
        initObjectMapper();
    }

    @Override
    public DorisRecord flush() {
        return null;
    }

    public String getDatabase() {
        return database;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }
}