package com.zhugeio.etl.pipeline.operator.gate;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.client.redis.RedisClient;
import com.zhugeio.etl.pipeline.entity.*;
import com.zhugeio.etl.pipeline.exceptions.BusinessTableReadException;
import com.zhugeio.etl.pipeline.util.OperatorUtil;
import io.lettuce.core.KeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ningjh
 * @name GateFlatMapFunction
 * @date 2025/12/10
 * @description
 */
public class AdvAddUtmFlatMapFunction extends RichFlatMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(AdvAddUtmFlatMapFunction.class);
    private static final long serialVersionUID = 1L;
    private Properties jobProperties;
    private HikariDataSource dataSource;
    private Connection connection;
    private final long intervalMS = 1000L * 60;
    private String redisHost;
    private int redisPort;
    private boolean redisCluster;
    private RedisClient redisClient;

    private final String APP_PRE = "adtfad";
    private final String UTM_PRE = "utm";

    public AdvAddUtmFlatMapFunction() {
    }

    public AdvAddUtmFlatMapFunction(Properties jobProperties) {
        this.jobProperties = jobProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisClient = new RedisClient(
                jobProperties.getProperty("adv.redis.host"),
                Integer.parseInt(jobProperties.getProperty("adv.redis.port")),
                false);
        redisClient.init();
    }

    @Override
    public void flatMap(ZGMessage zgMessage, Collector<ZGMessage> collector) throws Exception {
        List<ZGMessage> msgs = new ArrayList<>();
        msgs.add(zgMessage);


        addUtm(msgs);

        for (ZGMessage msg : msgs) {
            collector.collect(msg);
        }
    }

    /**
     * 转换消息，处理广告深度回传事件
     */
    private void addUtm(List<ZGMessage> msgs) throws ParseException, SQLException {
        Set<String> thisKeySet = new HashSet<>();
        Map<String, String> thisDataMap = new HashMap<>();
        // $zg_eid
        // 获取 key
        getUtmKeys(msgs, thisKeySet);

        // 从ssdb 批量获取 value
        if (thisKeySet.size() > 0) {
            List<KeyValue<String, String>> keyValues = redisClient.syncMGet(thisKeySet.toArray(new String[0]));
            for (KeyValue<String, String> keyValue : keyValues){
                String key = keyValue.getKey();
                String value = keyValue.getValue();
                if (value != null) {
                    thisDataMap.put(key, value);
                }
            }
            if (!thisDataMap.isEmpty()) {
                // 为事件添加utm属性 （内置属性）
                setEventPrUtm(msgs, thisDataMap);
            }
        }
    }

    /**
     * 获取从ssdb取utm数据的key
     */
    private void setEventPrUtm(List<ZGMessage> msgs, Map<String, String> thisDataMap) {
        for (ZGMessage msg : msgs) {
            // 这里用的 rawData，避免上游未处理，msg.data无数据 ?
            // ss -1, se -2
            if (msg.getResult() != -1) {
                long zgEid = msg.getZgEid();

                // 处理来自web端+APP端的广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
                if (zgEid > 0) {
                    Map<String, Object> dataMap = msg.getData();
                    if (dataMap != null) {
                        List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataMap.get("data");
                        if (dataList != null) {
                            for (Map<String, Object> dataItem : dataList) {
                                String dt = (String) dataItem.get("dt");
                                // 注意 "mkt" etl-dw-phoenix工程会处理该类型的消息，未写kudu
                                // "evt" == dt || "mkt" == dt || "abp" == dt
                                if ("evt".equals(dt) || "abp".equals(dt)) {
                                    String utmKey = UTM_PRE + ":" + zgEid;
                                    if (thisDataMap.containsKey(utmKey)) {
                                        String utmJsonStr = thisDataMap.get(utmKey);
                                        if (!StringUtils.isEmpty(utmJsonStr)) {
                                            JSONObject utmJsonObj = JSON.parseObject(utmJsonStr);

                                            // utm_source utm_medium utm_campaign  utm_content utm_term
                                            String source = utmJsonObj.getString("utm_source");
                                            String medium = utmJsonObj.getString("utm_medium");
                                            String campaign = utmJsonObj.getString("utm_campaign");
                                            String utmcontent = utmJsonObj.getString("utm_content");
                                            String term = utmJsonObj.getString("utm_term");

                                            Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");
                                            if (props != null) {
                                                props.put("$utm_source", source);
                                                props.put("$utm_medium", medium);
                                                props.put("$utm_campaign", campaign);
                                                props.put("$utm_content", utmcontent);
                                                props.put("$utm_term", term);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取从ssdb取utm数据的key
     */
    private void getUtmKeys(List<ZGMessage> msgs, Set<String> thisKeySet) {
        // 假设 utmPre 是类的一个成员变量
        for (ZGMessage msg : msgs) {
            // 这里用的 rawData，避免上游未处理，msg.data无数据 ?
            // ss -1, se -2
            if (msg.getResult() != -1) {
                long zgEid = msg.getZgEid();
                // 处理来自web端+APP端的广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
                if (zgEid > 0) {
                    Map<String, Object> dataMap = msg.getData();
                    if (dataMap != null) {
                        List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataMap.get("data");
                        if (dataList != null) {
                            for (Map<String, Object> dataItem : dataList) {
                                String dt = (String) dataItem.get("dt");
                                // 注意 "mkt" etl-dw-phoenix工程会处理该类型的消息，未写kudu
                                // "evt" == dt || "mkt" == dt || "abp" == dt
                                if ("evt".equals(dt) || "abp".equals(dt)) {
                                    thisKeySet.add(UTM_PRE + ":" + zgEid);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
