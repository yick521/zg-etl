package com.zhugeio.etl.pipeline.operator.gate;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.client.redis.RedisClient;
import com.zhugeio.etl.pipeline.entity.AdvMessage;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.exceptions.BusinessTableReadException;
import com.zhugeio.etl.common.util.ToolUtil;
import com.zhugeio.tool.commons.JsonUtil;
import io.lettuce.core.KeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
public class AdvLidAndUserFirstEndFlatMapFunction extends RichFlatMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(AdvLidAndUserFirstEndFlatMapFunction.class);
    private static final long serialVersionUID = 1L;
    private Properties jobProperties;
    private HikariDataSource dataSource;
    private Connection connection;
    private final long intervalMS = 1000L * 60;
    private String redisHost;
    private int redisPort;
    private boolean redisCluster;
    private RedisClient redisClient;
    private long expireTime = 1000L*60*60*24;
    private long tengxunWebExpireTime = 1000L*60;
    private Map<String,Integer> advertisingMap ;
    private final String APP_PRE = "adtfad";
    private final String SSDB_CT_PRE = "adtfuser";
    private final String SSDB_AD_DATA_PRE = "adtfdata";
    /** 首次广告来源 */
    public static final String USER_FIRST_LID_NAME = "_首次广告来源";

    /** 末次广告来源 */
    public static final String USER_FOLLOW_LID_NAME = "_末次广告来源";

    /** 广告分析链接ID */
    public static final String LID_NAME = "_广告分析链接ID";

    /** 广告渠道ID */
    public static final String CHANNEL_TYPE_NAME = "_广告渠道ID";

    /** 广告渠道名称 */
    public static final String CHANNEL_TYPE_NAME_STR = "_广告渠道名称";

    /** 广告账号ID */
    public static final String CHANNEL_ACCOUNT_ID_NAME = "_广告账号ID";

    /** 广告计划ID */
    public static final String CHANNEL_CAMPAIGN_ID_NAME = "_广告计划ID";

    /** 广告组ID */
    public static final String CHANNEL_ADGROUP_ID_NAME = "_广告组ID";

    /** 广告创意ID */
    public static final String CHANNEL_AD_ID_NAME = "_广告创意ID";

    /** 广告关键词ID */
    public static final String CHANNEL_KEYWORD_ID_NAME = "_广告关键词ID";

    /** 广告创意集ID */
    public static final String CHANNEL_CREATIVE_SET_ID_NAME = "_广告创意集ID";

    private Set<String> originExcludeSet = new HashSet<>();
    private Set<String> md5ExcludeSet = new HashSet<>();

    {
        originExcludeSet.add("");
        originExcludeSet.add("0");
        originExcludeSet.add("NULL");
        originExcludeSet.add("null");
        originExcludeSet.add("(null)"); //--新增
        md5ExcludeSet.add("cfcd208495d565ef66e7dff9f98764da");
        md5ExcludeSet.add("6c3e226b4d4795d518ab341b0824ec29");
        md5ExcludeSet.add("37a6259cc0c1dae299a7866489dff0bd");
        md5ExcludeSet.add("d41d8cd98f00b204e9800998ecf8427e");
        md5ExcludeSet.add("a4d2f177eb466a7d08f8f2b340b77129");
    }

    public AdvLidAndUserFirstEndFlatMapFunction() {
    }

    public AdvLidAndUserFirstEndFlatMapFunction(Properties jobProperties) {
        this.jobProperties = jobProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisClient = new RedisClient(
                jobProperties.getProperty("adv.redis.host"),
                Integer.parseInt(jobProperties.getProperty("adv.redis.port")),
                false);
        redisClient.init();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jobProperties.getProperty("rdbms.url"));
        hikariConfig.setUsername(jobProperties.getProperty("rdbms.userName"));
        hikariConfig.setPassword(jobProperties.getProperty("rdbms.password"));
        hikariConfig.setDriverClassName(jobProperties.getProperty("rdbms.driverClass"));
        hikariConfig.setMaximumPoolSize(Integer.parseInt(jobProperties.getProperty("rdbms.maxPoolSize")));
        hikariConfig.setMinimumIdle(Integer.parseInt(jobProperties.getProperty("rdbms.minPoolSize")));
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);
        dataSource = new HikariDataSource(hikariConfig);
        connection = dataSource.getConnection();

        String expire = jobProperties.getProperty("adv.buss.redis.expire");
        if (StringUtils.isNotBlank(expire)) {
            this.expireTime = Long.parseLong(expire);
        }

        String tengxunWebExpireTime = jobProperties.getProperty("adv.buss.redis.tengxun.web.expireTime");
        if (StringUtils.isNotBlank(tengxunWebExpireTime)) {
            this.tengxunWebExpireTime = Long.parseLong(tengxunWebExpireTime);
        }

        startBatchInit();

        // 周期读取mysql表数据
        AtomicBoolean isRunning = new AtomicBoolean(true);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                () -> {
                    // 这个任务会基于服务器时间，周期性执行
                    if (isRunning.get()) {
                        try {
                            startBatchInit(); // 读取mysql表数据
                        } catch (Exception e) {
                            logger.error("定时任务执行出错", e);
                        }
                    }
                },
                intervalMS, // 初始延迟：0毫秒，即立即开始
                intervalMS, // 执行间隔
                TimeUnit.MILLISECONDS // 时间单位
        );
        logger.info("定时任务已启动，间隔 {} 毫秒", intervalMS);
    }

    @Override
    public void flatMap(ZGMessage zgMessage, Collector<ZGMessage> collector) throws Exception {
        List<ZGMessage> msgs = new ArrayList<>();
        msgs.add(zgMessage);
        Set<String> thisDelKeySet = new HashSet<>();
        Set<String> thisMaxCtKeySet = new HashSet<>();
        Map<String, Long> thisMaxCtMap = new HashMap<>();
        Map<String, String> thisDataMap = new HashMap<>();

        Map<String, String> thisLIdLnameMap = new HashMap<>();

        Map<String, String> lastSsdbMaxCtMap = new HashMap<>();
        final Set<String> lastLIdLnameKeySet = new HashSet<>();
        Map<String, String> lastLIdLnameMap = new HashMap<>();

        Map<String, String> updateSsdbMaxCtMap = new HashMap<>();
        Set<String> updateLIdLnameKeySet = new HashSet<>();
        Map<String, String> updateLIdLnameMap = new HashMap<>();

        Set<String> updateForUserPrKeySet = new HashSet<>();

        Set<String> thisAppMaxCtKeySet = new HashSet<>();
        Map<String, String> thisAppSsdbMaxCtMap = new HashMap<>();

        Set<String> thisAppDataKeySet = new HashSet<>();
        Map<String, String> thisAppSsdbDataMap = new HashMap<>();

        // 取 app端 ip+ua 对应的广告数据
        setAppIpUaKeys(msgs, thisAppMaxCtKeySet);
        logger.debug("LidAndUserFirstEndService app 本批次ip +ua thisAppMaxCtKeySet : {} ",thisAppMaxCtKeySet);

        //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}"，ct）
        if (!thisAppMaxCtKeySet.isEmpty()) {
            List<KeyValue<String, String>> keyValues = redisClient.syncMGet(thisAppMaxCtKeySet.toArray(new String[0]));
            for (KeyValue<String, String> keyValue : keyValues) {
                String key = keyValue.getKey();
                String value = keyValue.getValue();
                if (value != null) {
                    thisAppSsdbMaxCtMap.put(key, value);
                }
            }
            logger.debug("LidAndUserFirstEndService app ssdb获取之前已存入的MaxCt thisAppSsdbMaxCtMap : {} ",thisAppMaxCtKeySet);

            Set<Map.Entry<String, String>> entrys = thisAppSsdbMaxCtMap.entrySet();
            for (Map.Entry<String, String> entry : entrys) {
                String key = entry.getKey();
                String value = entry.getValue();
                if(value != null)
                    thisAppDataKeySet.add(key+":"+value);
            }

            if (!thisAppDataKeySet.isEmpty()) {
                //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
                keyValues = redisClient.syncMGet(thisAppDataKeySet.toArray(new String[0]));
                for (KeyValue<String, String> keyValue : keyValues) {
                    String key = keyValue.getKey();
                    String value = keyValue.getValue();
                    if (value != null) {
                        thisAppSsdbDataMap.put(key, value);
                    }
                }
                logger.debug("LidAndUserFirstEndService app ssdb获取之前已存入的data thisAppSsdbDataMap : {} ",thisAppSsdbDataMap);

            }
        }

        // 获取取 zgid对应的广告数据
        setAdMaps(msgs, thisMaxCtKeySet, thisMaxCtMap, thisDataMap, thisLIdLnameMap, thisAppSsdbMaxCtMap, thisAppSsdbDataMap);

        if (!thisMaxCtKeySet.isEmpty()) {
            //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}"，ct）
            List<KeyValue<String, String>> keyValues = redisClient.syncMGet(thisMaxCtKeySet.toArray(new String[0]));
            for (KeyValue<String, String> keyValue : keyValues) {
                String key = keyValue.getKey();
                String value = keyValue.getValue();
                if (value != null) {
                    lastSsdbMaxCtMap.put(key, value);
                }
            }
            logger.debug("LidAndUserFirstEndService web+app ssdb获取之前已存入的MaxCt lastSsdbMaxCtMap : {} ",lastSsdbMaxCtMap);

            lastSsdbMaxCtMap.forEach((ctKey, maxCt) -> {
                lastLIdLnameKeySet.add(ctKey+":"+maxCt);
            });

            if (!lastLIdLnameKeySet.isEmpty()) {
                //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
                keyValues = redisClient.syncMGet(lastLIdLnameKeySet.toArray(new String[0]));
                for (KeyValue<String, String> keyValue : keyValues) {
                    String key = keyValue.getKey();
                    String value = keyValue.getValue();
                    if (value != null) {
                        lastLIdLnameMap.put(key, value);
                    }
                }
                logger.debug("LidAndUserFirstEndService web+app ssdb获取之前已存入的LIdLname lastLIdLnameMap : {} ",lastLIdLnameMap);
            }
        }

        //更新ssdb 最大ct
        // 更新ssdb中 用户最近的ct
        if (!thisMaxCtMap.isEmpty()) {
            logger.debug("LidAndUserFirstEndService web+app thisMaxCtMap : {} ",thisMaxCtMap);
            Set<Map.Entry<String, Long>> entries = thisMaxCtMap.entrySet();
            Iterator<Map.Entry<String, Long>> iterator = entries.iterator();
            while (iterator.hasNext()){
                Map.Entry<String, Long> next = iterator.next();
                String appZgidKey = next.getKey();
                Long thisMaxCt = next.getValue();
                String ctKey = SSDB_CT_PRE+":"+appZgidKey;
                if (!lastSsdbMaxCtMap.containsKey(ctKey) || (thisMaxCt >= Long.parseLong(lastSsdbMaxCtMap.get(ctKey)))) {
                    updateForUserPrKeySet.add(appZgidKey);
                    String dataKey = SSDB_AD_DATA_PRE+":"+appZgidKey+":"+thisMaxCt;
                    String lidLnameKey = SSDB_CT_PRE+":"+appZgidKey+":"+thisMaxCt;
                    long lastMaxCt = 0L;
                    String lastLidLnames = "";
                    if (lastSsdbMaxCtMap.containsKey(ctKey)) {
                        lastMaxCt = Long.parseLong(lastSsdbMaxCtMap.get(ctKey));
                        if (lastLIdLnameMap.containsKey(ctKey+":"+lastMaxCt)) {
                            lastLidLnames = lastLIdLnameMap.get(ctKey+":"+lastMaxCt);
                        }
                    }

                    //更新ssdb 最大ct
                    redisClient.syncSet(ctKey, thisMaxCt + "");

                    //更新ssdb 最大ct对应的广告信息
                    redisClient.syncSet(dataKey, thisDataMap.get(appZgidKey+":"+thisMaxCt));

                    //更新ssdb 最大ct对应的lid lname
                    String thisLidLname = thisLIdLnameMap.get(appZgidKey+":"+thisMaxCt);
                    logger.debug("LidAndUserFirstEndService web+app thisLIdLnameMap 获取 {} ",thisLidLname);

                    if (!StringUtils.isEmpty(lastLidLnames)) {
                        String lastLidLname = lastLidLnames.split("::")[0];
                        if (!lastLidLname.equals(thisLidLname)) {
                            thisLidLname = lastLidLname+"::"+thisLidLname;
                        }
                    }
                    redisClient.syncSet(lidLnameKey, thisLidLname);
                    logger.debug("LidAndUserFirstEndService web+app 存入ssdb lidLnameKey: {},lidLnames: {}",lidLnameKey,thisLidLname);

                    JSONObject dataJson = new JSONObject();
                    dataJson.put("key_ad_time", ctKey);
                    dataJson.put("key_ad_data", dataKey);
                    dataJson.put("key_ad_lid", lidLnameKey);
                    dataJson.put("value_ad_time", thisMaxCt + "");
                    dataJson.put("value_ad_data", thisDataMap.get(appZgidKey+":"+thisMaxCt));
                    dataJson.put("value_ad_lid", thisLidLname);
                    JSONObject jsonObj = new JSONObject();
                    jsonObj.put("tableName", "toufang_ad_click");
                    jsonObj.put("sinkType", "kudu");
                    jsonObj.put("data", dataJson.toJSONString().replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}"));
                    String kafkaMsg = jsonObj.toJSONString().replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}");
                    ZGMessage msg = msgs.get(0);
                    List<String> advKafkaMsgList = msg.getAdvKafkaMsgList();
                    if(advKafkaMsgList == null)
                        msg.setAdvKafkaMsgList(advKafkaMsgList=new ArrayList<>());
                    advKafkaMsgList.add(kafkaMsg);

                    //删除过期无效的数据
                    if (thisMaxCt > lastMaxCt) {
                        thisDelKeySet.add(SSDB_AD_DATA_PRE+":"+appZgidKey+":"+lastMaxCt);
                        thisDelKeySet.add(SSDB_CT_PRE+":"+appZgidKey+":"+lastMaxCt);
                    }
                }
            }
            //删除过期无效数据
            if (!thisDelKeySet.isEmpty()) {
                redisClient.asyncDel(thisDelKeySet.toArray(new String[0]));
            }
        }

        logger.debug("LidAndUserFirstEndService web+app 用户属性变更 updateForUserPrKeySet：{} ",updateForUserPrKeySet);

        if (!thisMaxCtKeySet.isEmpty()) {
            //ssdb 获取 更新后的（s"${ssdbCtPre}:${appZgidKey}"，ct）
            List<KeyValue<String, String>> keyValues = redisClient.syncMGet(thisMaxCtKeySet.toArray(new String[0]));
            for (KeyValue<String, String> keyValue : keyValues) {
                String key = keyValue.getKey();
                String value = keyValue.getValue();
                if (value != null) {
                    updateSsdbMaxCtMap.put(key, value);
                }
            }
            logger.debug("LidAndUserFirstEndService web+app ssdb获取更新后的MaxCt updateSsdbMaxCtMap：{} ",updateSsdbMaxCtMap);

            Set<Map.Entry<String, String>> entrySet = updateSsdbMaxCtMap.entrySet();
            Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();
            while (iterator.hasNext()){
                Map.Entry<String, String> entry = iterator.next();
                String ctKey = entry.getKey();
                String maxCt = entry.getValue();
                updateLIdLnameKeySet.add( ctKey+":"+ maxCt);
            }

            if (!updateLIdLnameKeySet.isEmpty()) {
                //ssdb 获取 更新后的（s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
                keyValues = redisClient.syncMGet(updateLIdLnameKeySet.toArray(new String[0]));
                for (KeyValue<String, String> keyValue : keyValues) {
                    String key = keyValue.getKey();
                    String value = keyValue.getValue();
                    if (value != null) {
                        updateLIdLnameMap.put(key, value);
                    }
                }
                logger.debug("LidAndUserFirstEndService web+app ssdb更新后的 updateLIdLnameMap：{} ",updateLIdLnameMap);
            }
        }

        // 带lid的数据 才添加用户属性 （首次投放链接 、后续投放链接）
        // 事件属性 data.pr：  不带lid的需要加上lid
        addUserProAndEventPro(msgs, updateSsdbMaxCtMap, updateLIdLnameMap, updateForUserPrKeySet);

        for (ZGMessage msg : msgs) {
            collector.collect(msg);
        }
    }

    /**
     * 增加用户属性（首次、后续）、事件属性（lid）
     */
    private void addUserProAndEventPro(
            List<ZGMessage> msgs,
            Map<String, String> updateSsdbMaxCtMap,
            Map<String, String> updateLIdLnameMap,
            Set<String> updateForUserPrKeySet) {

        for (ZGMessage msg : msgs) {
            try {
                if (msg.getResult() != -1) {
                    Map<String, Object> data = msg.getData();
                    String ak = (String) data.get("ak");
                    String pl = (String) data.get("pl");
                    Integer appId = msg.getAppId();

                    // 处理来自web端+APP端的广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
                    if (advertisingMap.containsKey(ak)) {
                        Object tz = data.get("tz");
                        List<Map<String, Object>> dataList = (List<Map<String, Object>>) data.get("data");
                        Iterator<Map<String, Object>> dataItemIterator = dataList.iterator();

                        JSONObject lidLnameJsonObj = new JSONObject();
                        lidLnameJsonObj.put("firstLid", -1);
                        lidLnameJsonObj.put("firstLname", "");
                        lidLnameJsonObj.put("folLid", -1);
                        lidLnameJsonObj.put("folLname", "");

                        boolean hasAddUsrPr = false;
                        // $zg_did $zg_zgid $ct
                        long thisZgdid = -1L;
                        long zgid = -1L;
                        long thisCt = -1L;
                        int lastLid = -1;
                        JSONObject admsJson = new JSONObject();

                        while (dataItemIterator.hasNext()) {
                            Map<String, Object> dataItem = dataItemIterator.next();
                            String dt = (String) dataItem.get("dt");

                            // TODO 添加事件属性 lid
                            if ("evt".equals(dt)) {
                                Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");

                                thisZgdid = Long.parseLong(String.valueOf(props.getOrDefault("$zg_did", "-1")));
                                thisCt = Long.parseLong(String.valueOf(props.getOrDefault("$ct", "-1")));

                                zgid = (Long) props.get("$zg_zgid");

                                // 获取用户首次后续 lid lname
                                if (StringUtils.isEmpty(lidLnameJsonObj.getString("firstLname"))) {
                                    Tuple2<Integer, JSONObject> tuple = getUserLastLidLname(
                                            appId, zgid, lidLnameJsonObj,
                                            updateSsdbMaxCtMap, updateLIdLnameMap, thisCt);
                                    lastLid = tuple.f0;
                                    admsJson = tuple.f1;
                                }
                                logger.debug("LidAndUserFirstEndService evt 添加事件属性：{} ",lastLid);

                                // 添加用户属性、事件属性 lid
                                if (lastLid != -1) {
                                    // 使用匹配到的最近的lid
                                    props.put(LID_NAME, lastLid);
                                    String channelType = admsJson.getString("channel_type");
                                    String channelAccountId = admsJson.getString("channel_account_id");
                                    long channelCampaignId = admsJson.getLongValue("channel_campaign_id");
                                    long channelAdgroupIdN = admsJson.getLongValue("channel_adgroup_id");
                                    long channelAdId = admsJson.getLongValue("channel_ad_id");
                                    long channelKeywordId = admsJson.getLongValue("channel_keyword_id");

                                    if (!StringUtils.isEmpty(channelType)) {
                                        props.put(CHANNEL_TYPE_NAME, channelType);
                                        props.put(CHANNEL_TYPE_NAME_STR, getchannelTypeNameStr(channelType));
                                    }
                                    if (!StringUtils.isEmpty(channelAccountId)) {
                                        props.put(CHANNEL_ACCOUNT_ID_NAME, channelType + "$" + channelAccountId + "");
                                    }
                                    if (channelCampaignId != 0L) {
                                        props.put(CHANNEL_CAMPAIGN_ID_NAME, channelType + "$" + channelCampaignId + "");
                                    }
                                    if (channelAdgroupIdN != 0L) {
                                        props.put(CHANNEL_ADGROUP_ID_NAME, channelType + "$" + channelAdgroupIdN + "");
                                    }
                                    if (channelAdId != 0L) {
                                        props.put(CHANNEL_AD_ID_NAME, channelType + "$" + channelAdId + "");
                                    }
                                    if (channelKeywordId != 0L) {
                                        props.put(CHANNEL_KEYWORD_ID_NAME, channelType + "$" + channelKeywordId + "");
                                    }
                                    logger.debug("LidAndUserFirstEndService web+app 添加事件属性：{} ",lastLid);
                                }
                            }

                            // TODO 添加用户属性
                            if ("usr".equals(dt)) {
                                Map<String, Object> usrPr = (Map<String, Object>) dataItem.get("pr");
                                thisZgdid = usrPr.get("$zg_did") == null ? -1L : (Long) usrPr.get("$zg_did");
                                thisCt = usrPr.get("$ct") == null ? -1L : (Long) usrPr.get("$ct");

                                zgid = (Long) usrPr.get("$zg_zgid");

                                // 获取用户首次后续 lid lname
                                if (StringUtils.isEmpty(lidLnameJsonObj.getString("firstLname"))) {
                                    Tuple2<Integer, JSONObject> tuple = getUserLastLidLname(
                                            appId, zgid, lidLnameJsonObj,
                                            updateSsdbMaxCtMap, updateLIdLnameMap, thisCt);
                                    lastLid = tuple.f0;
                                    admsJson = tuple.f1;
                                }

                                String firstLname = lidLnameJsonObj.getString("firstLname");
                                if (!StringUtils.isEmpty(firstLname)) {
                                    hasAddUsrPr = true;
                                    usrPr.put(USER_FIRST_LID_NAME + "lid", lidLnameJsonObj.getLong("firstLid"));
                                    usrPr.put(USER_FIRST_LID_NAME, firstLname);

                                    logger.debug("LidAndUserFirstEndService web+app 在已有的usr下添加用户属性，userFirstPrName：{} ，lid：{} ",USER_FIRST_LID_NAME, lidLnameJsonObj.getLong("firstLid"));

                                    String folLname = lidLnameJsonObj.getString("folLname");
                                    if (!StringUtils.isEmpty(folLname)) {
                                        usrPr.put(USER_FOLLOW_LID_NAME + "lid", lidLnameJsonObj.getLong("folLid"));
                                        usrPr.put(USER_FOLLOW_LID_NAME, folLname);
                                        logger.debug("LidAndUserFirstEndService web+app 在已有的usr下添加用户属性，userFolPrName：{} ，lid：{} ",USER_FOLLOW_LID_NAME,lidLnameJsonObj.getLong("folLid"));
                                    }
                                }
                            }

                            // TODO 会话事件匹配上app端广告信息，变更后需更新用户属性
                            if ("ss".equals(dt) || "adtf".equals(dt)) {
                                Map<String, Object> usrPr = (Map<String, Object>) dataItem.get("pr");
                                thisZgdid = usrPr.get("$zg_did") == null ? -1L : (Long) usrPr.get("$zg_did");
                                thisCt = usrPr.get("$ct") == null ? -1L : (Long) usrPr.get("$ct");
                                zgid = (Long) usrPr.get("$zg_zgid");

                                // 获取用户首次后续 lid lname
                                if (updateForUserPrKeySet.contains(appId + ":" + zgid) &&
                                        StringUtils.isEmpty(lidLnameJsonObj.getString("firstLname"))) {
                                    Tuple2<Integer, JSONObject> tuple = getUserLastLidLname(
                                            appId, zgid, lidLnameJsonObj,
                                            updateSsdbMaxCtMap, updateLIdLnameMap, thisCt);
                                    lastLid = tuple.f0;
                                    admsJson = tuple.f1;
                                }
                            }
                        }

                        // TODO 手动添加用户属性
                        if (!hasAddUsrPr && updateForUserPrKeySet.contains(appId + ":" + zgid)) {
                            // 添加了事件属性未添加用户属性，需手动添加dt=usr
                            // 先获取 $zg_uid
                            String zgUid = redisClient.syncHGet("zu:" + appId, zgid + "");
                            logger.debug("LidAndUserFirstEndService 添加用户属性 获取 " + "key: zu:" + appId + " hashKey: " + zgid + " 的 uid：" + zgUid);

                            Map<String, Object> map = new HashMap<>();
                            // $zg_did $zg_zgid $ct
                            map.put("dt", "usr");
                            JSONObject jsonObj = new JSONObject();

                            if (!StringUtils.isEmpty(zgUid)) {
                                jsonObj.put("$zg_uid", Long.parseLong(zgUid));
                            }
                            jsonObj.put("$tz", tz);
                            jsonObj.put("$zg_zgid", zgid);
                            if (thisZgdid != -1L) {
                                jsonObj.put("$zg_did", thisZgdid);
                            }
                            if (thisCt != -1L) {
                                jsonObj.put("$ct", thisCt);
                            }

                            String firstLname = lidLnameJsonObj.getString("firstLname");
                            if (!StringUtils.isEmpty(firstLname)) {
                                jsonObj.put(USER_FIRST_LID_NAME + "lid", lidLnameJsonObj.getLong("firstLid"));
                                jsonObj.put(USER_FIRST_LID_NAME, firstLname);

                                logger.debug("LidAndUserFirstEndService web+app 在新增的usr下添加用户属性，userFirstPrName：{} ，lid：{} ",USER_FIRST_LID_NAME, lidLnameJsonObj.getLong("firstLid"));

                                String folLname = lidLnameJsonObj.getString("folLname");
                                if (!StringUtils.isEmpty(folLname)) {
                                    jsonObj.put(USER_FOLLOW_LID_NAME + "lid", lidLnameJsonObj.getLong("folLid"));
                                    jsonObj.put(USER_FOLLOW_LID_NAME, folLname);
                                    logger.debug("LidAndUserFirstEndService web+app 在新增的usr下添加用户属性，userFolPrName：{} ，lid：{} ",USER_FOLLOW_LID_NAME,lidLnameJsonObj.getLong("folLid"));
                                }
                            }

                            map.put("pr", jsonObj);
                            dataList.add(map);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("异常的消息为:---------> " + msg.toString());
            }
        }
    }

    /**
     * 广告渠道名称映射
     */
    public String getchannelTypeNameStr(String channelType) {
        if (channelType == null) {
            return "";
        }

        switch (channelType) {
            case "1": return "百度信息流";
            case "2": return "巨量引擎";
            case "3": return "腾讯广告";
            case "4": return "百度搜索";
            case "5": return "苹果ASA";
            case "9": return "知乎营销";
            case "10": return "快手广告";
            case "11": return "微博-超级粉丝通";
            case "12": return "华为广告";
            case "13": return "小米营销";
            case "14": return "VIVO营销平台";
            case "15": return "OPPO营销平台";
            default: return "";
        }
    }

    /**
     * 获取用户首次和后续lid lname
     */
    private Tuple2<Integer, JSONObject> getUserLastLidLname(
            Integer appId,
            Long zgid,
            JSONObject jsonObj,
            Map<String, String> updateSsdbMaxCtMap,
            Map<String, String> updateLIdLnameMap,
            Long ct) {

        int lastLid = -1;
        String ctKey = SSDB_CT_PRE + ":" + appId + ":" + zgid;
        logger.debug("LidAndUserFirstEndService ctKey: {}",ctKey);

        String dataPreKey = SSDB_AD_DATA_PRE + ":" + appId + ":" + zgid;
        String lidLnames = "";
        long maxCt = 0L;

        if (updateSsdbMaxCtMap.containsKey(ctKey)) {
            String maxCtStr = updateSsdbMaxCtMap.get(ctKey);
            if (!StringUtils.isEmpty(maxCtStr)) {
                try {
                    maxCt = Long.parseLong(maxCtStr);
                    lidLnames = updateLIdLnameMap.get(ctKey + ":" + maxCtStr);
                } catch (NumberFormatException e) {
                    // 记录日志或处理异常
                    logger.debug("print:LidAndUserFirstEndService maxCtStr转换失败：{}",maxCtStr);
                }
            }
        }

        if (!StringUtils.isEmpty(lidLnames)) {
            // 添加用户属性 首次（lid lname） 后续（lid lname）
            String[] lidLname = lidLnames.split("::");
            String[] arrOne = lidLname[0].split(",");

            if (arrOne.length >= 2) {
                try {
                    int first = Integer.parseInt(arrOne[0]);
                    jsonObj.put("firstLid", first);
                    jsonObj.put("firstLname", arrOne[1]);
                    lastLid = first;

                    if (lidLname.length >= 2) {
                        String[] arrTwo = lidLname[1].split(",");
                        if (arrTwo.length >= 2) {
                            int last = Integer.parseInt(arrTwo[0]);
                            jsonObj.put("folLid", last);
                            jsonObj.put("folLname", arrTwo[1]);
                            lastLid = last;
                        }
                    }
                } catch (NumberFormatException e) {
                    logger.debug("LidAndUserFirstEndService lidLname解析失败：{}",lidLnames);
                }
            }
        }

        // 事件时间需大于广告时间 匹配事件不设窗口期 && ((ct - maxCt) <= expireTime * 1000)
        if (ct != null && ct > 0 && ct >= maxCt && lastLid != -1) {
            // TODO 因明细数据量较大故单条获取
            String adms = redisClient.syncGet(dataPreKey + ":" + maxCt);
            logger.debug("LidAndUserFirstEndService 广告数据key dataPreKey: {} , 获取的广告数据adms:{}",dataPreKey + ":" + maxCt,adms);

            if (!StringUtils.isEmpty(adms)) {
                try {
                    JSONObject admsJson = JSON.parseObject(adms);
                    return Tuple2.of(lastLid, admsJson);
                } catch (Exception e) {
                    logger.debug("LidAndUserFirstEndService 广告数据JSON解析失败：{}",adms);
                    return Tuple2.of(-1, null);
                }
            } else {
                return Tuple2.of(-1, null);
            }
        } else {
            logger.debug("LidAndUserFirstEndService 事件时间早于广告时间/没有广告 ct ：{} ,maxCt:{}",ct,maxCt);
            return Tuple2.of(-1, null);
        }
    }

    /**
     * 获取web端 app端对应的广告数据: zgid
     */
    private void setAdMaps(
            List<ZGMessage> msgs,
            Set<String> thisMaxCtKeySet,
            Map<String, Long> thisMaxCtMap,
            Map<String, String> thisDataMap,
            Map<String, String> thisLIdLnameMap,
            Map<String, String> thisAppSsdbMaxCtMap,
            Map<String, String> thisAppSsdbDataMap) {

        Set<String> thisDelKeySet = new HashSet<>();

        msgs.stream().filter(msg -> msg.getResult() != -1).forEach(msg -> {
            Map<String, Object> data = msg.getData();
            String ak = (String) data.get("ak");
            String pl = (String) data.get("pl");
            String sdk = (String) data.get("sdk");
            Integer appId = msg.getAppId();

            // 处理来自web端+APP端的广告投放数据
            if (advertisingMap.containsKey(ak)) {
                List<Map<String, Object>> dataList = (List<Map<String, Object>>) data.get("data");
                if (dataList != null) {
                    for (Map<String, Object> dataItem : dataList) {
                        String dt = (String) dataItem.get("dt");

                        // 处理usr和evt事件
                        if ("usr".equals(dt) || "evt".equals(dt)) {
                            Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");
                            if (props != null) {
                                Long zgid = (Long) props.get("$zg_zgid");
                                String appZgidKey = appId + ":" + zgid;
                                thisMaxCtKeySet.add(SSDB_CT_PRE + ":" + appZgidKey);
                            }
                        }

                        // web端 +苹果ASA 广告信息
                        if ("adtf".equals(dt) && !"zg_adtoufang".equals(sdk)) {
                            processWebAndAppleASA(dataItem, appId+"", ak, sdk, msg,
                                    thisDelKeySet, thisMaxCtKeySet, thisMaxCtMap, thisDataMap,
                                    thisLIdLnameMap);
                        }

                        // TODO app端广告信息
                        if ("ss".equals(dt)) {
                            processAppSS(dataItem, msg, appId+"", thisDelKeySet, thisMaxCtKeySet,
                                    thisMaxCtMap, thisDataMap, thisLIdLnameMap,
                                    thisAppSsdbMaxCtMap, thisAppSsdbDataMap);
                        }
                    }
                }
            }
        });

        //删除已匹配上ss的过期广告数据
        if (!thisDelKeySet.isEmpty()) {
            logger.debug("LidAndUserFirstEndService 删除已匹配上ss的广告数据thisDelKeySet : {} ",thisDelKeySet);
            redisClient.syncDel(thisDelKeySet.toArray(new String[0]));
        }
    }

    /**
     * 处理web端和苹果ASA广告
     */
    private void processWebAndAppleASA(
            Map<String, Object> dataItem,
            String appId,
            String ak,
            String sdk,
            ZGMessage msg,
            Set<String> thisDelKeySet,
            Set<String> thisMaxCtKeySet,
            Map<String, Long> thisMaxCtMap,
            Map<String, String> thisDataMap,
            Map<String, String> thisLIdLnameMap) {

        Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");
        if (props == null) {
            return;
        }

        Long ct = 0L;
        if (props.get("$ct") != null) {
            ct = Long.parseLong(String.valueOf(props.get("$ct")));
        }
        String webAd = props.get("$landing_url") == null ? "" : (String) props.get("$landing_url");
        String appleAsaAd = props.get("$apple_ad") == null ? "" : (String) props.get("$apple_ad");
        int appleAdChannel = props.get("$channel_type") == null ? -1 : (Integer) props.get("$channel_type");

        Long zgid = (Long) props.get("$zg_zgid");
        String appZgidKey = appId + ":" + zgid;
        Map<String, Object> adMap = new HashMap<>();

        // web端广告唯一点击标识字段
        if (webAd.contains("lid")) {
            adMap = ToolUtil.urlParseToMap(webAd);
        }

        // 苹果ASA广告
        if (appleAdChannel == 5) {
            JSONObject appleAdJson = new JSONObject();
            if (appleAsaAd.contains("iad-")) {
                // 10.0以上到14.3的苹果广告数据需进行转换
                appleAdJson = appleAdDataTransfer(appleAsaAd);
            } else {
                try {
                    appleAdJson = JSON.parseObject(appleAsaAd);
                } catch (Exception e) {
                    logger.error("$apple_ad json解析异常", e);
                }
            }

            if (!appleAdJson.isEmpty()) {
                adMap = appleASAFeildsMap(appleAdJson);

                String clickDateStr = appleAdJson.getString("clickDate");
                Long adClickDate = ToolUtil.dateUsStrToTimestamp(clickDateStr);
                if (adClickDate > 0L) {
                    ct = adClickDate;
                }
            }
        }

        if (!adMap.isEmpty()) {
            int channelType = Integer.parseInt(String.valueOf(adMap.get("channel_type")));
            boolean hashAdData = false;
            boolean isTengXunWeb = false;

            if (channelType == 3) {
                // 腾讯web需要单独匹配广告信息
                String muidProcess = null;
                if (adMap.containsKey("qz_gdt")) {
                    muidProcess = String.valueOf(adMap.get("qz_gdt"));
                } else if (adMap.containsKey("gdt_vid")) {
                    muidProcess = String.valueOf(adMap.get("gdt_vid"));
                }

                if (muidProcess != null) {
                    String maxCtKey = APP_PRE + ":" + appId + ":" + muidProcess;
                    String maxCt = redisClient.syncGet(maxCtKey);

                    if (StringUtils.isNotEmpty(maxCt)) {
                        String admsJsonStrKey = maxCtKey + ":" + maxCt;
                        String admsJsonStr = redisClient.syncGet(admsJsonStrKey);

                        if (StringUtils.isNotEmpty(admsJsonStr)) {
                            thisMaxCtKeySet.add(SSDB_CT_PRE + ":" + appZgidKey);

                            // json转map
                            adMap = JsonUtil.mapFromJson(admsJsonStr);
                            hashAdData = true;
                            isTengXunWeb = true;

                            // TODO 删除已匹配上广告的腾讯web广告点击
                            JSONObject adms = JSON.parseObject(admsJsonStr);
                            String ipUaKey = adms.getString("ip_ua_key");
                            String channelClickIdKey = adms.getString("channel_click_id_key");
                            String appMaxCt = adms.getString("click_time");
                            String appZgidCtKey = appId + ":" + zgid + ":" + appMaxCt;

                            if (StringUtils.isNotEmpty(ipUaKey)) {
                                thisDelKeySet.add(ipUaKey);
                                thisDelKeySet.add(ipUaKey + ":" + appMaxCt);
                            }
                            if (StringUtils.isNotEmpty(channelClickIdKey)) {
                                thisDelKeySet.add(channelClickIdKey);
                                thisDelKeySet.add(channelClickIdKey + ":" + appMaxCt);
                            }

                            adms.put("zuge_ct_key", SSDB_CT_PRE + ":" + appZgidKey);
                            adms.put("zuge_data_key", SSDB_AD_DATA_PRE + ":" + appZgidCtKey);
                            adms.put("is_delete", "true");
                            adms.put("other_key", maxCtKey + ":" + maxCt);

                            JSONObject jsonObj = new JSONObject();
                            jsonObj.put("tableName", "toufang_ad_click");
                            jsonObj.put("sinkType", "kudu");
                            jsonObj.put("data", adms.toJSONString());

                            String key = appId + ":" + ipUaKey;
                            String value = jsonObj.toJSONString()
                                    .replace("\\\"", "\"")
                                    .replace("\"{", "{")
                                    .replace("}\"", "}");

                            List<String> advKafkaMsgList = msg.getAdvKafkaMsgList();
                            if(advKafkaMsgList == null)
                                msg.setAdvKafkaMsgList(advKafkaMsgList=new ArrayList<>());
                            advKafkaMsgList.add(value);
                        }
                    } else {
                        // 腾讯web端广告数据未匹配上的先存redis
                        String rawDataKey = APP_PRE + ":" + muidProcess + ":rawdata";
                        redisClient.syncSet(rawDataKey, msg.getRawData());
                        redisClient.expire(rawDataKey, tengxunWebExpireTime);
                        logger.debug("LidAndUserFirstEndService 腾讯web未匹配上广告:：{}",rawDataKey);
                    }
                }
            } else {
                thisMaxCtKeySet.add(SSDB_CT_PRE + ":" + appZgidKey);
                hashAdData = true;
            }

            if (adMap.containsKey("lid") && hashAdData) {
                String lid = String.valueOf(adMap.get("lid"));
                String lname = String.valueOf(adMap.get("lname"));
                String appZgidCtKey = appId + ":" + zgid + ":" + ct;

                Long currentMaxCt = thisMaxCtMap.get(appZgidKey);
                if (currentMaxCt == null || ct >= currentMaxCt) {
                    AdvMessage adms = new AdvMessage();
                    adms.setZg_appid(Long.parseLong(appId));
                    adms.setLid(Long.parseLong(lid));
                    adms.setCt(ct);
                    adms.setApp_key(ak);
                    adms.setFieldsWithout(adMap);
                    //百度和巨量的 需要单独将 $landing_url 赋值给 callback (腾讯的用lua脚本解析的广告)
                    if (!isTengXunWeb) {
                        adms.setCallback_url(webAd);
                    }

                    if (adMap.containsKey("baidu_token")) {
                        adms.setToken(String.valueOf(adMap.get("baidu_token")));
                    }

                    thisMaxCtMap.put(appZgidKey, ct);
                    thisDataMap.put(appZgidCtKey, adms.toJsonString());

                    logger.debug("LidAndUserFirstEndService web端 +苹果ASA 放入thisLIdLnameMap:{} value: {},{} ",appZgidCtKey, lid, lname);
                    thisLIdLnameMap.put(appZgidCtKey, lid + "," + lname);
                }
            }
        }
    }

    /**
     * 苹果ASA广告信息字段映射
     */
    public Map<String, Object> appleASAFeildsMap(JSONObject json) {
        Map<String, Object> map = new HashMap<>();

        // 固定字段
        map.put("lname", "苹果ASA");
        map.put("lid", "-2");
        map.put("channel_type", "5");

        // 从JSON中获取字段值
        map.put("channel_account_id", String.valueOf(json.getLongValue("orgId")));
        map.put("channel_campaign_id", String.valueOf(json.getLongValue("campaignId")));
        map.put("channel_adgroup_id", String.valueOf(json.getLongValue("adGroupId")));

        // 处理adId，如果为0则使用creativeSetId
        long adId = json.getLongValue("adId");
        if (adId == 0L) {
            long creativeSetId = json.getLongValue("creativeSetId");
            map.put("channel_ad_id", String.valueOf(creativeSetId));
        } else {
            map.put("channel_ad_id", String.valueOf(adId));
        }

        map.put("channel_keyword_id", String.valueOf(json.getLongValue("keywordId")));

        return map;
    }

    /**
     * 苹果ASA 10.0以上到14.3 转换为 14.3以上广告格式
     */
    public JSONObject appleAdDataTransfer(String adStr) {
        JSONObject json = new JSONObject();

        if (adStr == null || adStr.trim().isEmpty()) {
            return json;
        }

        if (adStr.contains("=")) {
            // 格式：iad-org-id=123;iad-campaign-id=456;...
            String[] arr = adStr.split(";");

            for (String value : arr) {
                if (value.trim().isEmpty()) {
                    continue;
                }

                String[] str = value.split("=");
                if (str.length < 2) {
                    continue;
                }

                String key = str[0].trim();
                String val = str[1].trim();

                // 移除引号
                val = val.replace("\"", "").trim();

                if (key.contains("iad-org-id")) {
                    try {
                        json.put("orgId", Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        logger.error("iad-org-id格式错误: " + val);
                    }
                } else if (key.contains("iad-campaign-id")) {
                    try {
                        json.put("campaignId", Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        logger.error("iad-campaign-id格式错误: " + val);
                    }
                } else if (key.contains("iad-adgroup-id")) {
                    try {
                        json.put("adGroupId", Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        logger.error("iad-adgroup-id格式错误: " + val);
                    }
                } else if (key.contains("iad-keyword-id")) {
                    try {
                        json.put("keywordId", Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        logger.error("iad-keyword-id格式错误: " + val);
                    }
                } else if (key.contains("iad-ad-id")) {
                    try {
                        json.put("adId", Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        logger.error("iad-ad-id格式错误: " + val);
                    }
                } else if (key.contains("iad-click-date")) {
                    json.put("clickDate", val);
                }
            }
        } else {
            // 格式：JSON格式
            try {
                JSONObject jsonObj = JSON.parseObject(adStr);

                // 处理每个字段，提供默认值
                json.put("orgId", jsonObj.getLongValue("iad-org-id"));
                json.put("campaignId", jsonObj.getLongValue("iad-campaign-id"));
                json.put("adGroupId", jsonObj.getLongValue("iad-adgroup-id"));
                json.put("keywordId", jsonObj.getLongValue("iad-keyword-id"));
                json.put("adId", jsonObj.getLongValue("iad-ad-id"));
                json.put("clickDate", jsonObj.getString("iad-click-date"));

            } catch (Exception e) {
                logger.error("$apple_ad json解析异常: " + adStr, e);
            }
        }

        return json;
    }

    /**
     * 处理app端SS事件
     */
    private void processAppSS(
            Map<String, Object> dataItem,
            ZGMessage msg,
            String appId,
            Set<String> thisDelKeySet,
            Set<String> thisMaxCtKeySet,
            Map<String, Long> thisMaxCtMap,
            Map<String, String> thisDataMap,
            Map<String, String> thisLIdLnameMap,
            Map<String, String> thisAppSsdbMaxCtMap,
            Map<String, String> thisAppSsdbDataMap) {

        Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");
        if (props == null) {
            return;
        }

        Map<String, Object> msgData = msg.getData();
        String ip = (String) msgData.get("ip");
        String ua = (String) msgData.get("ua");

        String os = String.valueOf(props.getOrDefault("$os", ""));
        // ov变量在Java版本中未使用，注释掉
        // String ov = String.valueOf(props.getOrDefault("$ov", ""));

        String appProcessKey = "";
        long appMaxCt = -1L;

        // 精确匹配
        String idfaOrigin = String.valueOf(props.getOrDefault("$idfa", ""));
        String imeiOrigin = String.valueOf(props.getOrDefault("$imei", ""));
        String androidIdOrigin = String.valueOf(props.getOrDefault("$android_id", ""));
        String oaidOrigin = String.valueOf(props.getOrDefault("$oaid", ""));

        String idfa = processDeviceId(idfaOrigin);
        String imei = processDeviceId(imeiOrigin);
        String androidId = processDeviceId(androidIdOrigin);
        String oaid = processDeviceId(oaidOrigin);

        Long zgid = Long.parseLong(String.valueOf(props.get("$zg_zgid")));
        String appZgidKey = appId + ":" + zgid;

        if ("iOS".equals(os)) {
            // iOS系统: muid
            if (StringUtils.isNotEmpty(idfa)) {
                String processKey = APP_PRE + ":" + appId + ":" + idfa;
                if (thisAppSsdbMaxCtMap.containsKey(processKey) && !thisDelKeySet.contains(processKey)) {
                    long maxCt = Long.parseLong(thisAppSsdbMaxCtMap.get(processKey));
                    if (maxCt > appMaxCt) {
                        appMaxCt = maxCt;
                        appProcessKey = processKey;
                    }
                }
            }
        } else {
            boolean hasAndroidDevice = false;

            // 安卓系统：调整顺序 oaid>android_id>imei
            if (StringUtils.isNotEmpty(oaid) && !hasAndroidDevice) {
                String processKey = APP_PRE + ":" + appId + ":" + oaid;
                if (thisAppSsdbMaxCtMap.containsKey(processKey) && !thisDelKeySet.contains(processKey)) {
                    long maxCt = Long.parseLong(thisAppSsdbMaxCtMap.get(processKey));
                    if (maxCt > appMaxCt) {
                        appMaxCt = maxCt;
                        appProcessKey = processKey;
                        hasAndroidDevice = true;
                    }
                }
            }

            if (StringUtils.isNotEmpty(androidId) && !hasAndroidDevice) {
                String processKey = APP_PRE + ":" + appId + ":" + androidId;
                if (thisAppSsdbMaxCtMap.containsKey(processKey) && !thisDelKeySet.contains(processKey)) {
                    long maxCt = Long.parseLong(thisAppSsdbMaxCtMap.get(processKey));
                    if (maxCt > appMaxCt) {
                        appMaxCt = maxCt;
                        appProcessKey = processKey;
                        hasAndroidDevice = true;
                    }
                }
            }

            if (StringUtils.isNotEmpty(imei) && !hasAndroidDevice) {
                String processKey = APP_PRE + ":" + appId + ":" + imei;
                if (thisAppSsdbMaxCtMap.containsKey(processKey) && !thisDelKeySet.contains(processKey)) {
                    long maxCt = Long.parseLong(thisAppSsdbMaxCtMap.get(processKey));
                    if (maxCt > appMaxCt) {
                        appMaxCt = maxCt;
                        appProcessKey = processKey;
                        hasAndroidDevice = true;
                    }
                }
            }
        }

        if (StringUtils.isEmpty(appProcessKey)) {
            // 模糊匹配
            String uaProcess = ToolUtil.uaAnalysis(ua);
            String ipUaKey = APP_PRE + ":" + appId + ":" + ip + uaProcess;

            logger.debug("LidAndUserFirstEndService thisAppSsdbMaxCtMap:{}",ipUaKey);

            if (thisAppSsdbMaxCtMap.containsKey(ipUaKey) && !thisDelKeySet.contains(ipUaKey)) {
                appMaxCt = Long.parseLong(thisAppSsdbMaxCtMap.get(ipUaKey));
                appProcessKey = ipUaKey;
            }

            // iOS的广告需同时匹配 ip+version
            if ("iOS".equals(os)) {
                String[] arr = uaProcess.split(":");
                if (arr.length > 1) {
                    String version = ":" + arr[1];
                    String ipVersionKey = APP_PRE + ":" + appId + ":" + ip + version;
                    if (thisAppSsdbMaxCtMap.containsKey(ipVersionKey) && !thisDelKeySet.contains(ipVersionKey)) {
                        long maxCt = Long.parseLong(thisAppSsdbMaxCtMap.get(ipVersionKey));
                        if (maxCt > appMaxCt) {
                            appMaxCt = maxCt;
                            appProcessKey = ipVersionKey;
                        }
                    }
                }
            }
        }

        if (appMaxCt != -1L) {
            String appDataKey = appProcessKey + ":" + appMaxCt;
            if (thisAppSsdbDataMap.containsKey(appDataKey)) {
                String appZgidCtKey = appId + ":" + zgid + ":" + appMaxCt;
                thisMaxCtKeySet.add(SSDB_CT_PRE + ":" + appZgidKey);

                String admsJsonStr = thisAppSsdbDataMap.get(appDataKey);
                JSONObject adms = JSON.parseObject(admsJsonStr);

                // 删除已匹配上ss的广告
                String ipUaKey = adms.getString("ip_ua_key");
                String muidKey = adms.getString("muid_key");
                String idfaKey = adms.getString("idfa_key");
                String imeiKey = adms.getString("imei_key");
                String androidIdKey = adms.getString("android_id_key");
                String oaidKey = adms.getString("oaid_key");

                if (StringUtils.isNotEmpty(ipUaKey)) {
                    thisDelKeySet.add(ipUaKey);
                    thisDelKeySet.add(ipUaKey + ":" + appMaxCt);
                }
                if (StringUtils.isNotEmpty(muidKey)) {
                    thisDelKeySet.add(muidKey);
                    thisDelKeySet.add(muidKey + ":" + appMaxCt);
                }
                if (StringUtils.isNotEmpty(idfaKey)) {
                    thisDelKeySet.add(idfaKey);
                    thisDelKeySet.add(idfaKey + ":" + appMaxCt);
                }
                if (StringUtils.isNotEmpty(imeiKey)) {
                    thisDelKeySet.add(imeiKey);
                    thisDelKeySet.add(imeiKey + ":" + appMaxCt);
                }
                if (StringUtils.isNotEmpty(androidIdKey)) {
                    thisDelKeySet.add(androidIdKey);
                    thisDelKeySet.add(androidIdKey + ":" + appMaxCt);
                }
                if (StringUtils.isNotEmpty(oaidKey)) {
                    thisDelKeySet.add(oaidKey);
                    thisDelKeySet.add(oaidKey + ":" + appMaxCt);
                }

                adms.put("zuge_ct_key", SSDB_CT_PRE + ":" + appZgidKey);
                adms.put("zuge_data_key", SSDB_AD_DATA_PRE + ":" + appZgidCtKey);
                adms.put("is_delete", "true");
                adms.put("other_key", appDataKey);

                JSONObject jsonObj = new JSONObject();
                jsonObj.put("tableName", "toufang_ad_click");
                jsonObj.put("sinkType", "kudu");
                jsonObj.put("data", adms.toJSONString());

                String key = appId + ":" + ipUaKey;
                String value = jsonObj.toJSONString()
                        .replace("\\\"", "\"")
                        .replace("\"{", "{")
                        .replace("}\"", "}");
                List<String> advKafkaMsgList = msg.getAdvKafkaMsgList();
                if(advKafkaMsgList == null)
                    msg.setAdvKafkaMsgList(advKafkaMsgList=new ArrayList<>());
                advKafkaMsgList.add(value);

                Long lid = Long.parseLong(String.valueOf(adms.get("lid")));
                String lname = String.valueOf(adms.get("lname"));

                Long currentMaxCt = thisMaxCtMap.get(appZgidKey);
                if (currentMaxCt == null || appMaxCt >= currentMaxCt) {
                    thisMaxCtMap.put(appZgidKey, appMaxCt);
                    thisDataMap.put(appZgidCtKey, admsJsonStr);
                    logger.debug("LidAndUserFirstEndService app端ss事件 放入thisLIdLnameMap:{} value:{},{}", appZgidKey ,lid,lname);
                    thisLIdLnameMap.put(appZgidCtKey, lid + "," + lname);
                }
            }
        }
    }




    private void setAppIpUaKeys(
            List<ZGMessage> msgs,
            Set<String> thisAppMaxCtKeySet) {
        if (msgs == null || msgs.isEmpty()) {
            return;
        }

        msgs.stream()
                .filter(msg -> msg != null && msg.getResult() != -1)
                .forEach(msg -> {
                    Map<String, Object> data = msg.getData();
                    if (data == null || data.isEmpty()) {
                        return;
                    }

                    String ak = String.valueOf(data.get("ak"));
                    Integer appId = msg.getAppId();

                    if (advertisingMap.containsKey(ak)) {
                        List<Map<String, Object>> dataList = (List<Map<String, Object>>) data.get("data");
                        if (dataList == null || dataList.isEmpty()) {
                            return;
                        }

                        String ip = String.valueOf(data.get("ip"));
                        String ua = String.valueOf(data.get("ua"));

                        dataList.stream()
                                .filter(Objects::nonNull)
                                .forEach(dataItem -> {
                                    String dt = String.valueOf(dataItem.get("dt"));

                                    if ("ss".equals(dt) && StringUtils.isNotBlank(ip)) {
                                        Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");
                                        if (props == null) {
                                            return;
                                        }

                                        String os = String.valueOf(props.getOrDefault("$os", ""));

                                        // 模糊匹配：iphone做特殊解析
                                        String uaProcess = ToolUtil.uaAnalysis(ua);
                                        thisAppMaxCtKeySet.add(APP_PRE+":" + appId + ":" + ip + uaProcess);

                                        // iOS 的广告 需同时匹配 ip+version
                                        if ("iOS".equals(os)) {
                                            String[] arr = uaProcess.split(":");
                                            if (arr.length > 0) {
                                                thisAppMaxCtKeySet.add(APP_PRE+":" + appId + ":" + ip + ":" + arr[1]);
                                            }
                                        }

                                        // 精确匹配设备ID
                                        String idfaOrigin = String.valueOf(props.getOrDefault("$idfa", ""));
                                        String imeiOrigin = String.valueOf(props.getOrDefault("$imei", ""));
                                        String androidIdOrigin = String.valueOf(props.getOrDefault("$android_id", ""));
                                        String oaidOrigin = String.valueOf(props.getOrDefault("$oaid", ""));
                                        String qaidCaaOrigin = String.valueOf(props.getOrDefault("$qaid_caa", ""));

                                        String idfa = processDeviceId(idfaOrigin);
                                        String imei = processDeviceId(imeiOrigin);
                                        String androidId = processDeviceId(androidIdOrigin);
                                        String oaid = processDeviceId(oaidOrigin);
                                        String qaidCaa = processDeviceId(qaidCaaOrigin);

                                        if (StringUtils.isNotEmpty(idfa)) {
                                            String muidProcessKey = APP_PRE + ":" + appId + ":" + idfa;
                                            thisAppMaxCtKeySet.add(muidProcessKey);
                                        }
                                        if (StringUtils.isNotEmpty(imei)) {
                                            String muidProcessKey = APP_PRE + ":" + appId + ":" + imei;
                                            thisAppMaxCtKeySet.add(muidProcessKey);
                                        }
                                        if (StringUtils.isNotEmpty(androidId)) {
                                            String muidProcessKey = APP_PRE + ":" + appId + ":" + androidId;
                                            thisAppMaxCtKeySet.add(muidProcessKey);
                                        }
                                        if (StringUtils.isNotEmpty(oaid)) {
                                            String muidProcessKey = APP_PRE + ":" + appId + ":" + oaid;
                                            thisAppMaxCtKeySet.add(muidProcessKey);
                                        }
                                        if (StringUtils.isNotEmpty(qaidCaa)) {
                                            String muidProcessKey = APP_PRE + ":" + appId + ":" + qaidCaa;
                                            thisAppMaxCtKeySet.add(muidProcessKey);
                                        }
                                    }
                                });
                    }


                });

    }

    private String processDeviceId(String originId) {
        if (StringUtils.isEmpty(originId)) {
            return "";
        }

        // 检查是否在排除集中
        if (originExcludeSet.contains(originId) || md5ExcludeSet.contains(originId)) {
            return "";
        }

        // 如果是32位MD5，直接使用；否则计算MD5
        if (originId.length() == 32) {
            return originId;
        } else {
            return ToolUtil.getMD5Str(originId);
        }
    }


    private void startBatchInit(){
        long start = System.currentTimeMillis();
        retryInitTable2Advertising_app();
        logger.debug("startBatchInit init finish cost : {} ms .",System.currentTimeMillis()-start);
    }
    private void retryInitTable2Advertising_app()  {
        int count = 0;
        while (count < 3) {
            try {
                boolean flag = initTable2Advertising_app();
                if (flag)
                    return;
            } catch (SQLException e) {
                count++;
            }
        }
        throw new BusinessTableReadException("read table advertising_app error .");
    }
    private boolean initTable2Advertising_app() throws SQLException {
        Map<String, Integer> advertisingMap = new HashMap<>();
        String sql = "select app_key,app_id from advertising_app where is_delete = 0 and stop = 0";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String appKey = resultSet.getString("app_key");
            Integer appId = resultSet.getInt("app_id");
            advertisingMap.put(appKey, appId);
        }
        if(!advertisingMap.isEmpty()){
            this.advertisingMap = advertisingMap;
        }else {
            return false;
        }

        try {
            resultSet.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            preparedStatement.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        return true;
    }

}
