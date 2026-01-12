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
public class AdvConvertEventFlatMapFunction extends RichFlatMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(AdvConvertEventFlatMapFunction.class);
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
    private Map<String,Integer> advertisingMap ;
    private Map<String,AdsLinkEvent> adsLinkEventMap ;
    private Set<String> adFrequencySet ;

    private final String APP_PRE = "adtfad";
    private final String SSDB_CT_PRE = "adtfuser";
    private final String SSDB_AD_DATA_PRE = "adtfdata";

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

    public AdvConvertEventFlatMapFunction() {
    }

    public AdvConvertEventFlatMapFunction(Properties jobProperties) {
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


        convertMessage(msgs);

        for (ZGMessage msg : msgs) {
            collector.collect(msg);
        }
    }

    /**
     * 转换消息，处理广告深度回传事件
     */
    private void convertMessage(List<ZGMessage> msgs) throws ParseException, SQLException {
        // 常量定义
        final String EVT = "evt"; // 回传事件仅针对于全埋点
        final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String utmPre = "utm";
        final String adConvertPrefix = "ad:convert:status:";

        // 过滤出result不为-1的消息
        List<ZGMessage> validMsgs = new ArrayList<>();
        for (ZGMessage msg : msgs) {
            if (msg.getResult() != -1) {
                validMsgs.add(msg);
            }
        }

        for (ZGMessage msg : validMsgs) {
            Map<String, Object> data = msg.getData();
            String ak = (String) data.get("ak");
            String pl = (String) data.get("pl");
            Integer appId = msg.getAppId();

            // 处理来自web端的（"pl"="js"）广告投放应用
            if (advertisingMap.containsKey(ak)) {
                List<Map<String, Object>> dataList = (List<Map<String, Object>>) data.get("data");

                for (Map<String, Object> dataItem : dataList) {
                    String dt = (String) dataItem.get("dt");

                    // 处理事件属性 lid bd_vid
                    if ("evt".equals(dt)) {
                        Map<String, Object> props = (Map<String, Object>) dataItem.get("pr");

                        // 获取事件相关信息
                        String eventName = String.valueOf(props.get("$eid")); // 事件名称
                        int zgEid = Integer.parseInt(String.valueOf(props.get("$zg_eid"))); // 事件id
                        int zgDid = Integer.parseInt(String.valueOf(props.get("$zg_did"))); // 设备id
                        int zgId = Integer.parseInt(String.valueOf(props.get("$zg_zgid"))); // 诸葛id

                        // 事件时间
                        long ct = 0L;
                        Object ctObj = props.get("$ct");
                        if (ctObj != null) {
                            ct = Long.parseLong(String.valueOf(ctObj));
                        }

                        // 获取用户最后一条广告信息
                        String ctKey = SSDB_CT_PRE + ":" + appId + ":" + zgId;
                        String maxCt = redisClient.syncGet(ctKey);

                        if (StringUtils.isNotEmpty(maxCt)) {
                            // 事件时间在窗口期内:30天
                            String adDataKey = SSDB_AD_DATA_PRE + ":" + appId + ":" + zgId + ":" + maxCt;
                            String adMessageJsonStr = redisClient.syncGet(adDataKey);

                            if (StringUtils.isNotEmpty(adMessageJsonStr)) {
                                logger.debug("ConvertEventService 深度回传事件获取到广告信息  adtf:data:" + appId + ":" + zgId + ":" + maxCt);

                                JSONObject adMessageJson = JSON.parseObject(adMessageJsonStr);
                                int lid = Integer.parseInt(String.valueOf(adMessageJson.get("lid"))); // 链接ID
                                String lidConvertKey = zgEid + "_" + lid;

                                logger.debug("ConvertEventService 获取深度回传条件lidConvertKey:" + lidConvertKey);

                                if (adsLinkEventMap.containsKey(lidConvertKey)) {
                                    AdsLinkEvent adsLinkEvent = adsLinkEventMap.get(lidConvertKey);
                                    long windowTime = adsLinkEvent.getWindowTime(); // 原为 convertExpireTime

                                    logger.debug("ConvertEventService 获取时间 ct:" + ct + " maxCt：" + maxCt + " windowTime：" + windowTime + " flag：" + ((ct - Long.parseLong(maxCt)) <= windowTime * 1000));

                                    if (ct > 0 && ((ct - Long.parseLong(maxCt)) <= (windowTime * 1000)) && ct >= Long.parseLong(maxCt)) {
                                        // 判断首次记录表 ads_frequency_first 不存在转化数据 且 链接事件满足 ads_link_event 表 中的 链接id与深度转换事件 才开始转化流程
                                        String frequencyKey = zgEid + "_" + lid + "_" + zgId;
                                        boolean existFrequency = adFrequencySet.contains(frequencyKey);

                                        logger.debug("ConvertEventService 深度回传事件 首次回传表是否存在 existFrequency:" + existFrequency + " ,本次事件zgEid_lid_zgId：" + frequencyKey);

                                        if (!existFrequency) {
                                            logger.debug("ConvertEventService 深度回传事件 zgEid：" + zgEid + " 该事件满足 事件id 转化");

                                            ConvertMessageV2 convertMsgV2 = new ConvertMessageV2(); // 回传行为数据，发至kafka
                                            ConvertMessage adConvertEvent = new ConvertMessage(); // 回传事件记录信息 写入PG表 toufang_convert_event

                                            // 判断属性条件
                                            boolean flag = false;
                                            logger.debug("ConvertEventService matchJson type:从 ads_link_event 表获取回传事件的频次：" + adsLinkEvent.toJsonString());

                                            String matchJsonStr = adsLinkEvent.getMatchJson();
                                            if (StringUtils.isNotEmpty(matchJsonStr) && !"{}".equals(matchJsonStr)) {
                                                JSONObject matchJson = JSON.parseObject(matchJsonStr); // 判断属性是否符合

                                                logger.debug("ConvertEventService matchJson type:{}",matchJson.getInteger("type"));
                                                logger.debug("ConvertEventService matchJson operator:{}" ,matchJson.getString("operator"));
                                                logger.debug("ConvertEventService matchJson values:{}" ,matchJson.getJSONArray("values"));

                                                boolean proIsRight = OperatorUtil.compareProValue(appId, zgId, props, matchJson, zgDid);
                                                if (proIsRight) {
                                                    flag = true;
                                                }
                                            } else {
                                                flag = true;
                                            }

                                            // 满足事件属性条件
                                            if (flag) {
                                                boolean sendFlag = false;
                                                String[] eventIdsArr = adsLinkEvent.getEventIds().split(",");

                                                if (eventIdsArr.length > 1) {
                                                    // 存入ssdb
                                                    String adConvertStatusKey = adConvertPrefix + ":" + zgEid + ":" + lid;
                                                    redisClient.syncSet(adConvertStatusKey, "1");
                                                    redisClient.expire(adConvertStatusKey, windowTime);
                                                    logger.debug("ConvertEventService 存adConvertStatusKey :" + adConvertStatusKey + " windowTime:" + windowTime);

                                                    // 从ssdb获取所有eid的状态
                                                    Set<String> adConvertStatusKeySet = new HashSet<>();
                                                    for (String eventId : eventIdsArr) {
                                                        adConvertStatusKeySet.add(adConvertPrefix + ":" + eventId + ":" + lid);
                                                        logger.debug("ConvertEventService 获取adConvertStatusKey :" + adConvertPrefix + ":" + eventId + ":" + lid);
                                                    }
                                                    Map<String, String> adConvertResult = new HashMap<>();
                                                    List<KeyValue<String, String>> keyValues = redisClient.syncMGet(adConvertStatusKeySet.toArray(new String[0]));
                                                    for (KeyValue<String, String> keyValue : keyValues){
                                                        String key = keyValue.getKey();
                                                        String value = keyValue.getValue();
                                                        if (value!=null){
                                                            adConvertResult.put(key, value);
                                                        }
                                                    }
                                                    logger.debug("ConvertEventService 获取adConvertResult size: {}",adConvertResult.size());
                                                    sendFlag = (adConvertResult.size() == adConvertStatusKeySet.size());
                                                } else {
                                                    sendFlag = true;
                                                }

                                                // 满足深度回传发kafka
                                                if (sendFlag) {
                                                    logger.debug("ConvertEventService 深度回传事件 zgEid: {} 该事件满足 属性id 转化",zgEid);

                                                    adConvertEvent.setMatchJson(adsLinkEvent.getMatchJson());
                                                    Object tmpCallbackUrl = adMessageJson.get("callback_url");
                                                    if (tmpCallbackUrl == null || "null".equals(tmpCallbackUrl)) {
                                                        tmpCallbackUrl = "";
                                                    }

                                                    // 写入 ads_frequency_first 表
                                                    // insert into ads_frequency_first values(5633,179,'',10000000)
                                                    if (adsLinkEvent.getFrequency() == 0) { // 判断频次是首次(0)还是每次(1)
                                                        // 写入 web mysql
                                                        String sql = "insert into ads_frequency_first values(?,?,?,?,?) ";
                                                        PreparedStatement preparedStatement = connection.prepareStatement(sql);
                                                        preparedStatement.setLong(1, zgEid);
                                                        preparedStatement.setLong(2, lid);
                                                        preparedStatement.setLong(3, zgId);
                                                        preparedStatement.setString(4, adsLinkEvent.getChannelEvent());
                                                        preparedStatement.setString(5, formatter.format(new Timestamp(ct)));
                                                        boolean execute = preparedStatement.execute();
                                                        preparedStatement.close();
                                                        logger.debug("ConvertEventService 频次为首次（0），写入ads_frequency_first表，lid：{}",execute);
                                                        adFrequencySet.add(frequencyKey);
                                                    }

                                                    // 从 ads_link_toufang 表 获取广告自定义参数
                                                    Utm utm = null;
                                                    String utmSQL = "select utm_campaign,utm_source,utm_medium,utm_term,utm_content from ads_link_toufang where id = ? limit 1";
                                                    PreparedStatement preparedStatement = connection.prepareStatement(utmSQL);
                                                    preparedStatement.setLong(1, lid);
                                                    ResultSet resultSet = preparedStatement.executeQuery();
                                                    if (resultSet.next()){
                                                        utm = new Utm();
                                                        utm.setUtmCampaign(resultSet.getString("utm_campaign"));
                                                        utm.setUtmSource(resultSet.getString("utm_source"));
                                                        utm.setUtmMedium(resultSet.getString("utm_medium"));
                                                        utm.setUtmTerm(resultSet.getString("utm_term"));
                                                        utm.setUtmContent(resultSet.getString("utm_content"));
                                                    }
                                                    logger.debug("ConvertEventService 从 ads_link_toufang 表 获取广告utm信息:{}",utm.toJsonString());
                                                    if (utm != null) {
                                                        // 将 utm_source,utm_medium,utm_campaign,utm_content,utm_term 写入ssdb ，由dw模块写入kudu
                                                        String utmKey = utmPre + ":" + zgEid;
                                                        redisClient.syncSet(utmKey, utm.toJsonString());
                                                        redisClient.expire(utmKey, 86400); // 过期时间设为24小时
                                                        adConvertEvent.compileUtm(utm);
                                                    }

                                                    adConvertEvent.setZgEid(zgEid);
                                                    adConvertEvent.setZgAppid(appId);
                                                    adConvertEvent.setZgId(zgId);
                                                    adConvertEvent.setLid(lid);

                                                    adConvertEvent.setEventTime(ct);
                                                    adConvertEvent.setChannelEvent(adsLinkEvent.getChannelEvent());
                                                    adConvertEvent.setChannelId(adMessageJson.getLongValue("channel_id"));
                                                    adConvertEvent.setChannelAdgroupId(adMessageJson.getLongValue("channel_adgroup_id"));
                                                    adConvertEvent.setChannelAdgroupName(adMessageJson.getString("channel_adgroup_name"));
                                                    adConvertEvent.setClickTime(adMessageJson.getLongValue("click_time"));
                                                    adConvertEvent.setFrequency(adsLinkEvent.getFrequency());
                                                    adConvertEvent.setEventName(eventName);

                                                    // 将需要落库的数据发kafka
                                                    JSONObject jsonObj = new JSONObject();
                                                    jsonObj.put("tableName", "toufang_convert_event");
                                                    jsonObj.put("sinkType", "kudu");
                                                    jsonObj.put("data", adConvertEvent.toConvertJson());

                                                    String key = appId + ":" + zgId;
                                                    String value = jsonObj.toJSONString()
                                                            .replace("\\\"", "\"")
                                                            .replace("\"{", "{")
                                                            .replace("}\"", "}");
                                                    List<String> advKafkaMsgList = msg.getAdvKafkaMsgList();
                                                    if(advKafkaMsgList == null)
                                                        msg.setAdvKafkaMsgList(advKafkaMsgList=new ArrayList<>());
                                                    advKafkaMsgList.add(value);
                                                    logger.debug("ConvertEventService 转化行为 到 kafka toufang_kudu : {}",value);

                                                    // 将转化行为数据存入结果集
                                                    convertMsgV2.setCallbackUrl(String.valueOf(tmpCallbackUrl));
                                                    convertMsgV2.setLid(lid);
                                                    String eventType = null;
                                                    AdsLinkEvent adsLinkEvent1 = adsLinkEventMap.get(zgEid + "_" + lid);
                                                    if(adsLinkEvent1 != null)
                                                        eventType = adsLinkEvent1.getChannelEvent();

                                                    convertMsgV2.setEventType(eventType == null ? "" : eventType);
                                                    convertMsgV2.setActionTime(String.valueOf(ct));
                                                    convertMsgV2.setFields(adMessageJson);
                                                    convertMsgV2.setZgId(zgId);
                                                    List<String> advUserKafkaMsgList = msg.getAdvUserKafkaMsgList();
                                                    if(advUserKafkaMsgList == null)
                                                        msg.setAdvKafkaMsgList(advUserKafkaMsgList=new ArrayList<>());
                                                    advUserKafkaMsgList.add(convertMsgV2.toJsonString());
                                                    logger.debug("ConvertEventService 回传数据 到 kafka toufang_ad_user：{}",convertMsgV2.toJsonString());
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
    }



    private void startBatchInit(){
        long start = System.currentTimeMillis();
        retryInitTable2Advertising_app();
        retryInitTable2ads_link_event();
        retryInitTable2ads_frequency_first();
        logger.debug("startBatchInit init finish cost : {} ms .",System.currentTimeMillis()-start);
    }

    private void retryInitTable2ads_frequency_first()  {
        int count = 0;
        while (count < 3) {
            try {
                boolean flag = initTable2ads_frequency_first();
                if (flag)
                    return;
            } catch (SQLException e) {
                count++;
            }
        }
        throw new BusinessTableReadException("read table advertising_app error .");
    }
    private boolean initTable2ads_frequency_first() throws SQLException {
        Set<String> adFrequencySet = new HashSet<>();
        String sql = "select event_id,link_id,zg_id from ads_frequency_first";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            int eventId = resultSet.getInt("event_id");
            int lid = resultSet.getInt("link_id");
            int zgId = resultSet.getInt("zg_id");
            adFrequencySet.add(lid + "_" + eventId + "_" + zgId);
        }
        if(!adFrequencySet.isEmpty()){
            this.adFrequencySet = adFrequencySet;
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



    private void retryInitTable2ads_link_event()  {
        int count = 0;
        while (count < 3) {
            try {
                boolean flag = initTable2ads_link_event();
                if (flag)
                    return;
            } catch (SQLException e) {
                count++;
            }
        }
        throw new BusinessTableReadException("read table ads_link_event error .");
    }
    private boolean initTable2ads_link_event() throws SQLException {
        Map<String,AdsLinkEvent> adsLinkEventMap = new HashMap<>();

        String sql = "select link_id,event_id,event_ids,channel_event,match_json,frequency,windows_time " +
                "from ads_link_event where is_delete=0";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            Integer linkId = resultSet.getInt("link_id");

            Integer eventId = resultSet.getInt("event_id");

            String channelEvent = resultSet.getString("channel_event");

            // 处理match_json
            String matchJson = resultSet.getString("match_json");

            // 处理frequency
            Integer frequency = resultSet.getInt("frequency");

            // 处理window_time，默认30天（2592000秒）
            Long windowTime = resultSet.getLong("windows_time");
            if(windowTime == 0L)
                windowTime = 2592000L;

            // 处理event_ids
            String eventIds = resultSet.getString("event_ids");

            // 创建AdsLinkEvent对象
            AdsLinkEvent adsLinkEvent = new AdsLinkEvent();
            adsLinkEvent.setLinkId(linkId);
            adsLinkEvent.setEventId(eventId);
            adsLinkEvent.setChannelEvent(channelEvent);
            adsLinkEvent.setMatchJson(matchJson);
            adsLinkEvent.setFrequency(frequency);
            adsLinkEvent.setWindowTime(windowTime);
            adsLinkEvent.setEventIds(eventIds);
            // 构建key
            String key = eventId + "_" + linkId;
            adsLinkEventMap.put(key, adsLinkEvent);
        }

        if(!adsLinkEventMap.isEmpty()){
            this.adsLinkEventMap = adsLinkEventMap;
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
