package com.zhugeio.etl.pipeline.operator.gate;

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
public class AdvSaveAppAdDataFlatMapFunction extends RichFlatMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(AdvSaveAppAdDataFlatMapFunction.class);
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

    public AdvSaveAppAdDataFlatMapFunction() {
    }

    public AdvSaveAppAdDataFlatMapFunction(Properties jobProperties) {
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

        Set<String> thisAppMaxCtKeySet = new HashSet<>();
        Set<String> lastAppDataKeySet = new HashSet<>();
        Map<String, String> lastAppSsdbDataMap = new HashMap<>();
        Map<String, String> lastAppSsdbMaxCtMap = new HashMap<>();
        Map<String, Long> thisAppMaxCtMap = new HashMap<>();
        Map<String, String> thisAppDataMap = new HashMap<>();
        Set<String> thisAppDelKeySet =  new HashSet<>();

        setIpUaMaps(msgs, thisAppMaxCtKeySet, thisAppMaxCtMap, thisAppDataMap);
        logger.debug("AdService app 本批次thisAppMaxCtMap : {} ",thisAppMaxCtMap);

        //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}"，ct）
        if (!thisAppMaxCtKeySet.isEmpty()) {
            List<KeyValue<String, String>> keyValues = redisClient.syncMGet(thisAppMaxCtKeySet.toArray(new String[0]));
            for (KeyValue<String, String> keyValue : keyValues) {
                String key = keyValue.getKey();
                String value = keyValue.getValue();
                if (value != null) {
                    lastAppSsdbMaxCtMap.put(key, value);
                }
            }
            logger.debug("AdService 获取之前已存入的MaxCt lastAppSsdbMaxCtMap : {} ",lastAppSsdbMaxCtMap);

            lastAppSsdbMaxCtMap.forEach((key, value) -> {
                if(value!=null){
                    lastAppDataKeySet.add( key+":"+value);
                }
            });

            if (!lastAppDataKeySet.isEmpty()) {
                //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
                keyValues = redisClient.syncMGet(lastAppDataKeySet.toArray(new String[0]));
                for (KeyValue<String, String> keyValue : keyValues) {
                    String key = keyValue.getKey();
                    String value = keyValue.getValue();
                    if (value != null) {
                        lastAppSsdbDataMap.put(key, value);
                    }
                }
            }

            if (!thisAppMaxCtMap.isEmpty()) {
                thisAppMaxCtMap.forEach((ctKey, thisMaxCt) -> {
                    if (!lastAppSsdbMaxCtMap.containsKey(ctKey) || thisMaxCt >= Long.parseLong(lastAppSsdbMaxCtMap.get(ctKey))) {
                        String dataKey = ctKey+":"+thisMaxCt;
                        //更新ssdb 最大ct
                        logger.debug("AdService app 向 ssdb 存入广告信息，ctKey：{}， thisMaxCt：{}",ctKey,thisMaxCt);
                        redisClient.asyncSet(ctKey, thisMaxCt + "");

                        redisClient.expire(ctKey, expireTime); //过期时间设置 可配置
                        logger.debug("AdService app 向 ssdb 存入广告信息，dataKey：{}， thisAppDataMap：{}",dataKey,thisAppDataMap);
                        //更新ssdb 最大ct对应的广告信息
                        redisClient.asyncSet(dataKey, thisAppDataMap.get(dataKey));
                        redisClient.expire(dataKey, expireTime);

                        if (lastAppSsdbMaxCtMap.containsKey(ctKey)) {
                            long lastMaxCt = Long.parseLong(lastAppSsdbMaxCtMap.get(ctKey));
                            if (thisMaxCt > lastMaxCt) {
                                thisAppDelKeySet.add(ctKey+":"+lastMaxCt);
                            }
                        }

                    }
                });
            }
        }
        //删除过期无效数据
        if (thisAppDelKeySet.size() > 0) {
            redisClient.asyncDel(thisAppDelKeySet.toArray(new String[0]));
        }
        for (ZGMessage msg : msgs) {
            collector.collect(msg);
        }
    }

    /**
     * 为map赋值 ip+ua
     */
    private void setIpUaMaps(
            List<ZGMessage> msgs,
            Set<String> thisAppMaxCtKeySet,
            Map<String, Long> thisAppMaxCtMap,
            Map<String, String> thisAppDataMap) {

        Iterator<ZGMessage> iteratorList = msgs.iterator();
        while (iteratorList.hasNext()){
            ZGMessage zgMessage = iteratorList.next();
            if(zgMessage.getResult() != -1){
                //这里用的 rawData ，避免上游未处理，msg.data无数据  ?
                Map<String, Object> data = JsonUtil.mapFromJson(zgMessage.getRawData());
                String sdk = String.valueOf(data.get("sdk"));
                String ak = String.valueOf(data.get("ak"));
                if (!StringUtils.isEmpty(sdk) && sdk.equals("zg_adtoufang")) {
                    if (advertisingMap.containsKey(ak)) {
                        List<?> listData = (List<?>)data.get("data");
                        Iterator<?> iterator = listData.iterator();
                        while (iterator.hasNext()){
                            Map<?, ?> map = (Map<?, ?>)iterator.next();
                            String dt = String.valueOf(map.get("dt"));
                            //处理带有上报的事件
                            if ("adtf".equals(dt)) {
                                Map<?,?> pr = (Map<?,?>)map.get("pr");
                                Object ipObj = pr.get("$ip");
                                String ip = ipObj == null ? "" : String.valueOf(ipObj);
                                Object uaObj = pr.get("$ua");
                                String ua = uaObj == null ? "" : String.valueOf(uaObj);
                                //app端广告信息中 $ados ： android 0 、 ios 1 、其他 3
                                Object osObj = pr.get("$ados");
                                String os = osObj == null ? "" : String.valueOf(osObj);
                                //区分 广告信息来自哪个投放平台：$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4
                                Object channelTypeObj = pr.get("$channel_type");
                                String channelType = channelTypeObj == null ? "" : String.valueOf(channelTypeObj);

                                Object lidObj = pr.get("$lid");
                                Long lid = lidObj == null ? 0 : Long.parseLong(String.valueOf(lidObj));
                                //注意： app端广告信息的lua脚本中的 应用id的key名称叫 $zg_appid, app端没有ct，有 $click_time
                                Long appId = Long.parseLong(String.valueOf(pr.get("$zg_appid")));
                                //app端广告数据：
                                if (lid != 0L) {
                                    AdvMessage advMessage = new AdvMessage();
                                    advMessage.setZg_appid(appId);
                                    advMessage.setLid(lid);
                                    Object clickTimeObj = pr.get("$click_time");
                                    String clickTimeStr = clickTimeObj == null ? "" : String.valueOf(clickTimeObj);
                                    Long clickTime = Long.parseLong(clickTimeStr);
                                    if(clickTimeStr.length()==10){
                                        clickTime *= 1000L;
                                    }
                                    advMessage.setClick_time(clickTime);
                                    advMessage.setCt(clickTime);
                                    advMessage.setApp_key(ak);
                                    advMessage.setFields(map);

                                    String uaProcess = ToolUtil.uaAnalysis(ua);
                                    if ("1".equals(channelType) || "4".equals(channelType)) {
                                        //$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4
                                        //百度信息流、百度搜索 的ios 按 ip+版本号匹配 （因ipad 也解析成了iphone）
                                        if (ua.contains("iPhone")) {
                                            String[] arr = uaProcess.split(":");
                                            if (arr.length > 0) {
                                                String name = arr[0];
                                                String version = arr[1];
                                                uaProcess = ":" + version;
                                            }
                                        }
                                    }

                                    String ipUaKey = "adtfad:"+appId+":"+ip+uaProcess;
                                    if (StringUtils.isNotEmpty(ip)) {
                                        //模糊匹配：ip+ua 或 ip+os 或 ip
                                        advMessage.setIp_ua_key(ipUaKey);
                                        thisAppMaxCtKeySet.add(ipUaKey);
                                    }

                                    // 精确匹配
                                    setMuidProcess(channelType, appId + "", advMessage, thisAppMaxCtKeySet, thisAppMaxCtMap, thisAppDataMap);

                                    if (StringUtils.isNotEmpty(ip)) {
                                        //TODO 留下模糊匹配：最近一次点击的广告 (adms 需含有精确匹配 对应的key值)
                                        boolean isContainIpUaKey = thisAppMaxCtMap.containsKey(ipUaKey);
                                        Long maxCt = 0L;
                                        if (isContainIpUaKey) {
                                            maxCt = thisAppMaxCtMap.get(ipUaKey);
                                        }
                                        if (!isContainIpUaKey || advMessage.getCt() >= maxCt) {
                                            thisAppMaxCtMap.put(ipUaKey, advMessage.getCt());
                                            thisAppDataMap.put(ipUaKey+":"+advMessage.getCt(), advMessage.toJsonString());
                                        }
                                    }

                                    // 广告数据发kafka
                                    JSONObject jsonObj = new JSONObject();
                                    jsonObj.put("tableName", "toufang_ad_click");
                                    jsonObj.put("sinkType", "kudu");
                                    jsonObj.put("data", advMessage.toJsonString());
                                    String advKafkaMsg = jsonObj.toJSONString().replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}");
                                    List<String> advKafkaMsgList = zgMessage.getAdvKafkaMsgList();
                                    if(advKafkaMsgList==null)
                                        zgMessage.setAdvKafkaMsgList(advKafkaMsgList = new ArrayList<>());
                                    advKafkaMsgList.add(advKafkaMsg);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void putAd(
            String muidProcessKey,
            AdvMessage advMessage,
            Map<String, Long> thisAppMaxCtMap,
            Map<String, String> thisAppDataMap) {
        // 留下精确匹配：最近一次点击的广告
        boolean isContainMuidKey = thisAppMaxCtMap.containsKey(muidProcessKey);
        long muidMaxCt = 0L;
        if (isContainMuidKey) {
            muidMaxCt = thisAppMaxCtMap.get(muidProcessKey);
        }
        if (!isContainMuidKey || advMessage.getCt() >= muidMaxCt) {
            thisAppMaxCtMap.put(muidProcessKey, advMessage.getCt());
            thisAppDataMap.put(muidProcessKey+":"+advMessage.getCt(), advMessage.toJsonString());
        }
    }

    private void setMuidProcess(
            String channelType,
            String appId,
            AdvMessage advMessage,
            Set<String> thisAppMaxCtKeySet,
            Map<String, Long> thisAppMaxCtMap,
            Map<String, String> thisAppDataMap) {

        //注意事项：因广告点击监测时第三方平台传递数据可能传递“0”、NULL、“”，过滤掉“”、0、NULL和他们的MD5小写值
        String muid = null;
        if (originExcludeSet.contains(advMessage.getMuid()) || md5ExcludeSet.contains(advMessage.getMuid())) {
            muid = "";
        }else {
            if (advMessage.getMuid().length() == 32)
                muid = advMessage.getMuid();
            else
                muid = ToolUtil.getMD5Str(advMessage.getMuid());
        }

        String androidId = null;
        if(originExcludeSet.contains(advMessage.getAndroid_id()) || md5ExcludeSet.contains(advMessage.getAndroid_id())) {
            androidId = "";
        }else{
            if (advMessage.getAndroid_id().length() == 32)
                androidId = advMessage.getAndroid_id();
            else
                androidId = ToolUtil.getMD5Str(advMessage.getAndroid_id());
        }

        String oaid = null;
        if(originExcludeSet.contains(advMessage.getOaid()) || md5ExcludeSet.contains(advMessage.getOaid())) {
            oaid = "";
        }else{
            if (advMessage.getOaid().length() == 32)
                oaid = advMessage.getOaid();
            else
                oaid = ToolUtil.getMD5Str(advMessage.getOaid());
        }

        String idfa = null;
        if(originExcludeSet.contains(advMessage.getIdfa()) || md5ExcludeSet.contains(advMessage.getIdfa())) {
            idfa = "";
        }else{
            if (advMessage.getIdfa().length() == 32)
                idfa = advMessage.getIdfa();
            else
                idfa = ToolUtil.getMD5Str(advMessage.getIdfa());
        }

        String imei = null;
        if(originExcludeSet.contains(advMessage.getImei()) || md5ExcludeSet.contains(advMessage.getImei())) {
            imei = "";
        }else{
            if (advMessage.getImei().length() == 32)
                imei = advMessage.getImei();
            else
                imei = ToolUtil.getMD5Str(advMessage.getImei());
        }

        //app端广告信息中 $ados ： android 0 、 ios 1 、其他 3
        //区分 广告信息来自哪个投放平台：$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4 、知乎 9、快手 10、微博 11、华为 12
        // muid_key idfa_key  imei_key  android_id_key oaid_key
        //对md5值转小写
        muid = muid.toLowerCase();
        androidId = androidId.toLowerCase();
        oaid = oaid.toLowerCase();
        idfa = idfa.toLowerCase();
        imei = imei.toLowerCase();
        if ("3".equals(channelType) && "20".equals(advMessage.getPush_type()) && StringUtils.isNotEmpty(advMessage.getChannel_click_id())) {
            //push_type：10 app、20 web广告数据  腾讯广告 -web:channel_click_id
            if (StringUtils.isNotEmpty(advMessage.getChannel_click_id())) {
                String muidProcessKey = "adtfad:" + appId + ":" + advMessage.getChannel_click_id();
                advMessage.setChannel_click_id_key(muidProcessKey);
                thisAppMaxCtKeySet.add(muidProcessKey);
                putAd(muidProcessKey, advMessage, thisAppMaxCtMap, thisAppDataMap);
            }
        }else {
            if (StringUtils.isNotEmpty(muid)) {
                String muidProcessKey = "adtfad:"+appId+":"+muid;
                advMessage.setMuid_key(muidProcessKey);
                thisAppMaxCtKeySet.add(muidProcessKey);
                putAd(muidProcessKey, advMessage, thisAppMaxCtMap, thisAppDataMap);
            }

            if (StringUtils.isNotEmpty(idfa)) {
                //ios
                String muidProcessKey = "adtfad:"+appId+":"+idfa;
                advMessage.setIdfa_key(muidProcessKey);
                thisAppMaxCtKeySet.add(muidProcessKey);
                putAd(muidProcessKey, advMessage, thisAppMaxCtMap, thisAppDataMap);
            }
            if (StringUtils.isNotEmpty(imei)) {
                String muidProcessKey = "adtfad:"+appId+":"+imei;
                advMessage.setImei_key(muidProcessKey);
                thisAppMaxCtKeySet.add(muidProcessKey);
                putAd(muidProcessKey, advMessage, thisAppMaxCtMap, thisAppDataMap);
            }
            if (StringUtils.isNotEmpty(androidId)) {
                String muidProcessKey = "adtfad:"+appId+":"+androidId;
                advMessage.setAndroid_id_key(muidProcessKey);
                thisAppMaxCtKeySet.add(muidProcessKey);
                putAd(muidProcessKey, advMessage, thisAppMaxCtMap, thisAppDataMap);
            }
            if (StringUtils.isNotEmpty(oaid)) {
                String muidProcessKey = "adtfad:"+appId+":"+oaid;
                advMessage.setOaid_key(muidProcessKey);
                thisAppMaxCtKeySet.add(muidProcessKey);
                putAd(muidProcessKey, advMessage, thisAppMaxCtMap, thisAppDataMap);
            }
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
