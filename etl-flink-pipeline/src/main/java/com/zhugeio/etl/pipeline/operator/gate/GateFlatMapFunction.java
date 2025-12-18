package com.zhugeio.etl.pipeline.operator.gate;

import com.alibaba.fastjson2.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.exceptions.BusinessTableReadException;
import com.zhugeio.etl.pipeline.service.MsgResolver;
import com.zhugeio.etl.common.util.CheckJSONSchemaUtil;
import com.zhugeio.etl.common.util.DimUtils;
import com.zhugeio.etl.common.util.ToolUtil;
import com.zhugeio.tool.commons.JsonUtil;
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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author ningjh
 * @name GateFlatMapFunction
 * @date 2025/12/10
 * @description
 */
public class GateFlatMapFunction extends RichFlatMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GateFlatMapFunction.class);
    private static final long serialVersionUID = 1L;
    private Properties jobProperties;
    private HikariDataSource dataSource;
    private Connection connection;
    private final long intervalMS = 1000L * 60;
    private Map<String, Integer> ak2AppIdMap ;
    private Map<Integer, Integer> appId2companyIdMap;
    private Set<String> businessSet ;
    private Map<String, Map<String, Set<String>>> blackMap;

    public GateFlatMapFunction() {
    }

    public GateFlatMapFunction(Properties jobProperties) {
        this.jobProperties = jobProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
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
        int day = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        JSONObject allCountJson = new JSONObject();
        JSONObject errorCountJson = new JSONObject();
        List<String> errorLogList = new ArrayList<>();
        String resolveMsg = null;
        try {
            resolveMsg = MsgResolver.resolve(zgMessage.getRawData());
            if(resolveMsg == null){
                zgMessage.setResult(-1);
            }else {
                zgMessage.setRawData(resolveMsg);
            }
//                System.out.println("resolveMsg -> "+resolveMsg);
        } catch (Exception e){
            zgMessage.setResult(-1);
            zgMessage.setError("msg format total wrong");
        }

        if (zgMessage.getResult() != -1 ) {
            Map<String, Object> map = JsonUtil.mapFromJson(resolveMsg);
            if (map != null){
                zgMessage.setData(map);
                String appKey = getAppKey(map);
                if (!StringUtils.isEmpty(appKey)) {
                    zgMessage.setAppKey(appKey);
                    Integer appId = ak2AppIdMap.get(appKey);
                    if (null != appId) {
                        zgMessage.setAppId(appId);
                        if (CheckJSONSchemaUtil.checkBasic(resolveMsg)) {
                            String business = "";
                            if (map.containsKey("business")) {
                                business = String.valueOf(map.get("business"));
                            }
                            if (null != business && !business.isEmpty()) {
                                if (appId2companyIdMap.containsKey(appId)) {
                                    Integer cid = appId2companyIdMap.get(appId);
                                    if (!businessSet.contains(cid + "_" + business)) {
                                        business = "";
                                    }
                                }
                            }
                            zgMessage.setBusiness(business);
                            map.put("business", business);
                            zgMessage.setRawData(JsonUtil.toJson(map));

                            String pl = map.get("pl").toString();
                            int plat = DimUtils.sdk(pl);
                            String sdk = map.get("sdk").toString();
                            if (!blackMap.containsKey(appKey)) {
                                allCount(zgMessage,pl,plat,day,allCountJson);
                                zgMessage.setDataQuality(allCountJson);
                                collector.collect(zgMessage);
                            } else {
                                String ip = "";
                                if (map.containsKey("ip")) {
                                    ip = String.valueOf(map.get("ip"));
                                }
                                String ua = "";
                                if (map.containsKey("ua")) {
                                    ua = String.valueOf(map.get("ua"));
                                }
                                Set<String> ipSet = blackMap.get(appKey).get("ip");
                                if (!ipSet.contains(ip)) { // ip是否为黑名单
                                    boolean isPass = true;
                                    Iterator<String> uaItr = blackMap.get(appKey).get("ua").iterator();
                                    while (uaItr.hasNext()) {
                                        String uaTmp = uaItr.next();
                                        if (ua.contains(uaTmp)) {
                                            isPass = false;
                                            break;
                                        }
                                    }
                                    if (!isPass) {
                                        allCount(zgMessage,pl,plat,day,allCountJson);
                                        zgMessage.setDataQuality(allCountJson);
                                        collector.collect(zgMessage);

                                    } else {
                                        zgMessage.setErrorCode(ErrorMessageEnum.UA_BLOCK.getErrorCode());
                                        zgMessage.setErrorDescribe(ErrorMessageEnum.UA_BLOCK.getErrorMessage());
                                        errorCount(zgMessage,pl,plat,sdk,day,errorCountJson,errorLogList);
                                        zgMessage.setDataQuality(errorCountJson);
                                        zgMessage.setDataQualityError(errorCountJson);
                                        collector.collect(zgMessage);

                                    }
                                } else {
                                    zgMessage.setErrorCode(ErrorMessageEnum.IP_BLOCK.getErrorCode());
                                    zgMessage.setErrorDescribe(ErrorMessageEnum.IP_BLOCK.getErrorMessage());
                                    errorCount(zgMessage,pl,plat,sdk,day,errorCountJson,errorLogList);
                                    zgMessage.setDataQuality(errorCountJson);
                                    zgMessage.setDataQualityError(errorCountJson);
                                    collector.collect(zgMessage);
                                }
                            }
                        }else {
                            zgMessage.setResult(-1);
                            zgMessage.setError("msg checkBasic error");
//                                System.out.println("msg checkBasic error -> "+resolveMsg);
                        }
                    } else {
                        zgMessage.setResult(-1);
                        zgMessage.setError("msg not have appId");
//                            System.out.println("msg not have appId -> "+resolveMsg);
                    }
                } else {
                    zgMessage.setResult(-1);
                    zgMessage.setError("msg not have appKey");
//                        System.out.println("msg not have appKey -> "+resolveMsg);
                }
            } else{
                zgMessage.setResult(-1);
                zgMessage.setError("msg format total wrong");
//                    System.out.println("msg format total wrong -> "+resolveMsg);
            }
        }
    }

    private void errorCount(ZGMessage zgMessage,String pl,int plat,String sdk,int day,JSONObject errorCountJson,List<String> errorLogList){
        String key = "error_count" + "#" + zgMessage.getAppId() + ":" + day + ":" + plat + ":";
        List<?> listData = (List<?>)zgMessage.getData().get("data");
        Iterator<?> iterator = listData.iterator();
        while (iterator.hasNext()){
            Map<?, ?> map = (Map<?, ?>)iterator.next();
            String dt = String.valueOf(map.get("dt"));
            //处理带有上报的事件
            if ("evt".equals(dt) || "vtl".equals(dt) || "abp".equals(dt)) {
                Map<?,?> pr = (Map<?,?>)map.get("pr");
                //事件名称
                String eventName = String.valueOf(pr.get("$eid"));

                //事件时间
                Object ctObj = pr.get("$ct");
                long ct = Long.parseLong(String.valueOf(ctObj == null ? 0 :ctObj));

                if (errorCountJson.containsKey(key + eventName)) {
                    errorCountJson.put(key + eventName, errorCountJson.getIntValue(key + eventName) + 1);
                } else {
                    errorCountJson.put(key + eventName, 1);
                }
                HashMap<String, Object> dataMap = new HashMap<>();
                //错误日志详情数据组装
                dataMap.put("app_id", zgMessage.getAppId());
                dataMap.put("error_code", zgMessage.getErrorCode());
                dataMap.put("data_json", zgMessage.getRawData());
                dataMap.put("data_md5", ToolUtil.getMD5Str(zgMessage.getRawData()));
                dataMap.put("error_md5", ToolUtil.getMD5Str(zgMessage.getErrorDescribe()));
                dataMap.put("log_utc_date", System.currentTimeMillis());
                dataMap.put("log_utc_day_id", day);
                dataMap.put("event_begin_date", ct);
                dataMap.put("pl", pl);
                dataMap.put("sdk", sdk);
                dataMap.put("platform", plat);
                dataMap.put("pro_flag", 0); // 1表示事件属性相关错误日志标识
                dataMap.put("event_name", eventName);
                dataMap.put("error_msg", zgMessage.getErrorDescribe());
                // 错误明细收集
                JSONObject errorLogMap = new JSONObject();
                errorLogMap.put("data", dataMap);
                errorLogMap.put("type", "error-event-log");
                errorLogList.add(errorLogMap.toString());
            }
        }
    }

    private void allCount(ZGMessage zgMessage,String pl,int plat,int day,JSONObject allCountJson){
        logger.debug("success allCount zgMessage is {} ",zgMessage);
        String key = "all_count" + "#" + zgMessage.getAppId() + ":" + day + ":" + plat + ":";
        List<?> listData = (List<?>)zgMessage.getData().get("data");
        Iterator<?> iterator = listData.iterator();
        while (iterator.hasNext()){
            Map<?, ?> map = (Map<?, ?>)iterator.next();
            String dt = String.valueOf(map.get("dt"));
            //处理带有上报的事件
            if ("evt".equals(dt) || "vtl".equals(dt) || "abp".equals(dt)) {
                Map<?,?> pr = (Map<?,?>)map.get("pr");
                //事件名称
                String eventName = String.valueOf(pr.get("$eid"));
                if (allCountJson.containsKey(key + eventName)) {
                    allCountJson.put(key + eventName, allCountJson.getIntValue(key + eventName) + 1);
                } else {
                    allCountJson.put(key + eventName, 1);
                }
            }
        }
    }


    private String getAppKey(Map<String, Object> map) {
        if (map.containsKey("ak")) {
            return map.get("ak").toString();
        } else {
            return null;
        }
    }

    private void startBatchInit(){
        long start = System.currentTimeMillis();
        retryInitTable2Company_app();
        retryInitTable2Business();
        retryInitTable2DataAccessFilter();
        logger.debug("startBatchInit init finish cost : {} ms .",System.currentTimeMillis()-start);
    }

    private void retryInitTable2Company_app()  {
        int count = 0;
        while (count < 3) {
            try {
                boolean flag = initTable2Company_app();
                if (flag)
                    return;
            } catch (SQLException e) {
                count++;
            }
        }
        throw new BusinessTableReadException("read table company_app error .");
    }
    private boolean initTable2Company_app() throws SQLException {
        Map<String, Integer> ak2AppIdMap = new HashMap<>();
        Map<Integer, Integer> appId2companyIdMap = new HashMap<>();
        String sql = "select id,app_key,company_id from company_app where is_delete = 0 and stop = 0 and id not in (select id from tmp_transfer where status = 2)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            Integer appId = resultSet.getInt("id");
            String appKey = resultSet.getString("app_key");
            Integer companyId = resultSet.getInt("company_id");
            ak2AppIdMap.put(appKey, appId);
            appId2companyIdMap.put(appId, companyId);
        }
        if(!ak2AppIdMap.isEmpty()){
            this.ak2AppIdMap = ak2AppIdMap;
        }else {
            return false;
        }
        if(!appId2companyIdMap.isEmpty()){
            this.appId2companyIdMap = appId2companyIdMap;
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


    private void retryInitTable2Business()  {
        int count = 0;
        while (count < 3) {
            try {
                boolean flag = initTable2Business();
                if (flag)
                    return;
            } catch (SQLException e) {
                count++;
            }
        }
        throw new BusinessTableReadException("read table business error .");
    }
    private boolean initTable2Business() throws SQLException {
        Set<String> businessSet = new HashSet<>();
        String sql = "select company_id,identifier from business  where del = 0 and state = 1";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            Integer companyId = resultSet.getInt("company_id");
            String identifier = resultSet.getString("identifier");
            businessSet.add(companyId + "_" + identifier);
        }
        if(!businessSet.isEmpty()){
            this.businessSet = businessSet;
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

    private void retryInitTable2DataAccessFilter()  {
        int count = 0;
        while (count < 3) {
            try {
                boolean flag = initTable2DataAccessFilter();
                if (flag)
                    return;
            } catch (SQLException e) {
                count++;
            }
        }
        throw new BusinessTableReadException("read table business error .");
    }

    private boolean initTable2DataAccessFilter() throws SQLException {
        Map<String, Map<String, Set<String>>> blackMap = new HashMap<>();
        String sql = "select a.app_key ak,b.ip ip,b.ua ua from company_app a join data_access_filter b on a.id = b.app_id";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            HashMap<String, Set<String>> stringSetHashMap = new HashMap<>();
            String appKey = resultSet.getString("ak");
            String ipSting = resultSet.getString("ip");
            if(!StringUtils.isEmpty(ipSting) && ipSting.length()>4){
                Set<String> ipSet = Arrays.stream(ipSting.substring(1, ipSting.length() - 1).replace("\"", "").split(","))
                        .collect(Collectors.toSet());
                stringSetHashMap.put("ip", ipSet);
            }else {
                stringSetHashMap.put("ip", new HashSet<String>());
            }

            String uaSting = resultSet.getString("ua");
            if(!StringUtils.isEmpty(uaSting) && uaSting.length()>4){
                Set<String> uaSet = Arrays.stream(uaSting.substring(1, uaSting.length() - 1).replace("\"", "").split(","))
                        .collect(Collectors.toSet());
                stringSetHashMap.put("ua", uaSet);
            }else {
                stringSetHashMap.put("ua", new HashSet<String>());
            }
            blackMap.put(appKey, stringSetHashMap);
        }

        this.blackMap = blackMap;
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

    private Connection getConnection() {
        return connection;
    }
}
