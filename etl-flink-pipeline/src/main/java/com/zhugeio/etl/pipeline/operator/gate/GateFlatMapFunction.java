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
        String resolveMsg = null;
        try {
            resolveMsg = MsgResolver.resolve(zgMessage.getRawData());
        } catch (Exception e){
            zgMessage.setResult(-1);
            zgMessage.setError("msg format error : resolve exception");
        }

        if(resolveMsg == null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg format error : resolveMsg is null");
            collector.collect(zgMessage);
            return;
        }else {
            zgMessage.setRawData(resolveMsg);
        }

        Map<String, Object> map = JsonUtil.mapFromJson(resolveMsg);
        if(map==null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg format error : resolveMsg mapFromJson is null");
            return;
        }
        zgMessage.setData(map);

        String appKey = getAppKey(map);
        if (appKey == null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg not have appKey");
            return;
        }
        zgMessage.setAppKey(appKey);

        Integer appId = ak2AppIdMap.get(appKey);
        if(appId == null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg not have appId");
            return;
        }

        zgMessage.setAppId(appId);
        boolean checkedBasic = CheckJSONSchemaUtil.checkBasic(resolveMsg);
        if(!checkedBasic){
            zgMessage.setResult(-1);
            zgMessage.setError("msg checkBasic error");
            return;
        }
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
        // 配置了黑名单过滤，这里进行相关校验
        if (blackMap.containsKey(appKey)) {
            // ip校验
            String ip = "";
            if (map.containsKey("ip")) {
                ip = String.valueOf(map.get("ip"));
            }
            Set<String> ipSet = blackMap.get(appKey).get("ip");
            if (ipSet.contains(ip)) {
                zgMessage.setResult(-1);
                zgMessage.setErrorCode(ErrorMessageEnum.IP_BLOCK.getErrorCode());
                zgMessage.setErrorDescribe(ErrorMessageEnum.IP_BLOCK.getErrorMessage());
                collector.collect(zgMessage);
                return;
            }

            // ua校验
            String ua = "";
            if (map.containsKey("ua")) {
                ua = String.valueOf(map.get("ua"));
            }
            Set<String> uaSet = blackMap.get(appKey).get("ua");
            if (uaSet.contains(ua)) {
                zgMessage.setResult(-1);
                zgMessage.setErrorCode(ErrorMessageEnum.UA_BLOCK.getErrorCode());
                zgMessage.setErrorDescribe(ErrorMessageEnum.UA_BLOCK.getErrorMessage());
                collector.collect(zgMessage);
                return;
            }
        }
        // 主数据输出下游
        collector.collect(zgMessage);
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
