package com.zhugeio.etl.pipeline.operator.adv;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.util.CheckJSONSchemaUtil;
import com.zhugeio.etl.common.util.DimUtils;
import com.zhugeio.etl.common.util.ToolUtil;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.exceptions.BusinessTableReadException;
import com.zhugeio.etl.pipeline.model.ToufangConvertEventRow;
import com.zhugeio.etl.pipeline.service.MsgResolver;
import com.zhugeio.tool.commons.JsonUtil;
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
public class AdvFlatMapFunction extends RichFlatMapFunction<ZGMessage, Tuple2<String,Object>> {
    private static final Logger logger = LoggerFactory.getLogger(AdvFlatMapFunction.class);
    private static final long serialVersionUID = 1L;
    private Properties jobProperties;
    private HikariDataSource dataSource;
    private Connection connection;
    private final long intervalMS = 1000L * 60;
    private Map<String, Integer> ak2AppIdMap ;
    private Map<Integer, Integer> appId2companyIdMap;
    private Set<String> businessSet ;
    private Map<String, Map<String, Set<String>>> blackMap;

    public AdvFlatMapFunction() {
    }

    public AdvFlatMapFunction(Properties jobProperties) {
        this.jobProperties = jobProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void flatMap(ZGMessage zgMessage, Collector<Tuple2<String,Object>> collector) throws Exception {
        String rawData = zgMessage.getRawData();
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(rawData);
        }catch (Exception e){
            logger.error("json parse error ",e);
        }
        if (jsonObject != null && !jsonObject.isEmpty()){
            String tableName = jsonObject.getString("tableName");
            if ("toufang_convert_event".equals(tableName)){
                ToufangConvertEventRow rowDataFromJson = jsonToToufangConvertEventRow(jsonObject);
                collector.collect(new Tuple2<String,Object>("",rowDataFromJson));
            }

            if("toufang_ad_click".equals(tableName)) {

            }

        }
    }



    /**
     * 将 JSONObject 转换为 ToufangConvertEventRow
     */
    public static ToufangConvertEventRow jsonToToufangConvertEventRow(JSONObject json) {
        JSONObject dataJson = json.getJSONObject("data");
        ToufangConvertEventRow row = new ToufangConvertEventRow();
        row.setZgAppid(dataJson.getInteger("zg_appid"));
        row.setZgId(dataJson.getInteger("zg_id"));
        row.setLid(dataJson.getInteger("lid"));
        row.setChannelId(dataJson.getInteger("channel_id"));
        row.setZgEid(dataJson.getInteger("zg_eid"));
        row.setEventTime(dataJson.getLong("event_time"));
        row.setChannelAdgroupId(dataJson.getString("channel_adgroup_id"));
        row.setChannelAdgroupName(dataJson.getString("channel_adgroup_name"));
        row.setClickTime(dataJson.getLong("click_time"));
        row.setEventName(dataJson.getString("event_name"));
        row.setChannelEvent(dataJson.getString("channel_event"));
        row.setMatchJson(dataJson.getString("match_json"));
        row.setFrequency(dataJson.getInteger("frequency"));
        row.setUtmCampaign(dataJson.getString("utm_campaign"));
        row.setUtmSource(dataJson.getString("utm_source"));
        row.setUtmMedium(dataJson.getString("utm_medium"));
        row.setUtmTerm(dataJson.getString("utm_term"));
        row.setUtmContent(dataJson.getString("utm_content"));
        return row;
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
