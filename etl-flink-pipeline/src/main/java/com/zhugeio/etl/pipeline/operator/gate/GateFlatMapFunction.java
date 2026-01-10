package com.zhugeio.etl.pipeline.operator.gate;

import com.zhugeio.etl.common.cache.CacheKeyConstants;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.exceptions.BusinessTableReadException;
import com.zhugeio.etl.pipeline.service.MsgResolver;
import com.zhugeio.etl.common.util.CheckJSONSchemaUtil;
import com.zhugeio.tool.commons.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
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
    private static final Logger LOG = LoggerFactory.getLogger(GateFlatMapFunction.class);
    private static final long serialVersionUID = 1L;

    // kv 相关
    private final long intervalMS = 1000L * 60;
    private KvrocksClient kvrocksClient;
    private boolean kvrocksCluster;


    // 业务依赖的 mysql库中数据  从 kvrocks 进行初始化并 定时获取
    private Map<String, Integer> ak2AppIdMap ;
    private Map<Integer, Integer> appId2companyIdMap;
    private Set<String> businessSet ;
    private Map<Integer, Set<String>> ipBlackMap;
    private Map<Integer, Set<String>> uaBlackMap;

    public GateFlatMapFunction() {}

    @Override
    public void open(Configuration parameters) throws Exception {
        String kvrocksHost = Config.getString(Config.KVROCKS_HOST, "localhost");
        int kvrocksPort = Config.getInt(Config.KVROCKS_PORT, 6379);
        kvrocksCluster = Config.getBoolean(Config.KVROCKS_CLUSTER, true);
        kvrocksClient = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocksClient.init();

        if (!kvrocksClient.testConnection()) {
            throw new RuntimeException("KVRocks连接失败: " + kvrocksHost + ":" + kvrocksPort);
        }

        kvrocksCluster = Config.getBoolean(Config.KVROCKS_CLUSTER, true);

        // 初始化业务数据
        loadMysqlData();
        LOG.trace("init: load mysql data from kv-rocks finish ");


        // 设置定时更新
        AtomicBoolean isRunning = new AtomicBoolean(true);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                () -> {
                    // 这个任务会基于服务器时间，周期性执行
                    if (isRunning.get()) {
                        try {
                            loadMysqlData(); // 获取mysql数据
                        } catch (Exception e){
                            e.printStackTrace();
                            LOG.error("GateFlatMapFunction 定时任务 读取mysql执行出错",e);
                            // todo: 添加错误处理逻辑 中止程序
//                            throw new BusinessTableReadException("GateFlatMapFunction 定时任务 读取mysql执行出错");
                        }
                    }
                },
                intervalMS, // 初始延迟：0毫秒，即立即开始
                intervalMS, // 执行间隔
                TimeUnit.MILLISECONDS // 时间单位毫秒
        );
        LOG.info("GateFlatMapFunction 定时任务已启动，间隔 {} 毫秒", intervalMS);
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
        LOG.debug("GateFlatMapFunction resolveMsg: {}", resolveMsg);
        if(resolveMsg == null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg format error : resolveMsg is null");
            collector.collect(zgMessage);
            return;
        }else {
            zgMessage.setRawData(resolveMsg);
        }

        Map<String, Object> map = JsonUtil.mapFromJson(resolveMsg);
        LOG.debug("GateFlatMapFunction mapFromJson : {}", map);
        if(map==null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg format error : resolveMsg mapFromJson is null");
            return;
        }
        zgMessage.setData(map);

        String appKey = getAppKey(map);
        LOG.debug("GateFlatMapFunction getAppKey : {}", appKey);
        if (appKey == null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg not have appKey");
            return;
        }
        zgMessage.setAppKey(appKey);

        Integer appId = ak2AppIdMap.get(appKey);
        LOG.debug("GateFlatMapFunction appId : {}", appId);
        if(appId == null){
            zgMessage.setResult(-1);
            zgMessage.setError("msg not have appId");
            return;
        }

        zgMessage.setAppId(appId);
        boolean checkedBasic = CheckJSONSchemaUtil.checkBasic(resolveMsg);
        LOG.debug("GateFlatMapFunction CheckJSONSchemaUtil : {}", checkedBasic);
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
        LOG.debug("GateFlatMapFunction business : {}", business);
        zgMessage.setBusiness(business);
        map.put("business", business);
        zgMessage.setRawData(JsonUtil.toJson(map));

        // 配置了 ip 黑名单过滤，这里进行相关校验
        if (ipBlackMap.containsKey(appId)) {
            // ip校验
            String ip = "";
            if (map.containsKey("ip")) {
                ip = String.valueOf(map.get("ip"));
            }
            Set<String> ipSet = ipBlackMap.get(appId);
            if (ipSet.contains(ip)) {
                zgMessage.setResult(-1);
                zgMessage.setErrorCode(ErrorMessageEnum.IP_BLOCK.getErrorCode());
                zgMessage.setErrorDescribe(ErrorMessageEnum.IP_BLOCK.getErrorMessage());
                collector.collect(zgMessage);
                return;
            }
        }

        // 配置了 ua 黑名单过滤，这里进行相关校验
        if(uaBlackMap.containsKey(appId)){
            String ua = "";
            if (map.containsKey("ua")) {
                ua = String.valueOf(map.get("ua"));
            }
            Set<String> uaSet = uaBlackMap.get(appId);
            if (uaSet.contains(ua)) {
                zgMessage.setResult(-1);
                zgMessage.setErrorCode(ErrorMessageEnum.UA_BLOCK.getErrorCode());
                zgMessage.setErrorDescribe(ErrorMessageEnum.UA_BLOCK.getErrorMessage());
                collector.collect(zgMessage);
                return;
            }
        }

        // 主数据输出下游
        LOG.debug("GateFlatMapFunction collect : {}", zgMessage);
        collector.collect(zgMessage);
    }

    private String getAppKey(Map<String, Object> map) {
        if (map.containsKey("ak")) {
            return map.get("ak").toString();
        } else {
            return null;
        }
    }

    private void loadMysqlData(){
        long start = System.currentTimeMillis();
        retryInitTable2Company_app();
        retryInitTable2Business();
        retryInitTable2DataAccessFilter();
        LOG.trace("load mysql data from kv-rocks finish , cost : {} ms .",System.currentTimeMillis()-start);
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

    /**
     * 获取实际的 KVRocks Key (集群模式加 Hash Tag)
     */
    private String getActualKey(String key) {
        return kvrocksCluster ? "{" + key + "}" : key;
    }

    private boolean initTable2Company_app() throws SQLException {
        Map<Integer, Integer> appId2companyIdMap = new HashMap<>();
        String actualKey = getActualKey(CacheKeyConstants.CID_BY_AID_MAP);
        kvrocksClient
                .asyncHGetAll(actualKey)
                .thenAccept(
                        map -> {
                            for (Map.Entry<String, String> entry : map.entrySet()){
                                Integer appId = Integer.parseInt(entry.getKey());
                                Integer companyId = Integer.parseInt(entry.getValue());
                                appId2companyIdMap.put(appId, companyId);
                            }
                        }
                ).join();
        actualKey = getActualKey(CacheKeyConstants.APP_KEY_APP_ID_MAP);
        Map<String, Integer> ak2AppIdMap = new HashMap<>();
        kvrocksClient
                .asyncHGetAll(actualKey)
                .thenAccept(
                        map -> {
                            for (Map.Entry<String, String> entry : map.entrySet()){
                                String ak = entry.getKey();
                                Integer appId = Integer.parseInt(entry.getValue());
                                ak2AppIdMap.put(ak, appId);
                            }
                        }
                ).join();
        if(appId2companyIdMap.isEmpty() ){
            LOG.warn("GateFlatMapFunction appId2companyIdMap.isEmpty ");
            return false;
        }

        if(ak2AppIdMap.isEmpty()){
            LOG.warn("GateFlatMapFunction ak2AppIdMap.isEmpty() ");
            return false;
        }
        this.appId2companyIdMap = appId2companyIdMap;
        LOG.trace("GateFlatMapFunction appId2companyIdMap : {}", appId2companyIdMap);
        this.ak2AppIdMap = ak2AppIdMap;
        LOG.trace("GateFlatMapFunction ak2AppIdMap : {}", ak2AppIdMap);
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
        kvrocksClient
                .asyncSmembers(getActualKey(CacheKeyConstants.BUSINESS_MAP))
                .thenAccept(businessSet::addAll);
        this.businessSet = businessSet;
        LOG.trace("GateFlatMapFunction businessSet : {}", businessSet);
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
        Map<Integer, Set<String>> ipBlackMap = new HashMap<>();
        String actualKey = getActualKey(CacheKeyConstants.IP_BLACK_MAP);
        kvrocksClient
                .asyncHGetAll(actualKey)
                .thenAccept(
                        map -> {
                            for (Map.Entry<String, String> entry : map.entrySet()){
                                Integer appId = Integer.parseInt(entry.getKey());
                                String ipSting = entry.getValue();
                                if(!StringUtils.isEmpty(ipSting) && ipSting.length()>4){
                                    Set<String> ipSet = Arrays.stream(ipSting.substring(1, ipSting.length() - 1).replace("\"", "").split(","))
                                            .collect(Collectors.toSet());
                                    ipBlackMap.put(appId, ipSet);
                                }else {
                                    ipBlackMap.put(appId, Collections.<String>emptySet());
                                }
                            }
                        }
                );

        Map<Integer, Set<String>> uaBlackMap = new HashMap<>();
        actualKey = getActualKey(CacheKeyConstants.UA_BLACK_MAP);
        kvrocksClient
                .asyncHGetAll(actualKey)
                .thenAccept(
                        map -> {
                            for (Map.Entry<String, String> entry : map.entrySet()){
                                Integer appId = Integer.parseInt(entry.getKey());
                                String uaSting = entry.getValue();
                                if(!StringUtils.isEmpty(uaSting) && uaSting.length()>4){
                                    Set<String> uaSet = Arrays.stream(uaSting.substring(1, uaSting.length() - 1).replace("\"", "").split(","))
                                            .collect(Collectors.toSet());
                                    uaBlackMap.put(appId, uaSet);
                                }else {
                                    uaBlackMap.put(appId, Collections.<String>emptySet());
                                }

                            }
                        }
                );

        this.ipBlackMap = ipBlackMap;
        LOG.trace("GateFlatMapFunction ipBlackMap : {}", ipBlackMap);
        this.uaBlackMap = ipBlackMap;
        LOG.trace("GateFlatMapFunction uaBlackMap : {}", uaBlackMap);
        return true;
    }
}
