package com.zhugeio.etl.pipeline.operator.gate;

import com.zhugeio.etl.common.client.redis.RedisClient;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import io.lettuce.core.KeyValue;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author ningjh
 * @name sxxx
 * @date 2025/12/10
 * @description
 */
public class AdvAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger logger = LoggerFactory.getLogger(AdvAsyncOperator.class);
    private String redisHost;
    private int redisPort;
    private boolean redisCluster;
    private RedisClient redisClient;

    private transient int subtaskIndex;

    public AdvAsyncOperator() {
    }

    public AdvAsyncOperator(String redisHost, int redisPort) {
        this(redisHost, redisPort, true);
    }

    public AdvAsyncOperator(String redisHost, int redisPort, boolean redisCluster) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisCluster = redisCluster;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        redisClient = new RedisClient(redisHost, redisPort, redisCluster);
        redisClient.init();
    }

    /**
     * 异步处理
     * 广告的逻辑，判断现有数据是否符合要求，若符合则产生新数据-并删除redis中的数据
     *
     * @param zgMessage
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(ZGMessage zgMessage, ResultFuture<ZGMessage> resultFuture) throws Exception {
        if (zgMessage.getRawData().contains("zg_adtoufang") && zgMessage.getRawData().contains("$channel_click_id")) {
            List<ZGMessage> finalResultList = new ArrayList<>();
            finalResultList.add(zgMessage);
            // 保存所有需要查询的key
            Set<String> keys = new HashSet<>();
            List<?> listData = (List<?>)zgMessage.getData().get("data");
            for (Object listDatum : listData) {
                Map<?, ?> map = (Map<?, ?>) listDatum;
                String dt = String.valueOf(map.get("dt"));
                if ("adtf".equals(dt)) {
                    Map<?, ?> pr = (Map<?, ?>) map.get("pr");
                    String channelClickId = String.valueOf(pr.get("$channel_click_id"));
                    String key = "adtfad:" + channelClickId + ":rawdata";
                    keys.add(key);
                }
            }

            // 没有符合条件的元素
            if(keys.isEmpty()){
                resultFuture.complete(finalResultList);
                return;
            }

            // 处理符合条件的元素
            redisClient
                    .asyncMGet(keys.toArray(new String[0]))
                    .thenCompose(entryList -> {
                        if (entryList != null) { // 若key存在则删除key，并且根据value生成一条新数据
                            // 1. 过滤出 value 不为 null 的 KeyValue
                            List<KeyValue<String, String>> existingEntries = entryList.stream()
                                    .filter(kv -> kv.getValue() != null)
                                    .collect(Collectors.toList());

                            if (existingEntries.isEmpty()) {
                                return CompletableFuture.completedFuture(Collections.<ZGMessage>emptyList());
                            }

                            // 2. 只删除存在的 key
                            String[] keysToDelete = existingEntries.stream()
                                    .map(KeyValue::getKey)
                                    .toArray(String[]::new);

                            // 3. 只处理存在的 value
                            List<String> existingValues = existingEntries.stream()
                                    .map(KeyValue::getValue)
                                    .collect(Collectors.toList());

                            // 4. 异步删除 key , 之后回调产生结果集
                            return redisClient.asyncDel(keysToDelete)
                                    .thenCompose(v -> {
                                        List<ZGMessage> newMessages = existingValues.stream()
                                                .map(valueRow -> {
                                                    ZGMessage newMessage = new ZGMessage();
                                                    newMessage.setAppId(zgMessage.getAppId());
                                                    newMessage.setAppKey(zgMessage.getAppKey());
                                                    newMessage.setRawData(valueRow);
                                                    return newMessage;
                                                }).collect(Collectors.toList());
                                        return CompletableFuture.completedFuture(newMessages);
                                    });
                        }
                        return CompletableFuture.completedFuture(Collections.<ZGMessage>emptyList());
                    }).whenComplete((newMessages, throwable) -> {
                        if(throwable != null){
                            resultFuture.complete(finalResultList);
                        }else {
                            finalResultList.addAll(newMessages);
                            resultFuture.complete(finalResultList);
                        }
                    });
        }else {
            resultFuture.complete(Collections.singleton(zgMessage));
        }
    }
}
