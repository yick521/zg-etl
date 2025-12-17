package com.zhugeio.etl.pipeline.operator.id;

import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.tool.commons.JsonUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CheckJsonAndOwnerOperator extends RichMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(CheckJsonAndOwnerOperator.class);

    @Override
    public ZGMessage map(ZGMessage zgMessage) throws Exception {
        try {
            // 尝试将原始数据解析为Map
            Map<String, Object> map = JsonUtil.mapFromJson(zgMessage.getRawData());
            Map<String, Object> errMap = JsonUtil.mapFromJson(zgMessage.getRawData());

            if (map == null) {
                // JSON解析失败
                zgMessage.setResult(-1);
                zgMessage.setError("msg not json");
                zgMessage.setErrorCode(ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorCode());
                zgMessage.setErrorDescribe(ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorMessage());
                zgMessage.setErrData(zgMessage.getData());
                logger.error("{}: {}", ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorMessage(), zgMessage.getRawData());
            } else {
                // 处理owner字段
                Object ownerObj = map.get("owner");
                String owner = (ownerObj instanceof String) ? (String) ownerObj : "";

                // 根据owner值设置对应的owner
                switch (owner) {
                    case "zg_adp":
                        owner = "zg_adp";
                        break;
                    case "zg_mkt":
                        owner = "zg_mkt";
                        break;
                    case "zg_cdp":
                        owner = "zg_cdp";
                        break;
                    default:
                        owner = "zg";
                        break;
                }

                map.put("owner", owner);
                zgMessage.setData(map);
                errMap.remove("data");
                zgMessage.setErrData(errMap);
            }
        } catch (Exception e) {
            // 处理其他可能的异常
            zgMessage.setResult(-1);
            zgMessage.setError("msg not json");
            zgMessage.setErrorCode(ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorCode());
            zgMessage.setErrorDescribe(ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorMessage());
            zgMessage.setErrData(zgMessage.getData());
            logger.error("{}: {}", ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorMessage(), zgMessage.getRawData(), e);
        }
        return zgMessage;
    }
}