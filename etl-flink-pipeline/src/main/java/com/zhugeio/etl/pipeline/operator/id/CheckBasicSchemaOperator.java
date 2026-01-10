package com.zhugeio.etl.pipeline.operator.id;

import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.Check;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CheckBasicSchemaOperator extends RichMapFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(CheckBasicSchemaOperator.class);

    @Override
    public ZGMessage map(ZGMessage zgMessage) throws Exception {
        Check.Either<List<String>, Boolean> checkResult = Check.checkBasic(zgMessage.getJson());
        if (checkResult.isLeft()) {
            zgMessage.setResult(-1);
            zgMessage.setError(String.join("||", checkResult.getLeft()));
            zgMessage.setErrorCode(zgMessage.getErrorCode() + ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorCode());
            zgMessage.setErrorDescribe(zgMessage.getErrorDescribe() + ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorMessage());
            zgMessage.setErrData(zgMessage.getData());
            logger.error("checkBasic error: {}", zgMessage);
        }
        return zgMessage;
    }
}