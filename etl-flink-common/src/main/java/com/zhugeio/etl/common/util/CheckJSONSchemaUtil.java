package com.zhugeio.etl.common.util;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JSON Schema 验证工具 (Java 8 版本)
 */
public class CheckJSONSchemaUtil implements Serializable {

    // 预加载基础schema，使用静态final保证线程安全
    private static final Schema BASIC_SCHEMA = loadSchema("/basicSchema.json");

    /**
     * 从类路径资源加载并编译JSON Schema
     * @param schemaPath 资源路径
     * @return 编译好的Schema对象
     * @throws RuntimeException 如果资源读取或解析失败
     */
    private static Schema loadSchema(String schemaPath) {
        // 使用try-with-resources确保流正确关闭
        try (InputStream is = CheckJSONSchemaUtil.class.getResourceAsStream(schemaPath);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(is, StandardCharsets.UTF_8))) {

            // 使用Java 8 Stream API高效读取文件所有行
            String rawJson = reader.lines().collect(Collectors.joining("\n"));
            JSONObject rawSchema = new JSONObject(new JSONTokener(rawJson));
            return SchemaLoader.load(rawSchema);

        } catch (Exception e) {
            // 将检查异常转换为运行时异常，简化调用
            throw new RuntimeException("Failed to load schema from: " + schemaPath, e);
        }
    }

    /**
     * 验证JSON字符串是否符合基础Schema
     * @param jsonBasic 待验证的JSON字符串
     * @return 一个Optional对象:
     *         - 如果验证成功，Optional包含一个空的错误列表
     *         - 如果验证失败，Optional包含错误信息字符串列表
     *         (用Optional.empty()替代Scala的Right(true)，用Optional.of(errors)替代Left)
     */
    public static boolean checkBasic(String jsonBasic) {
        try {
            JSONObject jsonToValidate = new JSONObject(new JSONTokener(jsonBasic));
            BASIC_SCHEMA.validate(jsonToValidate);
            // 验证成功，返回一个空的Optional（类似于Scala的Right）
            return true;
        } catch (ValidationException e) {
            // 专门处理验证失败异常
            List<String> errorMessages;
            if (e.getViolationCount() == 1) {
                errorMessages = Collections.singletonList(e.getMessage());
            } else {
                // 将异常集合转换为错误信息列表
                errorMessages = e.getCausingExceptions().stream()
                                 .map(Throwable::getMessage)
                                 .collect(Collectors.toList());
            }
            // 验证失败，返回包含错误列表的Optional（类似于Scala的Left）
            return false;

        } catch (Exception e) {
            // 处理其他所有异常（如JSON解析异常）
            return false;
        }
    }
}