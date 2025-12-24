package com.zhugeio.etl.common.util;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JSON Schema 验证工具
 */
public class Check {

    public static final Schema basicSchema = getSchema("/basicSchema.json");

    /**
     * 从类路径资源加载并编译JSON Schema
     *
     * @param schemaPath 资源路径
     * @return 编译好的Schema对象
     */
    public static Schema getSchema(String schemaPath) {
        try (InputStream is = Check.class.getResourceAsStream(schemaPath);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(is, StandardCharsets.UTF_8))) {

            StringBuilder sb = new StringBuilder();
            String line = reader.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = reader.readLine();
            }

            JSONObject rawSchema = new JSONObject(new JSONTokener(sb.toString()));
            return SchemaLoader.load(rawSchema);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load schema from: " + schemaPath, e);
        }
    }

    /**
     * 验证JSON字符串是否符合基础Schema
     *
     * @param jsonBasic 待验证的JSON字符串
     * @return 验证结果，Right表示验证成功，Left表示验证失败并包含错误信息列表
     */
    public static Either<List<String>, Boolean> checkBasic(String jsonBasic) {
        try {
            basicSchema.validate(new JSONObject(new JSONTokener(jsonBasic)));
            return Either.Right(true);
        } catch (ValidationException e) {
            if (e.getViolationCount() == 1) {
                return Either.Left(Collections.singletonList(e.getMessage()));
            } else {
                List<String> errorMessages = new ArrayList<>();
                e.getCausingExceptions().forEach(ex -> errorMessages.add(ex.getMessage()));
                return Either.Left(errorMessages);
            }
        } catch (Exception e1) {
            return Either.Left(Collections.singletonList(e1.getMessage()));
        }
    }

    /**
     * Scala Either 类型的简单Java实现
     *
     * @param <L> Left类型
     * @param <R> Right类型
     */
    public static class Either<L, R> {
        private final L left;
        private final R right;
        private final boolean isRight;

        private Either(L left, R right, boolean isRight) {
            this.left = left;
            this.right = right;
            this.isRight = isRight;
        }

        public static <L, R> Either<L, R> Left(L value) {
            return new Either<>(value, null, false);
        }

        public static <L, R> Either<L, R> Right(R value) {
            return new Either<>(null, value, true);
        }

        public boolean isRight() {
            return isRight;
        }

        public boolean isLeft() {
            return !isRight;
        }

        public L getLeft() {
            if (isRight) {
                throw new UnsupportedOperationException("Cannot get left value from Right");
            }
            return left;
        }

        public R getRight() {
            if (!isRight) {
                throw new UnsupportedOperationException("Cannot get right value from Left");
            }
            return right;
        }
    }
}