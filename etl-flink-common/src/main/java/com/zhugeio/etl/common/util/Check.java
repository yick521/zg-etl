package com.zhugeio.etl.common.util;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JSON Schema 验证工具
 *
 * 支持从以下位置加载 schema 文件（按优先级）：
 * 1. 当前工作目录（Flink -yt 上传的文件）
 * 2. Classpath 资源
 */
public class Check {

    public static final Schema basicSchema = loadBasicSchema();

    /**
     * 加载基础 Schema
     * 优先从本地文件加载，回退到 classpath
     */
    private static Schema loadBasicSchema() {
        String classpathPath = "/schema/basicSchema.json";
        String localPath = "schema/basicSchema.json";

        try {
            InputStream is = null;
            String loadedFrom = null;

            // 1. 优先从当前目录读取（Flink -yt 上传的文件）
            File localFile = new File(localPath);
            if (localFile.exists()) {
                is = new FileInputStream(localFile);
                loadedFrom = "local file: " + localFile.getAbsolutePath();
            }

            // 2. 尝试从 config 子目录读取
            if (is == null) {
                File configFile = new File("config/schema/basicSchema.json");
                if (configFile.exists()) {
                    is = new FileInputStream(configFile);
                    loadedFrom = "config file: " + configFile.getAbsolutePath();
                }
            }

            // 3. 回退到 classpath
            if (is == null) {
                is = Check.class.getResourceAsStream(classpathPath);
                if (is != null) {
                    loadedFrom = "classpath: " + classpathPath;
                }
            }

            // 4. 检查是否找到文件
            if (is == null) {
                throw new RuntimeException(
                        "Schema file not found! Searched locations:\n" +
                                "  1. " + new File(localPath).getAbsolutePath() + "\n" +
                                "  2. " + new File("config/schema/basicSchema.json").getAbsolutePath() + "\n" +
                                "  3. classpath:" + classpathPath
                );
            }

            System.out.println("[Check] Loading schema from " + loadedFrom);

            // 读取文件内容
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            reader.close();
            is.close();

            // 解析并编译 Schema
            JSONObject rawSchema = new JSONObject(new JSONTokener(sb.toString()));
            Schema schema = SchemaLoader.load(rawSchema);

            System.out.println("[Check] Schema loaded successfully");
            return schema;

        } catch (Exception e) {
            System.err.println("[Check] Failed to load schema: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to load basicSchema.json", e);
        }
    }

    /**
     * 从指定路径加载 Schema（保留原方法兼容性）
     *
     * @param schemaPath 资源路径
     * @return 编译好的Schema对象
     */
    public static Schema getSchema(String schemaPath) {
        try {
            InputStream is = null;

            // 提取文件名
            String fileName = schemaPath;
            if (fileName.startsWith("/")) {
                fileName = fileName.substring(1);
            }

            // 1. 尝试从当前目录读取
            File localFile = new File(fileName);
            if (localFile.exists()) {
                is = new FileInputStream(localFile);
            }

            // 2. 尝试从 config 目录读取
            if (is == null) {
                File configFile = new File("config/" + fileName);
                if (configFile.exists()) {
                    is = new FileInputStream(configFile);
                }
            }

            // 3. 回退到 classpath
            if (is == null) {
                is = Check.class.getResourceAsStream(schemaPath);
            }

            if (is == null) {
                throw new RuntimeException("Schema file not found: " + schemaPath);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            reader.close();
            is.close();

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