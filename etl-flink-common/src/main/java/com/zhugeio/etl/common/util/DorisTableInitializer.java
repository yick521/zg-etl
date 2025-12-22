package com.zhugeio.etl.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Doris 表初始化工具
 * 
 * 用于在 Flink Job 启动前执行建表 SQL
 */
public class DorisTableInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DorisTableInitializer.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DorisTableInitializer(String feNodes, String database,
                                 String username, String password) {
        // feNodes 格式: "host1:8030,host2:8030"，取第一个节点
        String firstNode = feNodes.split(",")[0].trim();
        String host = firstNode.split(":")[0];
        // HTTP 端口是 8030，JDBC 端口是 9030
        int port = 9030;
        this.jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        this.username = username;
        this.password = password;
    }

    /**
     * 从文件路径加载并执行 SQL
     */
    public void init(String sqlFilePath) {
        LOG.info("从文件加载 SQL: {}", sqlFilePath);

        try (BufferedReader reader = new BufferedReader(
                new FileReader(sqlFilePath))) {
            String content = readAll(reader);
            executeSqlStatements(content);
        } catch (Exception e) {
            LOG.error("加载 SQL 文件失败: {}", sqlFilePath, e);
            throw new RuntimeException("加载 SQL 文件失败: " + sqlFilePath, e);
        }
    }

    private void executeSqlStatements(String sqlContent) {
        List<String> statements = parseSqlStatements(sqlContent);
        LOG.info("解析到 {} 条 SQL 语句", statements.size());

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement()) {

            for (String sql : statements) {
                LOG.info("执行 SQL: {}...", sql.substring(0, Math.min(80, sql.length())));
                stmt.execute(sql);
            }

            LOG.info("所有 SQL 语句执行完成");

        } catch (Exception e) {
            LOG.error("执行 SQL 失败", e);
            throw new RuntimeException("执行 SQL 失败", e);
        }
    }

    /**
     * 解析 SQL 文件，分割为单条语句
     * - 忽略注释
     * - 按分号分割
     */
    private List<String> parseSqlStatements(String content) {
        List<String> statements = new ArrayList<>();

        // 移除多行注释
        content = content.replaceAll("/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/", "");

        StringBuilder current = new StringBuilder();
        for (String line : content.split("\n")) {
            // 移除单行注释
            int commentIndex = line.indexOf("--");
            if (commentIndex >= 0) {
                line = line.substring(0, commentIndex);
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            current.append(line).append(" ");

            if (line.endsWith(";")) {
                String sql = current.toString().trim();
                sql = sql.substring(0, sql.length() - 1).trim();
                if (!sql.isEmpty()) {
                    statements.add(sql);
                }
                current = new StringBuilder();
            }
        }

        return statements;
    }

    private String readAll(BufferedReader reader) throws Exception {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }
}
