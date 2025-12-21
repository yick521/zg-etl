package com.zhugeio.etl.common.sink;

import java.io.Serializable;
import java.util.function.Function;

/**
 * 可序列化的 Function 接口
 * 
 * Flink 要求所有算子和函数都必须可序列化，
 * 而标准的 java.util.function.Function 不是 Serializable 的。
 * 
 * 使用方式：
 * <pre>
 * // 方法引用会自动推断为 SerializableFunction
 * SerializableFunction<UserRow, Integer> extractor = UserRow::getAppId;
 * 
 * // Lambda 表达式
 * SerializableFunction<String, Integer> parser = s -> Integer.parseInt(s);
 * </pre>
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
