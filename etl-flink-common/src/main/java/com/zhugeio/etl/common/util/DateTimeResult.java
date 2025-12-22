package com.zhugeio.etl.common.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateTimeResult {
    private final LocalDateTime dateTime;
    private final String formatted;

    public DateTimeResult(LocalDateTime dateTime) {
        this.dateTime = dateTime;
        // 使用用户期望的默认格式
        this.formatted = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    @Override
    public String toString() {
        return formatted;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DateTimeResult that = (DateTimeResult) obj;
        return dateTime.equals(that.dateTime);
    }

    public int hashCode() {
        return dateTime.hashCode();
    }
}
