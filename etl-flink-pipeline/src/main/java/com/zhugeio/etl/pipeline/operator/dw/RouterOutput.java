package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 路由输出包装类
 * 
 * 由于 RichAsyncFunction 不直接支持侧输出，
 * 使用此类将所有类型的输出包装在一起，
 * 下游使用 RouterOutputUnpacker 解包并路由到侧输出
 */
public class RouterOutput implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final List<UserRow> userRows;
    private final List<DeviceRow> deviceRows;
    private final List<UserPropertyRow> userPropertyRows;
    private final List<EventAttrRow> eventAttrRows;
    
    public RouterOutput() {
        this.userRows = new ArrayList<>();
        this.deviceRows = new ArrayList<>();
        this.userPropertyRows = new ArrayList<>();
        this.eventAttrRows = new ArrayList<>();
    }
    
    public static RouterOutput empty() {
        return new RouterOutput();
    }
    
    // ============ 添加方法 ============
    
    public void addUserRow(UserRow row) {
        if (row != null) {
            userRows.add(row);
        }
    }
    
    public void addDeviceRow(DeviceRow row) {
        if (row != null) {
            deviceRows.add(row);
        }
    }
    
    public void addUserPropertyRow(UserPropertyRow row) {
        if (row != null) {
            userPropertyRows.add(row);
        }
    }
    
    public void addEventAttrRow(EventAttrRow row) {
        if (row != null) {
            eventAttrRows.add(row);
        }
    }
    
    // ============ 获取方法 ============
    
    public List<UserRow> getUserRows() {
        return userRows;
    }
    
    public List<DeviceRow> getDeviceRows() {
        return deviceRows;
    }
    
    public List<UserPropertyRow> getUserPropertyRows() {
        return userPropertyRows;
    }
    
    public List<EventAttrRow> getEventAttrRows() {
        return eventAttrRows;
    }
    
    // ============ 判断方法 ============
    
    public boolean isEmpty() {
        return userRows.isEmpty() && deviceRows.isEmpty() 
                && userPropertyRows.isEmpty() && eventAttrRows.isEmpty();
    }
    
    public boolean hasUserRows() {
        return !userRows.isEmpty();
    }
    
    public boolean hasDeviceRows() {
        return !deviceRows.isEmpty();
    }
    
    public boolean hasUserPropertyRows() {
        return !userPropertyRows.isEmpty();
    }
    
    public boolean hasEventAttrRows() {
        return !eventAttrRows.isEmpty();
    }
    
    public int getTotalCount() {
        return userRows.size() + deviceRows.size() 
                + userPropertyRows.size() + eventAttrRows.size();
    }
    
    @Override
    public String toString() {
        return String.format("RouterOutput{user=%d, device=%d, userProp=%d, eventAttr=%d}",
                userRows.size(), deviceRows.size(), 
                userPropertyRows.size(), eventAttrRows.size());
    }
}
