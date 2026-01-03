package com.zhugeio.etl.common.model;

import java.io.Serializable;

/**
 * 属性结果类
 * 存储属性ID和列名
 */
public class AttrResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer attrId;
    private String columnName;

    public AttrResult(Integer attrId, String columnName) {
        this.attrId = attrId;
        this.columnName = columnName;
    }

    public Integer getAttrId() {
        return attrId;
    }

    public void setAttrId(Integer attrId) {
        this.attrId = attrId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
