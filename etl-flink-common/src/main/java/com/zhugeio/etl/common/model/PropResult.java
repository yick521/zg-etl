package com.zhugeio.etl.common.model;

import java.io.Serializable;

public class PropResult  implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer propId;
    private final String originalName;

    public PropResult(Integer propId, String originalName) {
        this.propId = propId;
        this.originalName = originalName;
    }

    public Integer getPropId() {
        return propId;
    }

    public String getOriginalName() {
        return originalName;
    }

    @Override
    public String toString() {
        return "PropResult{" +
                "propId=" + propId +
                ", originalName='" + originalName + '\'' +
                '}';
    }
}
