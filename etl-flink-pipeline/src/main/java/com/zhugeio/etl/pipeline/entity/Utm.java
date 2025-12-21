package com.zhugeio.etl.pipeline.entity;

/**
 * UTM参数实体类
 */
public class Utm {
    
    /**
     * 活动名称
     */
    private String utmCampaign;
    
    /**
     * 广告来源
     */
    private String utmSource;
    
    /**
     * 广告媒介
     */
    private String utmMedium;
    
    /**
     * 关键词
     */
    private String utmTerm;
    
    /**
     * 广告内容
     */
    private String utmContent;

    // 无参构造器
    public Utm() {
    }

    // 全参构造器
    public Utm(String utmCampaign, String utmSource, String utmMedium, String utmTerm, String utmContent) {
        this.utmCampaign = utmCampaign;
        this.utmSource = utmSource;
        this.utmMedium = utmMedium;
        this.utmTerm = utmTerm;
        this.utmContent = utmContent;
    }

    // Getter 和 Setter 方法（驼峰命名）
    public String getUtmCampaign() {
        return utmCampaign;
    }

    public void setUtmCampaign(String utmCampaign) {
        this.utmCampaign = utmCampaign;
    }

    public String getUtmSource() {
        return utmSource;
    }

    public void setUtmSource(String utmSource) {
        this.utmSource = utmSource;
    }

    public String getUtmMedium() {
        return utmMedium;
    }

    public void setUtmMedium(String utmMedium) {
        this.utmMedium = utmMedium;
    }

    public String getUtmTerm() {
        return utmTerm;
    }

    public void setUtmTerm(String utmTerm) {
        this.utmTerm = utmTerm;
    }

    public String getUtmContent() {
        return utmContent;
    }

    public void setUtmContent(String utmContent) {
        this.utmContent = utmContent;
    }

    /**
     * 转换为JSON字符串
     * 注意：JSON键名保持蛇形命名以保持兼容性
     */
    public String toJsonString() {
        return "{\"utm_campaign\":\"" + (utmCampaign != null ? utmCampaign : "") +
               "\",\"utm_source\":\"" + (utmSource != null ? utmSource : "") +
               "\",\"utm_medium\":\"" + (utmMedium != null ? utmMedium : "") +
               "\",\"utm_term\":\"" + (utmTerm != null ? utmTerm : "") +
               "\",\"utm_content\":\"" + (utmContent != null ? utmContent : "") + "\"}";
    }

    @Override
    public String toString() {
        return "Utm{" +
                "utmCampaign='" + utmCampaign + '\'' +
                ", utmSource='" + utmSource + '\'' +
                ", utmMedium='" + utmMedium + '\'' +
                ", utmTerm='" + utmTerm + '\'' +
                ", utmContent='" + utmContent + '\'' +
                '}';
    }
}