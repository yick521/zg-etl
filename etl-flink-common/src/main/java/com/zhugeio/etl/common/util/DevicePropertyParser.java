package com.zhugeio.etl.common.util;

import com.zhugeio.etl.common.model.DeviceProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 设备属性解析器
 *
 * 功能:
 * 1. 品牌名标准化
 * 2. 型号识别和标准化
 * 3. 设备分类判断
 * 4. 价格区间估算
 */
public class DevicePropertyParser implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DevicePropertyParser.class);
    private static final long serialVersionUID = 1L;

    // ========== 品牌名标准化映射 ==========
    private static final Map<String, String> BRAND_MAPPING = new HashMap<>();

    static {
        // 华为系
        BRAND_MAPPING.put("huawei", "Huawei");
        BRAND_MAPPING.put("honor", "Honor");
        BRAND_MAPPING.put("荣耀", "Honor");

        // 小米系
        BRAND_MAPPING.put("xiaomi", "Xiaomi");
        BRAND_MAPPING.put("mi", "Xiaomi");
        BRAND_MAPPING.put("redmi", "Redmi");
        BRAND_MAPPING.put("红米", "Redmi");

        // OPPO系
        BRAND_MAPPING.put("oppo", "OPPO");
        BRAND_MAPPING.put("realme", "Realme");
        BRAND_MAPPING.put("oneplus", "OnePlus");

        // vivo系
        BRAND_MAPPING.put("vivo", "Vivo");
        BRAND_MAPPING.put("iqoo", "iQOO");

        // 苹果
        BRAND_MAPPING.put("apple", "Apple");
        BRAND_MAPPING.put("iphone", "Apple");

        // 三星
        BRAND_MAPPING.put("samsung", "Samsung");

        // 其他品牌
        BRAND_MAPPING.put("meizu", "Meizu");
        BRAND_MAPPING.put("魅族", "Meizu");
        BRAND_MAPPING.put("lenovo", "Lenovo");
        BRAND_MAPPING.put("联想", "Lenovo");
        BRAND_MAPPING.put("zte", "ZTE");
        BRAND_MAPPING.put("中兴", "ZTE");
        BRAND_MAPPING.put("nubia", "Nubia");
        BRAND_MAPPING.put("努比亚", "Nubia");
        BRAND_MAPPING.put("黑鲨", "BlackShark");
        BRAND_MAPPING.put("黑莓", "BlackBerry");
    }

    // ========== 旗舰机型关键字 ==========
    private static final String[] FLAGSHIP_KEYWORDS = {
            "Pro", "Ultra", "Max", "Plus", "Mate", "Find", "X", "Pro+",
            "GT", "Ace", "旗舰", "折叠"
    };

    // ========== 折叠屏关键字 ==========
    private static final String[] FOLDABLE_KEYWORDS = {
            "Fold", "Flip", "Mix Fold", "Mate X", "折叠"
    };

    // ========== 5G关键字 ==========
    private static final Pattern FIVE_G_PATTERN = Pattern.compile(
            "5G|5g|全网通", Pattern.CASE_INSENSITIVE);

    /**
     * 解析设备属性
     */
    public DeviceProperty parse(String brand, String model) {
        DeviceProperty property = new DeviceProperty(brand, model);

        if (brand == null || brand.trim().isEmpty()) {
            setDefaults(property);
            return property;
        }

        try {
            // 1. 标准化品牌名
            String standardBrand = standardizeBrand(brand);
            property.setStandardBrand(standardBrand);

            // 2. 标准化型号
            String standardModel = standardizeModel(model);
            property.setStandardModel(standardModel);

            // 3. 判断设备分类
            String category = determineCategory(standardBrand, standardModel);
            property.setDeviceCategory(category);

            // 4. 判断价格区间
            String priceRange = determinePriceRange(standardBrand, standardModel, category);
            property.setPriceRange(priceRange);

            // 5. 判断是否支持5G
            boolean is5G = is5GSupported(model);
            property.set5GSupported(is5G);

            // 6. 判断是否为折叠屏
            boolean isFoldable = isFoldableDevice(model);
            property.setFoldable(isFoldable);

            // 7. 提取发布年份 (如果可能)
            String year = extractReleaseYear(standardModel);
            property.setReleaseYear(year);

        } catch (Exception e) {
            LOG.error("设备属性解析失败: brand={}, model={}", brand, model, e);
            setDefaults(property);
        }

        return property;
    }

    /**
     * 标准化品牌名
     */
    private String standardizeBrand(String brand) {
        if (brand == null || brand.isEmpty()) {
            return "Unknown";
        }

        String lower = brand.toLowerCase().trim();

        // 精确匹配
        if (BRAND_MAPPING.containsKey(lower)) {
            return BRAND_MAPPING.get(lower);
        }

        // 模糊匹配
        for (Map.Entry<String, String> entry : BRAND_MAPPING.entrySet()) {
            if (lower.contains(entry.getKey())) {
                return entry.getValue();
            }
        }

        // 首字母大写
        return capitalize(brand);
    }

    /**
     * 标准化型号
     */
    private String standardizeModel(String model) {
        if (model == null || model.isEmpty()) {
            return "";
        }

        // 去除多余空格
        String cleaned = model.trim().replaceAll("\\s+", " ");

        // 去除特殊字符 (保留字母、数字、空格、+-符号)
        cleaned = cleaned.replaceAll("[^a-zA-Z0-9\\s+\\-]", "");

        return cleaned;
    }

    /**
     * 判断设备分类
     */
    private String determineCategory(String brand, String model) {
        if (model == null || model.isEmpty()) {
            return "Unknown";
        }

        String upperModel = model.toUpperCase();

        // 折叠屏
        for (String keyword : FOLDABLE_KEYWORDS) {
            if (upperModel.contains(keyword.toUpperCase())) {
                return "Foldable";
            }
        }

        // 旗舰机
        for (String keyword : FLAGSHIP_KEYWORDS) {
            if (upperModel.contains(keyword.toUpperCase())) {
                return "Flagship";
            }
        }

        // iPhone 特殊处理
        if ("Apple".equals(brand)) {
            if (upperModel.contains("PRO") || upperModel.contains("MAX")) {
                return "Flagship";
            } else if (upperModel.contains("SE")) {
                return "Entry-level";
            } else {
                return "Mid-range";
            }
        }

        // 小米 Redmi/红米 系列通常是中低端
        if ("Redmi".equals(brand)) {
            if (upperModel.contains("K") || upperModel.contains("NOTE")) {
                return "Mid-range";
            } else {
                return "Entry-level";
            }
        }

        // 默认中端
        return "Mid-range";
    }

    /**
     * 判断价格区间
     */
    private String determinePriceRange(String brand, String model, String category) {
        // 基于分类
        if ("Foldable".equals(category)) {
            return "Premium";  // 折叠屏通常是高端
        }

        if ("Flagship".equals(category)) {
            return "Premium";  // 旗舰机
        }

        if ("Entry-level".equals(category)) {
            return "Budget";   // 入门级
        }

        // 基于品牌
        if ("Apple".equals(brand)) {
            return "Premium";  // iPhone 通常高端
        }

        if ("Redmi".equals(brand) || "Realme".equals(brand)) {
            return "Budget";   // 性价比品牌
        }

        // 默认中端
        return "Mid";
    }

    /**
     * 判断是否支持5G
     */
    private boolean is5GSupported(String model) {
        if (model == null || model.isEmpty()) {
            return false;
        }

        return FIVE_G_PATTERN.matcher(model).find();
    }

    /**
     * 判断是否为折叠屏
     */
    private boolean isFoldableDevice(String model) {
        if (model == null || model.isEmpty()) {
            return false;
        }

        String upper = model.toUpperCase();
        for (String keyword : FOLDABLE_KEYWORDS) {
            if (upper.contains(keyword.toUpperCase())) {
                return true;
            }
        }

        return false;
    }

    /**
     * 提取发布年份
     */
    private String extractReleaseYear(String model) {
        if (model == null || model.isEmpty()) {
            return "";
        }

        // 简单匹配 4 位数字年份
        if (model.matches(".*20[12]\\d.*")) {
            // 提取 2010-2029 的年份
            java.util.regex.Matcher matcher =
                    Pattern.compile("(20[12]\\d)").matcher(model);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }

        return "";
    }

    /**
     * 首字母大写
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
    }

    /**
     * 设置默认值
     */
    private void setDefaults(DeviceProperty property) {
        if (property.getStandardBrand() == null) {
            property.setStandardBrand("Unknown");
        }
        if (property.getStandardModel() == null) {
            property.setStandardModel("");
        }
        if (property.getDeviceCategory() == null) {
            property.setDeviceCategory("Unknown");
        }
        if (property.getPriceRange() == null) {
            property.setPriceRange("Unknown");
        }
    }
}
