package com.zhugeio.etl.common.util;

import com.zhugeio.etl.common.model.UserAgentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import useragentanalysis.Browser;
import useragentanalysis.DeviceType;
import useragentanalysis.Manufacturer;
import useragentanalysis.OperatingSystem;
import useragentanalysis.UserAgent;

import java.io.Serializable;

/**
 * User-Agent解析器
 *
 */
public class UserAgentParser implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(UserAgentParser.class);
    private static final long serialVersionUID = 1L;

    /**
     * 解析User-Agent字符串
     *
     * @param ua User-Agent字符串
     * @return 解析后的UA信息
     */
    public UserAgentInfo parse(String ua) {
        UserAgentInfo info = new UserAgentInfo(ua);

        if (ua == null || ua.trim().isEmpty()) {
            setDefaults(info);
            return info;
        }

        try {
            UserAgent ur = UserAgent.parseUserAgentString(ua);

            if (ur != null) {
                // 解析操作系统
                parseOperatingSystem(ur, info);

                // 解析浏览器
                parseBrowser(ur, info);

                // 解析设备类型和品牌
                parseDevice(ur, info);
            } else {
                setDefaults(info);
            }

        } catch (Exception e) {
            LOG.error("UA解析异常: {}", ua, e);
            setDefaults(info);
        }

        return info;
    }

    /**
     * 解析操作系统
     * 参考旧工程 EventAllTransfer.scala / EventAttrTransfer.scala
     */
    private void parseOperatingSystem(UserAgent ur, UserAgentInfo info) {
        try {
            OperatingSystem os = ur.getOperatingSystem();
            if (os != null) {
                String osString = os.toString();
                String[] tmpob = osString.split("_");

                // 操作系统名称
                String osName = tmpob[0];
                info.setOsName(osName);

                // 操作系统家族
                if (os.getGroup() != null) {
                    info.setOsFamily(os.getGroup().toString());
                } else {
                    info.setOsFamily(osName);
                }

                // 操作系统版本（取最后一个数字部分）
                if (tmpob.length > 1) {
                    String lastPart = tmpob[tmpob.length - 1];
                    if (isNumeric(lastPart)) {
                        info.setOsVersion(lastPart);
                    }
                }
            } else {
                info.setOsName("UNKNOWN");
                info.setOsFamily("UNKNOWN");
            }
        } catch (Exception e) {
            LOG.warn("解析操作系统异常: {}", e.getMessage());
            info.setOsName("UNKNOWN");
            info.setOsFamily("UNKNOWN");
        }
    }

    /**
     * 解析浏览器
     */
    private void parseBrowser(UserAgent ur, UserAgentInfo info) {
        try {
            Browser browser = ur.getBrowser();
            LOG.debug("DEBUG UA: browser={}, browserVersion={}",
                    browser, ur.getBrowserVersion());

            // 浏览器名称
            if (browser != null) {
                info.setBrowserName(browser.toString());

                // 浏览器家族
                if (browser.getGroup() != null) {
                    info.setBrowserFamily(browser.getGroup().toString());
                } else {
                    info.setBrowserFamily(browser.toString());
                }
            } else {
                info.setBrowserName("UNKNOWN");
                info.setBrowserFamily("UNKNOWN");
            }

            // 浏览器版本
            // 旧工程逻辑：browserbrand = context.ur.getBrowserVersion.toString.split("\\.").apply(0)
            if (ur.getBrowserVersion() != null) {
                String fullVersion = ur.getBrowserVersion().toString();
                LOG.debug("DEBUG UA: fullVersion={}", fullVersion);
                // 提取主版本号
                String[] parts = fullVersion.split("\\.");
                if (parts.length > 0 && !parts[0].isEmpty()) {
                    info.setBrowserVersion(parts[0]);  // 只存主版本号
                    LOG.debug("DEBUG UA: majorVersion={}", parts[0]);
                } else {
                    LOG.debug("DEBUG UA: getBrowserVersion() 返回 null");
                    info.setBrowserVersion(fullVersion);
                }
            }
        } catch (Exception e) {
            LOG.warn("解析浏览器异常: {}", e.getMessage());
            info.setBrowserName("UNKNOWN");
            info.setBrowserFamily("UNKNOWN");
        }
    }

    /**
     * 解析设备类型和品牌
     */
    private void parseDevice(UserAgent ur, UserAgentInfo info) {
        try {
            OperatingSystem os = ur.getOperatingSystem();

            if (os != null) {
                // 设备类型
                DeviceType deviceType = os.getDeviceType();
                if (deviceType != null) {
                    info.setDeviceType(mapDeviceType(deviceType));
                }

                // 设备品牌/制造商
                Manufacturer manufacturer = os.getManufacturer();
                if (manufacturer != null && manufacturer != Manufacturer.OTHER) {
                    info.setDeviceBrand(manufacturer.toString());
                }
            }

            // 如果设备类型还是空，根据OS推断
            if (info.getDeviceType() == null) {
                inferDeviceType(info);
            }

        } catch (Exception e) {
            LOG.warn("解析设备异常: {}", e.getMessage());
            inferDeviceType(info);
        }
    }

    /**
     * 映射设备类型枚举到字符串
     */
    private String mapDeviceType(DeviceType deviceType) {
        if (deviceType == null) {
            return "UNKNOWN";
        }

        switch (deviceType) {
            case MOBILE:
                return "Mobile";
            case TABLET:
                return "Tablet";
            case COMPUTER:
                return "Desktop";
            case DMR:
            case GAME_CONSOLE:
            case WEARABLE:
                return deviceType.toString();
            default:
                return "UNKNOWN";
        }
    }

    /**
     * 根据操作系统推断设备类型（兜底逻辑）
     * 参考旧工程逻辑
     */
    private void inferDeviceType(UserAgentInfo info) {
        String osName = info.getOsName();
        if (osName == null) {
            info.setDeviceType("UNKNOWN");
            return;
        }

        String osLower = osName.toLowerCase();

        if (osLower.contains("android")) {
            if (info.getDeviceType() == null) {
                info.setDeviceType("Mobile");
            }
        } else if (osLower.contains("ios") || osLower.contains("iphone")) {
            info.setDeviceType("Mobile");
            if (info.getDeviceBrand() == null) {
                info.setDeviceBrand("Apple");
            }
        } else if (osLower.contains("ipad")) {
            info.setDeviceType("Tablet");
            if (info.getDeviceBrand() == null) {
                info.setDeviceBrand("Apple");
            }
        } else if (osLower.contains("windows")) {
            info.setDeviceType("Desktop");
        } else if (osLower.contains("mac")) {
            info.setDeviceType("Desktop");
            if (info.getDeviceBrand() == null) {
                info.setDeviceBrand("Apple");
            }
        } else if (osLower.contains("linux")) {
            info.setDeviceType("Desktop");
        } else {
            if (info.getDeviceType() == null) {
                info.setDeviceType("UNKNOWN");
            }
        }
    }

    /**
     * 检查字符串是否为数字
     */
    private boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c) && c != '.') {
                return false;
            }
        }
        return true;
    }

    /**
     * 设置默认值
     */
    private void setDefaults(UserAgentInfo info) {
        if (info.getOsName() == null) {
            info.setOsName("UNKNOWN");
        }
        if (info.getOsFamily() == null) {
            info.setOsFamily("UNKNOWN");
        }
        if (info.getBrowserName() == null) {
            info.setBrowserName("UNKNOWN");
        }
        if (info.getBrowserFamily() == null) {
            info.setBrowserFamily("UNKNOWN");
        }
        if (info.getDeviceType() == null) {
            info.setDeviceType("UNKNOWN");
        }
    }
}