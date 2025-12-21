package com.zhugeio.etl.common.util;

import com.zhugeio.tool.commons.FormatUtil;
import com.zhugeio.tool.commons.JsonUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimUtils {
    static Map<String, Map<String, Integer>> DIMS = new HashMap<String, Map<String, Integer>>();

    static void init() {
        try {
            String json = IOUtils.toString(DimUtils.class.getResourceAsStream("/dim.json"), "UTF-8");
            Map<String, Object> dimObject = JsonUtil.mapFromJson(json);
            Map<String, Object> dims = (Map<String, Object>) dimObject.get("dims");
            for (String dim : dims.keySet()) {
                Map<String, Integer> map = new HashMap<String, Integer>();
                List<Map<String, Object>> list = (List<Map<String, Object>>) dims.get(dim);
                for (Map<String, Object> m : list) {
                    Integer id = (Integer) m.get("id");
                    List<String> keys = (List<String>) m.get("keys");
                    for (String key : keys) {
                        map.put(key.toLowerCase(), id);
                    }
                }
                DIMS.put(dim, map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static {
        init();
    }

    static int getDimId(String dim, String key) {
        if (key == null) {
            return 0;
        }

        String k = key.toLowerCase();
        Map<String, Integer> dims = DIMS.get(dim);
        return dims.getOrDefault(k, 0);
    }

    public static int carrier(String s) {
        if (s != null && s.matches("\\d{6}")) {
            return FormatUtil.toInt(s);
        }
        return 0;
    }

    public static int jail(String key) {
        return getDimId("jail", key);
    }

    public static int pirate(String key) {
        return getDimId("pirate", key);
    }

    public static int gender(String key) {
        return getDimId("gender", key);
    }

    public static String birthday(String s) {
        return FormatUtil.formatDate(s, "yyyy/MM/dd", "yyyyMMdd");
    }

    public static String phone(String s) {
        if (s == null)
            return null;
        if (s.length() > 11)
            s = s.substring(s.length() - 11);
        if (s.matches("\\d+")) {
            return s;
        }

        return null;
    }

    public static int sdk(String key) {
        return getDimId("sdk", key);
    }

    public static boolean isSdkvVersionValid(String sdkv) {
		if (sdkv == null) {
			return false;
		}
		return sdkv.matches("^v(\\d+\\.)*\\d+");
	}

	public static int compareVersion(String sdkv1, String sdkv2) {
		String[] sdkv1Array = sdkv1.replace("v", "").split("\\.");
		String[] sdkv2Array = sdkv2.replace("v", "").split("\\.");
		for (int i = 0; i < sdkv1Array.length; i++) {
			if (sdkv2Array.length < i + 1) {
				return 1;
			}
			int sdk1 = FormatUtil.toInt(sdkv1Array[i]);
			int sdk2 = FormatUtil.toInt(sdkv2Array[i]);
			if (sdk1 != sdk2) {
				return sdk1 > sdk2 ? 1 : -1;
			}
		}
		return 0;
	}

	public static double getSdkV(String sdkv) {
		if (sdkv == null || sdkv.length() == 1) {
			return 0;
		} else {
			return FormatUtil.toDouble(sdkv.substring(1));
		}
	}

	public static void main(String[] args) {
		System.out.println(compareVersion("v1.6.2", "v1.6.0"));
	}
    public static int eventType(String key) {
        return getDimId("event_type", key);
    }

    public static long timestamp(int sdk, long sts, long ts) {
        // 网站采用服务端时间戳
        // Android & IOS采用客户端时间戳 (???都采用服务端)
        if (sdk == 3)
            return sts;
        else
            return ts;
    }
    
    

    public static String newSessionId(int did, String sessionId) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumIntegerDigits(9);
        nf.setMinimumIntegerDigits(9);
        return sessionId + nf.format(did);
    }

    public static int network(String key) {
        return getDimId("network", key);
    }

    public static int net(int sdk, int net, int mnet, String radio) {
        int ret = 0;
        if (sdk == 1) {
            ret = net(net, mnet);
        } else if (sdk == 3) {
            ret = netIOS(net, radio);
        }

        return ret;
    }

    public static int netIOS(int net, String radio) {
        if (net == 1) {
            String m = StringUtils.trimToEmpty(radio).toUpperCase();
            if ("".equals(m) || "GPRS".equals(m) || "CDMA1X".equals(m) || "EDGE".equals(m)) {
                return 1;
            } else if ("HSDPA".equals(m) || "HSUPA".equals(m) || "WCDMA".equals(m) || "EHRPD".equals(m) || m.startsWith("CDMAEVDO")) {
                return 2;
            } else if ("LTE".equals(m)) {
                return 3;
            } else {
                return 1;
            }
        } else if (net == 4) {
            return 4;
        } else {
            return 0;
        }
    }

    public static int net(int net, int mnet) {
        if (net == Net.TYPE_WIFI) {
            return 4;
        } else if (net == Net.TYPE_MOBILE) {
            return MNet.getNetworkClass(mnet);
        } else {
            return 0;
        }
    }

    public static class Net {
        public static final int TYPE_MOBILE = 0;
        public static final int TYPE_WIFI = 1;
        public static final int TYPE_MOBILE_MMS = 2;
        public static final int TYPE_MOBILE_SUPL = 3;
        public static final int TYPE_MOBILE_DUN = 4;
        public static final int TYPE_MOBILE_HIPRI = 5;
        public static final int TYPE_WIMAX = 6;
        public static final int TYPE_BLUETOOTH = 7;
        public static final int TYPE_DUMMY = 8;
        public static final int TYPE_ETHERNET = 9;
    }

    public static class MNet {
        public static final int NETWORK_TYPE_UNKNOWN = 0;
        public static final int NETWORK_TYPE_GPRS = 1;
        public static final int NETWORK_TYPE_EDGE = 2;
        public static final int NETWORK_TYPE_UMTS = 3;
        public static final int NETWORK_TYPE_CDMA = 4;
        public static final int NETWORK_TYPE_EVDO_0 = 5;
        public static final int NETWORK_TYPE_EVDO_A = 6;
        public static final int NETWORK_TYPE_1xRTT = 7;
        public static final int NETWORK_TYPE_HSDPA = 8;
        public static final int NETWORK_TYPE_HSUPA = 9;
        public static final int NETWORK_TYPE_HSPA = 10;
        public static final int NETWORK_TYPE_IDEN = 11;
        public static final int NETWORK_TYPE_EVDO_B = 12;
        public static final int NETWORK_TYPE_LTE = 13;
        public static final int NETWORK_TYPE_EHRPD = 14;
        public static final int NETWORK_TYPE_HSPAP = 15;

        public static final int NETWORK_CLASS_UNKNOWN = 0;
        public static final int NETWORK_CLASS_2_G = 1;
        public static final int NETWORK_CLASS_3_G = 2;
        public static final int NETWORK_CLASS_4_G = 3;

        public static int getNetworkClass(int networkType) {
            switch (networkType) {
            case NETWORK_TYPE_GPRS:
            case NETWORK_TYPE_EDGE:
            case NETWORK_TYPE_CDMA:
            case NETWORK_TYPE_1xRTT:
            case NETWORK_TYPE_IDEN:
                return NETWORK_CLASS_2_G;
            case NETWORK_TYPE_UMTS:
            case NETWORK_TYPE_EVDO_0:
            case NETWORK_TYPE_EVDO_A:
            case NETWORK_TYPE_HSDPA:
            case NETWORK_TYPE_HSUPA:
            case NETWORK_TYPE_HSPA:
            case NETWORK_TYPE_EVDO_B:
            case NETWORK_TYPE_EHRPD:
            case NETWORK_TYPE_HSPAP:
                return NETWORK_CLASS_3_G;
            case NETWORK_TYPE_LTE:
                return NETWORK_CLASS_4_G;
            default:
                return NETWORK_CLASS_UNKNOWN;
            }
        }
    }
}
