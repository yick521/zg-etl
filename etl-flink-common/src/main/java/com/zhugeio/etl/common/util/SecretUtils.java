package com.zhugeio.etl.common.util;


import com.zhugeio.etl.common.config.Config;

/**
 * @author IDEA
 * @date 2022/1/13
 */
public class SecretUtils {

    private static final String iv = "63N9KEOvtDtrXbTl";

    public static String decryptionPass(String content) {
        String encryption_work_secret = Config.readResource(Config.getProp(Config.WORKER_SECRET_PATH));
        String encryption_encryption_secret = Config.readResource(Config.getProp(Config.ENCRYPTION_SECRET_PATH));
        String root_secret = Config.readResource(Config.getProp(Config.ROOT_SECRET_PATH));
        String encryption_secret = AESUtil.decrypt(root_secret, iv, encryption_encryption_secret);
        String work_secret = AESUtil.decrypt(encryption_secret, iv, encryption_work_secret);
        return AESUtil.decrypt(work_secret, iv, content);
    }

}
