package com.zhugeio.etl.common.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Base64;
import java.util.Locale;
import java.util.Random;

/**
 * AES工具类
 */
public abstract class AESUtil {
    private static final Random RANDOM = new Random();
    private static final char[] RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890".toCharArray();

    private static final String AES = "AES";
    // 加密解密算法/加密模式/填充方式
    private static final String CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    static {
        Security.setProperty("crypto.policy", "unlimited");
    }

    /**
     * AES加密
     *
     * @param secretKey 密钥，长度：128bit、192bit、256bit
     * @param iv        初始向量，128bit
     * @param content   加密内容
     */
    public static String encrypt(String secretKey, String iv, String content) {
        try {
            if (secretKey == null || iv == null || content == null) {
                return null;
            }
            SecretKey sk = new SecretKeySpec(secretKey.getBytes(), AES);
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, sk, new IvParameterSpec(iv.getBytes(StandardCharsets.UTF_8)));

            // 获取加密内容的字节数组(这里要设置为utf-8)不然内容中如果有中文和英文混合中文就会解密为乱码
            byte[] byteEncode = content.getBytes(StandardCharsets.UTF_8);
            // 根据密码器的初始化方式加密
            byte[] byteAes = cipher.doFinal(byteEncode);
            // 将加密后的数据转换为字符串
            return BASE64_ENCODER.encodeToString(byteAes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * AES解密
     *
     * @param secretKey 密钥，长度：128bit、192bit、256bit，与加密时保持一致
     * @param iv        初始向量，128bit，与加密时保持一致
     * @param content   解密内容
     */
    public static String decrypt(String secretKey, String iv, String content) {
        try {
            if (secretKey == null || iv == null || content == null) {
                return null;
            }
            final SecretKey sk = new SecretKeySpec(secretKey.getBytes(), AES);
            final Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, sk, new IvParameterSpec(iv.getBytes(StandardCharsets.UTF_8)));
            // 将加密并编码后的内容解码成字节数组
            final byte[] byteContent = BASE64_DECODER.decode(content);
            // 解密
            final byte[] byteDecode = cipher.doFinal(byteContent);
            return new String(byteDecode, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 随机iv
     */
    public static String randomIV() {
        return randomKey(16);
    }

    /**
     * 128密钥
     */
    public static String randomSecureKey128() {
        return randomSecureKey(AESLevel.AES128);
    }

    /**
     * 192密钥
     */
    public static String randomSecureKey192() {
        return randomSecureKey(AESLevel.AES192);
    }

    /**
     * 256密钥
     */
    public static String randomSecureKey256() {
        return randomSecureKey(AESLevel.AES256);
    }

    public static String randomSecureKey(AESLevel aesLevel) {
        return randomKey(aesLevel.charNum);
    }

    private static String randomKey(int num) {
        final StringBuilder keyBuilder = new StringBuilder(32);
        for (int i = 0; i < num; i++) {
            keyBuilder.append(RANDOM_CHARS[randomNum(0, RANDOM_CHARS.length - 1)]);
        }
        return keyBuilder.toString();
    }

    /**
     * 随机范围值
     *
     * @param min 最小值
     * @param max 最大值
     */
    private static int randomNum(int min, int max) {
        return RANDOM.nextInt(max) % (max - min + 1) + min;
    }

    public static void main(String[] args) {
        String key = randomSecureKey256();
        String iv = randomIV();
        System.out.printf("key: %s, iv: %s%n", key, iv);

        String str = "123456";
        String enc = AESUtil.encrypt(key, iv, str);
        System.out.println("encrypt: " + enc);

        String dec = AESUtil.decrypt(key, iv, enc);
        System.out.println("decrypt:" + dec);
        System.out.println(key.getBytes(StandardCharsets.UTF_8).length);
        System.out.println(iv.getBytes(StandardCharsets.UTF_8).length);
    }

    public enum AESLevel {
        AES128(16),
        AES192(24),
        AES256(32);

        private final int charNum;

        AESLevel(int charNum) {
            this.charNum = charNum;
        }

        public static AESLevel valueOf2(String levelName) {
            if (levelName == null) {
                return null;
            }
            levelName = levelName.toLowerCase(Locale.ROOT);
            for (AESLevel l : AESLevel.values()) {
                if (l.name().toLowerCase(Locale.ROOT).contains(levelName)) {
                    return l;
                }
            }
            return null;
        }
    }
}