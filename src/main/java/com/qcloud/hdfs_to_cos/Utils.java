package com.qcloud.hdfs_to_cos;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Utils {
    /**
     * 计算输入流的校验和，支持MD5和SHA
     *
     * @param inputStream   待计算输入流
     * @param algorithm 要计算校验和的算法，支持MD5和SHA算法
     * @return  校验和的十六进制字符串
     */
    public static String calInputStreamCheckSum(InputStream inputStream, String algorithm) throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance(algorithm);
        byte[] databytes = new byte[1024];
        int nRead = 0;
        while ((nRead = inputStream.read(databytes)) != -1) {
            digest.update(databytes, 0, nRead);
        }

        byte[] md5Bytes = digest.digest();
        StringBuffer md5 = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            md5.append(Integer.toString((md5Bytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        return md5.toString();
    }
}
