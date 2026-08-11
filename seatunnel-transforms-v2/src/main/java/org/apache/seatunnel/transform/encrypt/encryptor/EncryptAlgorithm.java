/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.encrypt.encryptor;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 加密算法枚举，负责密钥合法性校验与加密器创建。
 *
 * <p>与 seatunnel-share 侧 EncryptAlgorithm 枚举字面量保持严格一致。
 */
public enum EncryptAlgorithm {

    /** Base64 编码（非加密，仅防肉眼直读） */
    BASE64 {
        @Override
        public FieldEncryptor createEncryptor(String secretKey, String salt) {
            return new Base64Encryptor();
        }
    },

    /** AES 对称加密，AES/ECB/PKCS5Padding，密钥 16/24/32 字节 */
    AES {
        @Override
        public FieldEncryptor createEncryptor(String secretKey, String salt) {
            byte[] key = requireKeyLength(name(), secretKey, 16, 24, 32);
            return new JceSymmetricEncryptor("AES", "AES/ECB/PKCS5Padding", key);
        }
    },

    /** DES 对称加密，DES/ECB/PKCS5Padding，密钥 8 字节（仅兼容保留，不推荐） */
    DES {
        @Override
        public FieldEncryptor createEncryptor(String secretKey, String salt) {
            byte[] key = requireKeyLength(name(), secretKey, 8);
            return new JceSymmetricEncryptor("DES", "DES/ECB/PKCS5Padding", key);
        }
    },

    /** 国密 SM4 对称加密，ECB + PKCS7Padding（BouncyCastle 轻量 API），密钥 16 字节 */
    SM4 {
        @Override
        public FieldEncryptor createEncryptor(String secretKey, String salt) {
            byte[] key = requireKeyLength(name(), secretKey, 16);
            return new Sm4Encryptor(key);
        }
    },

    /** 国密 SM3 摘要（不可逆），可选盐值，输出 64 字符 hex */
    SM3 {
        @Override
        public FieldEncryptor createEncryptor(String secretKey, String salt) {
            return new Sm3DigestEncryptor(salt);
        }
    };

    /**
     * 创建该算法的加密器，构造时完成密钥校验（fail-fast）。
     *
     * @param secretKey 对称密钥（BASE64/SM3 忽略）
     * @param salt      盐值（仅 SM3 生效）
     */
    public abstract FieldEncryptor createEncryptor(String secretKey, String salt);

    /**
     * 按名称解析算法（大小写宽容），未知算法直接抛错。
     */
    public static EncryptAlgorithm of(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("加密算法不能为空, 可选值: " + Arrays.toString(values()));
        }
        try {
            return valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "不支持的加密算法: " + name + ", 可选值: " + Arrays.toString(values()));
        }
    }

    /**
     * 校验密钥的 UTF-8 字节长度必须为给定值之一，返回密钥字节。
     */
    static byte[] requireKeyLength(String algorithm, String secretKey, int... allowedLengths) {
        if (secretKey == null || secretKey.isEmpty()) {
            throw new IllegalArgumentException(algorithm + " 算法必须配置密钥(secret_key)");
        }
        byte[] key = secretKey.getBytes(StandardCharsets.UTF_8);
        for (int allowed : allowedLengths) {
            if (key.length == allowed) {
                return key;
            }
        }
        throw new IllegalArgumentException(
                algorithm + " 密钥长度非法: 当前 " + key.length + " 字节(UTF-8), 要求 "
                        + Arrays.toString(allowedLengths) + " 字节");
    }
}
