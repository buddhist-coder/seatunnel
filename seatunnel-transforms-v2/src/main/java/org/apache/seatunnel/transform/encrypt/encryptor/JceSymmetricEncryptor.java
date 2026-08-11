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

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

/**
 * JDK 内置 JCE 的对称加密器，供 AES/DES 共用。
 *
 * <p>密文输出为 Base64 文本。Cipher 不可序列化且非线程安全：
 * 声明 transient 惰性初始化，encrypt 全程 synchronized。
 */
public class JceSymmetricEncryptor implements FieldEncryptor {

    private static final long serialVersionUID = 1L;

    /** JCE 密钥算法名，如 AES/DES */
    private final String algorithm;
    /** JCE 转换名，如 AES/ECB/PKCS5Padding */
    private final String transformation;
    private final byte[] keyBytes;

    private transient Cipher cipher;

    public JceSymmetricEncryptor(String algorithm, String transformation, byte[] keyBytes) {
        this.algorithm = algorithm;
        this.transformation = transformation;
        this.keyBytes = keyBytes;
        // 构造时立即初始化一次, 让密钥/环境问题(如 JDK8u161 前的 AES-256 策略限制)在提交阶段暴露
        try {
            initCipher();
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException(buildInitErrorMessage(e), e);
        }
    }

    @Override
    public synchronized String encrypt(String plaintext) {
        try {
            if (cipher == null) {
                // 反序列化后 transient 字段为 null, 惰性重建
                initCipher();
            }
            byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(algorithm + " 加密失败: " + e.getMessage(), e);
        }
    }

    private void initCipher() throws GeneralSecurityException {
        Cipher created = Cipher.getInstance(transformation);
        created.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(keyBytes, algorithm));
        this.cipher = created;
    }

    private String buildInitErrorMessage(GeneralSecurityException e) {
        String message = algorithm + " 加密器初始化失败: " + e.getMessage();
        if ("AES".equals(algorithm) && keyBytes.length > 16) {
            message += " (AES-192/256 需要 JDK 8u161 及以上, 或安装 JCE 无限制策略文件)";
        }
        return message;
    }
}
