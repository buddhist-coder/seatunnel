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

import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * 国密 SM4 对称加密器，ECB 模式 + PKCS7 填充，密文输出 Base64。
 *
 * <p>刻意使用 BouncyCastle 轻量 API（org.bouncycastle.crypto.*）而非 JCE Provider 注册方式：
 * transforms-v2 的 shade 打包会剥除 BC jar 的签名文件（META-INF/*.SF），
 * JCE 对 provider 的验签会因此失败（JceSecurityException），轻量 API 完全绕开该机制。
 */
public class Sm4Encryptor implements FieldEncryptor {

    private static final long serialVersionUID = 1L;

    private final byte[] keyBytes;

    /** PaddedBufferedBlockCipher 非线程安全且不可序列化: transient 惰性初始化 + synchronized 使用 */
    private transient PaddedBufferedBlockCipher cipher;

    public Sm4Encryptor(byte[] keyBytes) {
        this.keyBytes = keyBytes;
    }

    @Override
    public synchronized String encrypt(String plaintext) {
        byte[] input = plaintext.getBytes(StandardCharsets.UTF_8);
        PaddedBufferedBlockCipher blockCipher = cipher();
        // 复用实例, 每次加密前重置内部状态
        blockCipher.reset();
        byte[] output = new byte[blockCipher.getOutputSize(input.length)];
        int length = blockCipher.processBytes(input, 0, input.length, output, 0);
        try {
            length += blockCipher.doFinal(output, length);
        } catch (InvalidCipherTextException e) {
            // 加密方向不会出现填充错误, 此处仅为满足受检异常
            throw new IllegalStateException("SM4 加密失败: " + e.getMessage(), e);
        }
        byte[] ciphertext = length == output.length ? output : Arrays.copyOf(output, length);
        return Base64.getEncoder().encodeToString(ciphertext);
    }

    private PaddedBufferedBlockCipher cipher() {
        if (cipher == null) {
            // 默认 PKCS7Padding, 对 16 字节分组与 PKCS5 等价
            PaddedBufferedBlockCipher created = new PaddedBufferedBlockCipher(new SM4Engine());
            created.init(true, new KeyParameter(keyBytes));
            cipher = created;
        }
        return cipher;
    }
}
