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

import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.util.encoders.Hex;

import java.nio.charset.StandardCharsets;

/**
 * 国密 SM3 摘要器（不可逆），输出 64 字符小写 hex。
 *
 * <p>使用 BouncyCastle 轻量 API（原因见 {@link Sm4Encryptor} 类注释）。
 * 可选盐值：摘要前拼接在明文之前（salt + plaintext），用于抵御低熵数据的彩虹表还原。
 */
public class Sm3DigestEncryptor implements FieldEncryptor {

    private static final long serialVersionUID = 1L;

    private final String salt;

    public Sm3DigestEncryptor(String salt) {
        this.salt = salt;
    }

    @Override
    public String encrypt(String plaintext) {
        String input = (salt == null || salt.isEmpty()) ? plaintext : salt + plaintext;
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        // SM3Digest 轻量无状态依赖, 每次新建, 天然线程安全
        SM3Digest digest = new SM3Digest();
        digest.update(bytes, 0, bytes.length);
        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);
        return Hex.toHexString(hash);
    }
}
