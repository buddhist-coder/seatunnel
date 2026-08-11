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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.transform.encrypt.EncryptTransformFactory;
import org.apache.seatunnel.transform.encrypt.encryptor.EncryptAlgorithm;
import org.apache.seatunnel.transform.encrypt.encryptor.FieldEncryptor;

import org.bouncycastle.crypto.engines.SM4Engine;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/** Encrypt transform 的加密器行为测试。 */
public class EncryptTransformFactoryTest {

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(new EncryptTransformFactory().optionRule());
    }

    @Test
    public void testBase64() {
        FieldEncryptor encryptor = EncryptAlgorithm.BASE64.createEncryptor(null, null);
        Assertions.assertEquals("aGVsbG8=", encryptor.encrypt("hello"));
    }

    @Test
    public void testSm3StandardVector() {
        // GB/T 32905-2016 附录 A 标准向量: SM3("abc")
        FieldEncryptor encryptor = EncryptAlgorithm.SM3.createEncryptor(null, null);
        Assertions.assertEquals(
                "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
                encryptor.encrypt("abc"));
    }

    @Test
    public void testSm3Salt() {
        FieldEncryptor plain = EncryptAlgorithm.SM3.createEncryptor(null, null);
        FieldEncryptor salted = EncryptAlgorithm.SM3.createEncryptor(null, "s1");
        // 加盐后摘要不同, 且等价于对 salt+明文 求摘要
        Assertions.assertNotEquals(plain.encrypt("abc"), salted.encrypt("abc"));
        Assertions.assertEquals(plain.encrypt("s1abc"), salted.encrypt("abc"));
    }

    @Test
    public void testAesRoundTrip() throws Exception {
        String key = "0123456789abcdef";
        FieldEncryptor encryptor = EncryptAlgorithm.AES.createEncryptor(key, null);
        String ciphertext = encryptor.encrypt("13812345678");

        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(
                Cipher.DECRYPT_MODE,
                new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "AES"));
        byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(ciphertext));
        Assertions.assertEquals("13812345678", new String(decrypted, StandardCharsets.UTF_8));
    }

    @Test
    public void testDesRoundTrip() throws Exception {
        String key = "12345678";
        FieldEncryptor encryptor = EncryptAlgorithm.DES.createEncryptor(key, null);
        String ciphertext = encryptor.encrypt("张三-测试数据");

        Cipher cipher = Cipher.getInstance("DES/ECB/PKCS5Padding");
        cipher.init(
                Cipher.DECRYPT_MODE,
                new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "DES"));
        byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(ciphertext));
        Assertions.assertEquals("张三-测试数据", new String(decrypted, StandardCharsets.UTF_8));
    }

    @Test
    public void testSm4RoundTrip() throws Exception {
        String key = "fedcba9876543210";
        FieldEncryptor encryptor = EncryptAlgorithm.SM4.createEncryptor(key, null);
        String ciphertext = encryptor.encrypt("110101199001011234");

        // 用 BC 轻量 API 解密验证可还原
        PaddedBufferedBlockCipher cipher = new PaddedBufferedBlockCipher(new SM4Engine());
        cipher.init(false, new KeyParameter(key.getBytes(StandardCharsets.UTF_8)));
        byte[] input = Base64.getDecoder().decode(ciphertext);
        byte[] output = new byte[cipher.getOutputSize(input.length)];
        int length = cipher.processBytes(input, 0, input.length, output, 0);
        length += cipher.doFinal(output, length);
        Assertions.assertEquals(
                "110101199001011234",
                new String(Arrays.copyOf(output, length), StandardCharsets.UTF_8));
    }

    @Test
    public void testEncryptorReusableAcrossCalls() {
        // 同一加密器连续加密多值(复用 Cipher 实例), 结果确定且互相独立
        FieldEncryptor encryptor = EncryptAlgorithm.SM4.createEncryptor("0123456789abcdef", null);
        String first = encryptor.encrypt("aaa");
        String second = encryptor.encrypt("bbbbbbbbbbbbbbbbbbbb");
        Assertions.assertEquals(first, encryptor.encrypt("aaa"));
        Assertions.assertNotEquals(first, second);
    }

    @Test
    public void testInvalidKeyLength() {
        // AES 密钥须 16/24/32 字节
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EncryptAlgorithm.AES.createEncryptor("123", null));
        // DES 密钥须 8 字节
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EncryptAlgorithm.DES.createEncryptor("123456789", null));
        // SM4 密钥须 16 字节
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EncryptAlgorithm.SM4.createEncryptor("0123456789abcdef0", null));
        // 对称算法缺密钥
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EncryptAlgorithm.AES.createEncryptor(null, null));
        // 密钥长度按 UTF-8 字节数校验: 8 个中文字符 = 24 字节, AES 合法
        Assertions.assertNotNull(EncryptAlgorithm.AES.createEncryptor("中文密钥中文密钥", null));
    }

    @Test
    public void testUnknownAlgorithm() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> EncryptAlgorithm.of("RSA"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> EncryptAlgorithm.of(null));
        // 大小写宽容
        Assertions.assertEquals(EncryptAlgorithm.AES, EncryptAlgorithm.of("aes"));
    }
}
