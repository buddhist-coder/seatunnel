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

import java.io.Serializable;

/**
 * 字段加密器策略接口。
 *
 * <p>实现类必须可序列化（Transform 会被序列化分发到集群节点），
 * 内部持有的 Cipher/Digest 等不可序列化对象须声明为 transient 并惰性初始化。
 */
public interface FieldEncryptor extends Serializable {

    /**
     * 加密单个字段值。
     *
     * @param plaintext 明文（调用方保证非 null）
     * @return 密文文本（Base64 或 hex）
     */
    String encrypt(String plaintext);
}
