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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Encrypt transform 的配置定义。
 *
 * <p>配置示例：
 *
 * <pre>{@code
 * Encrypt {
 *   plugin_input  = "..."
 *   plugin_output = "..."
 *   encrypt_columns = [
 *     { field = "phone",    algorithm = "AES", secret_key = "0123456789abcdef" },
 *     { field = "password", algorithm = "SM3", salt = "idss" }
 *   ]
 * }
 * }</pre>
 */
public class EncryptTransformConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Option<List<EncryptColumn>> ENCRYPT_COLUMNS =
            Options.key("encrypt_columns")
                    .listType(EncryptColumn.class)
                    .noDefaultValue()
                    .withDescription(
                            "The columns to encrypt, each with field/algorithm/secret_key/salt");

    /** 单列加密配置 */
    @Data
    public static class EncryptColumn implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 字段名（须存在于输入 schema） */
        @JsonAlias("field")
        private String field;

        /** 加密算法: BASE64/AES/DES/SM4/SM3 */
        @JsonAlias("algorithm")
        private String algorithm;

        /** 对称密钥（AES/DES/SM4 必填） */
        @JsonAlias("secret_key")
        private String secretKey;

        /** 盐值（仅 SM3 生效） */
        @JsonAlias("salt")
        private String salt;
    }
}
