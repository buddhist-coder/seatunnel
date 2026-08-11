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

package org.apache.seatunnel.transform.mask.masker;

import java.util.Arrays;

/**
 * 截断位置枚举：删除字符串的前/后/居中 N 位。
 */
public enum TruncatePosition {

    /** 删除前 N 位 */
    FRONT,

    /** 删除后 N 位 */
    BACK,

    /** 删除居中 N 位，起始下标 = floor((L - N) / 2) */
    MIDDLE;

    /** 按名称解析（大小写宽容），未知位置直接抛错。 */
    public static TruncatePosition of(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("截断位置不能为空, 可选值: " + Arrays.toString(values()));
        }
        try {
            return valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "不支持的截断位置: " + name + ", 可选值: " + Arrays.toString(values()));
        }
    }
}
