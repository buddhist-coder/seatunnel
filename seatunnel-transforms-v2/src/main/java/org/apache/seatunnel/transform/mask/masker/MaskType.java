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
 * 脱敏类型枚举，与 seatunnel-share 侧 MaskType 枚举字面量保持严格一致。
 */
public enum MaskType {

    /** 截断：删除 [前/后/中间] N 位，多段链式叠加 */
    TRUNCATE,

    /** 偏移：字符循环移位（前偏移=左移，后偏移=右移） */
    OFFSET,

    /** 规整：多行条件映射，首个命中生效，未命中保留原值 */
    NORMALIZE,

    /** 替换：多条内容替换，链式应用 */
    REPLACE;

    /** 按名称解析（大小写宽容），未知类型直接抛错。 */
    public static MaskType of(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("脱敏类型不能为空, 可选值: " + Arrays.toString(values()));
        }
        try {
            return valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "不支持的脱敏类型: " + name + ", 可选值: " + Arrays.toString(values()));
        }
    }
}
