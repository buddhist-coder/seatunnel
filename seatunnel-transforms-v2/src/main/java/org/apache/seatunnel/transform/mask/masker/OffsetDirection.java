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
 * 偏移方向枚举：字符循环移位方向。
 */
public enum OffsetDirection {

    /** 前偏移：整体左移，前 N 个字符转到尾部 */
    FORWARD,

    /** 后偏移：整体右移，后 N 个字符转到头部 */
    BACKWARD;

    /** 按名称解析（大小写宽容），未知方向直接抛错。 */
    public static OffsetDirection of(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("偏移方向不能为空, 可选值: " + Arrays.toString(values()));
        }
        try {
            return valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "不支持的偏移方向: " + name + ", 可选值: " + Arrays.toString(values()));
        }
    }
}
