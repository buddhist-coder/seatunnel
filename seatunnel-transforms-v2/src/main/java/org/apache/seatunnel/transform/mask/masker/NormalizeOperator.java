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
 * 规整条件运算符枚举：对 compareTo 结果的谓词。
 */
public enum NormalizeOperator {

    /** 大于 */
    GT {
        @Override
        public boolean test(int compareResult) {
            return compareResult > 0;
        }
    },

    /** 大于等于 */
    GE {
        @Override
        public boolean test(int compareResult) {
            return compareResult >= 0;
        }
    },

    /** 等于 */
    EQ {
        @Override
        public boolean test(int compareResult) {
            return compareResult == 0;
        }
    },

    /** 小于 */
    LT {
        @Override
        public boolean test(int compareResult) {
            return compareResult < 0;
        }
    },

    /** 小于等于 */
    LE {
        @Override
        public boolean test(int compareResult) {
            return compareResult <= 0;
        }
    };

    /**
     * 判定条件是否命中。
     *
     * @param compareResult 字段值.compareTo(比较值) 的结果（BigDecimal 精确比较）
     */
    public abstract boolean test(int compareResult);

    /** 按名称解析（大小写宽容），未知运算符直接抛错。 */
    public static NormalizeOperator of(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("规整运算符不能为空, 可选值: " + Arrays.toString(values()));
        }
        try {
            return valueOf(name.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "不支持的规整运算符: " + name + ", 可选值: " + Arrays.toString(values()));
        }
    }
}
