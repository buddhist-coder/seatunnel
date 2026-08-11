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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.NormalizeRule;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * 规整脱敏器：多行条件映射，按配置顺序首个命中生效；未命中保留原值（评审确认）。
 *
 * <p>比较统一用 BigDecimal 精确执行（EQ 用 compareTo 而非 equals，1 与 1.0 判等）。
 *
 * <p>输出类型判定（构造时静态完成）：所有行的 target 都能解析为原列类型 → 保持原列类型
 * （target 预转换缓存）；任一不能 → 输出 String（未命中的原值也转 String，保证列类型一致）。
 */
public class NormalizeMasker implements FieldMasker {

    private static final long serialVersionUID = 1L;

    /** 解析后的行：运算符 + 比较值 + 预转换的目标值 */
    private final List<Entry> entries;

    /** 是否保持原列类型输出 */
    private final boolean keepOriginalType;

    public NormalizeMasker(List<NormalizeRule> rules, SeaTunnelDataType<?> originalType) {
        if (rules == null || rules.isEmpty()) {
            throw new IllegalArgumentException("NORMALIZE 脱敏至少需要一行规整规则(normalize_rules)");
        }
        // 先判定输出类型: 所有 target 均可解析为原列类型才保持原类型
        boolean keep = true;
        for (NormalizeRule rule : rules) {
            if (rule.getTarget() == null
                    || convertOrNull(rule.getTarget(), originalType) == null) {
                keep = false;
                break;
            }
        }
        this.keepOriginalType = keep;

        this.entries = new ArrayList<>(rules.size());
        for (NormalizeRule rule : rules) {
            NormalizeOperator operator = NormalizeOperator.of(rule.getOperator());
            BigDecimal compare = parseCompare(rule.getCompare());
            String target = rule.getTarget();
            if (target == null) {
                throw new IllegalArgumentException("NORMALIZE 目标值(target)不能为空");
            }
            Object outputValue = keep ? convertOrNull(target, originalType) : target;
            entries.add(new Entry(operator, compare, outputValue));
        }
    }

    /** 输出是否保持原列类型（false 时输出列须覆写为 STRING） */
    public boolean isKeepOriginalType() {
        return keepOriginalType;
    }

    @Override
    public Object mask(Object value) {
        BigDecimal actual = new BigDecimal(String.valueOf(value));
        for (Entry entry : entries) {
            if (entry.operator.test(actual.compareTo(entry.compare))) {
                return entry.outputValue;
            }
        }
        // 未命中保留原值; 输出列为 String 时须转字符串保证与列类型一致
        return keepOriginalType ? value : String.valueOf(value);
    }

    private static BigDecimal parseCompare(String compare) {
        if (compare == null || compare.trim().isEmpty()) {
            throw new IllegalArgumentException("NORMALIZE 比较值(compare)不能为空");
        }
        try {
            return new BigDecimal(compare.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("NORMALIZE 比较值必须是数值, 当前: " + compare);
        }
    }

    /** 将 target 文本转换为原列类型的值，不能转换返回 null */
    private static Object convertOrNull(String target, SeaTunnelDataType<?> type) {
        try {
            String trimmed = target.trim();
            switch (type.getSqlType()) {
                case TINYINT:
                    return Byte.valueOf(trimmed);
                case SMALLINT:
                    return Short.valueOf(trimmed);
                case INT:
                    return Integer.valueOf(trimmed);
                case BIGINT:
                    return Long.valueOf(trimmed);
                case FLOAT:
                    return Float.valueOf(trimmed);
                case DOUBLE:
                    return Double.valueOf(trimmed);
                case DECIMAL:
                    return new BigDecimal(trimmed);
                default:
                    return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 单行规整规则（目标值已按输出类型预转换） */
    private static class Entry implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final NormalizeOperator operator;
        private final BigDecimal compare;
        private final Object outputValue;

        Entry(NormalizeOperator operator, BigDecimal compare, Object outputValue) {
            this.operator = operator;
            this.compare = compare;
            this.outputValue = outputValue;
        }
    }
}
