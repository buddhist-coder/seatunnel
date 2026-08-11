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

import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.TruncateRule;

import java.util.ArrayList;
import java.util.List;

/**
 * 截断脱敏器：删除 [前/后/中间] N 位，多段**链式**叠加（每段作用于上一段结果，评审确认）。
 *
 * <p>任一段删除位数 ≥ 当前串长时结果为空串，后续段短路。输出恒为 String。
 */
public class TruncateMasker implements FieldMasker {

    private static final long serialVersionUID = 1L;

    /** 解析后的段列表：position + length，有序 */
    private final List<Segment> segments;

    public TruncateMasker(List<TruncateRule> rules) {
        if (rules == null || rules.isEmpty()) {
            throw new IllegalArgumentException("TRUNCATE 脱敏至少需要一段截断规则(truncate_rules)");
        }
        this.segments = new ArrayList<>(rules.size());
        for (TruncateRule rule : rules) {
            TruncatePosition position = TruncatePosition.of(rule.getPosition());
            Integer length = rule.getLength();
            if (length == null || length <= 0) {
                throw new IllegalArgumentException("TRUNCATE 截断位数必须大于 0, 当前: " + length);
            }
            segments.add(new Segment(position, length));
        }
    }

    @Override
    public Object mask(Object value) {
        String result = String.valueOf(value);
        for (Segment segment : segments) {
            if (result.isEmpty()) {
                break;
            }
            result = segment.apply(result);
        }
        return result;
    }

    /** 单段截断动作 */
    private static class Segment implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final TruncatePosition position;
        private final int length;

        Segment(TruncatePosition position, int length) {
            this.position = position;
            this.length = length;
        }

        String apply(String input) {
            int total = input.length();
            if (length >= total) {
                return "";
            }
            switch (position) {
                case FRONT:
                    return input.substring(length);
                case BACK:
                    return input.substring(0, total - length);
                case MIDDLE:
                default:
                    // 居中删除, 起始下标向下取整
                    int start = (total - length) / 2;
                    return input.substring(0, start) + input.substring(start + length);
            }
        }
    }
}
