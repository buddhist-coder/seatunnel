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

/**
 * 偏移脱敏器：字符循环移位（评审确认语义）。
 *
 * <p>前偏移 = 整体左移 N 位（前 N 字符转到尾部）；后偏移 = 整体右移 N 位。
 * 位数按串长取模；串长 ≤ 1 或取模后为 0 时原样返回。输出恒为 String。
 */
public class OffsetMasker implements FieldMasker {

    private static final long serialVersionUID = 1L;

    private final OffsetDirection direction;
    private final int offsetLength;

    public OffsetMasker(String direction, Integer offsetLength) {
        this.direction = OffsetDirection.of(direction);
        if (offsetLength == null || offsetLength <= 0) {
            throw new IllegalArgumentException("OFFSET 偏移位数必须大于 0, 当前: " + offsetLength);
        }
        this.offsetLength = offsetLength;
    }

    @Override
    public Object mask(Object value) {
        String input = String.valueOf(value);
        int total = input.length();
        if (total <= 1) {
            return input;
        }
        int shift = offsetLength % total;
        if (shift == 0) {
            return input;
        }
        if (direction == OffsetDirection.FORWARD) {
            // 左移: 前 shift 位转到尾部
            return input.substring(shift) + input.substring(0, shift);
        }
        // 右移: 后 shift 位转到头部
        return input.substring(total - shift) + input.substring(0, total - shift);
    }
}
