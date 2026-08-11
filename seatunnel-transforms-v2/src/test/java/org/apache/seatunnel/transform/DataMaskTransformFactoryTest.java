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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.MaskColumn;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.NormalizeRule;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.ReplaceRule;
import org.apache.seatunnel.transform.mask.DataMaskTransformConfig.TruncateRule;
import org.apache.seatunnel.transform.mask.DataMaskTransformFactory;
import org.apache.seatunnel.transform.mask.masker.NormalizeMasker;
import org.apache.seatunnel.transform.mask.masker.OffsetMasker;
import org.apache.seatunnel.transform.mask.masker.ReplaceMasker;
import org.apache.seatunnel.transform.mask.masker.TruncateMasker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** DataMask transform 的脱敏器行为测试。 */
public class DataMaskTransformFactoryTest {

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull(new DataMaskTransformFactory().optionRule());
    }

    @Test
    public void testNestedConfigDeserialization() {
        // 验证 ReadonlyConfig 对 mask_columns 嵌套 List<POJO> 结构的反序列化
        Map<String, Object> truncateRule = new HashMap<>();
        truncateRule.put("position", "FRONT");
        truncateRule.put("length", 3);
        Map<String, Object> truncateColumn = new HashMap<>();
        truncateColumn.put("field", "phone");
        truncateColumn.put("mask_type", "TRUNCATE");
        truncateColumn.put("truncate_rules", Collections.singletonList(truncateRule));

        Map<String, Object> normalizeRule = new HashMap<>();
        normalizeRule.put("operator", "LT");
        normalizeRule.put("compare", "30");
        normalizeRule.put("target", "20");
        Map<String, Object> normalizeColumn = new HashMap<>();
        normalizeColumn.put("field", "age");
        normalizeColumn.put("mask_type", "NORMALIZE");
        normalizeColumn.put("normalize_rules", Collections.singletonList(normalizeRule));

        Map<String, Object> root = new HashMap<>();
        root.put("mask_columns", Arrays.asList(truncateColumn, normalizeColumn));

        List<MaskColumn> columns =
                ReadonlyConfig.fromMap(root).get(DataMaskTransformConfig.MASK_COLUMNS);
        Assertions.assertEquals(2, columns.size());
        Assertions.assertEquals("phone", columns.get(0).getField());
        Assertions.assertEquals("TRUNCATE", columns.get(0).getMaskType());
        Assertions.assertEquals(1, columns.get(0).getTruncateRules().size());
        Assertions.assertEquals("FRONT", columns.get(0).getTruncateRules().get(0).getPosition());
        Assertions.assertEquals(3, columns.get(0).getTruncateRules().get(0).getLength());
        Assertions.assertEquals("30", columns.get(1).getNormalizeRules().get(0).getCompare());
        Assertions.assertEquals("20", columns.get(1).getNormalizeRules().get(0).getTarget());
    }

    // ---------------- 截断 ----------------

    @Test
    public void testTruncateFront() {
        TruncateMasker masker = new TruncateMasker(truncateRules("FRONT", 3));
        Assertions.assertEquals("12345678", masker.mask("13812345678"));
    }

    @Test
    public void testTruncateBack() {
        TruncateMasker masker = new TruncateMasker(truncateRules("BACK", 4));
        Assertions.assertEquals("1381234", masker.mask("13812345678"));
    }

    @Test
    public void testTruncateMiddle() {
        // L=11, N=3, 起始下标 floor((11-3)/2)=4, 删下标 4..6
        TruncateMasker masker = new TruncateMasker(truncateRules("MIDDLE", 3));
        Assertions.assertEquals("13815678", masker.mask("13812345678"));
    }

    @Test
    public void testTruncateChained() {
        // 链式: 前删 2 得 812345678(L=9), 再居中删 3(起始 3, 删下标 3..5) 得 812678
        TruncateRule front = truncateRule("FRONT", 2);
        TruncateRule middle = truncateRule("MIDDLE", 3);
        TruncateMasker masker = new TruncateMasker(Arrays.asList(front, middle));
        Assertions.assertEquals("812678", masker.mask("13812345678"));
    }

    @Test
    public void testTruncateExceedsLength() {
        // 删除位数 >= 串长 -> 空串, 后续段短路
        TruncateMasker masker =
                new TruncateMasker(Arrays.asList(truncateRule("FRONT", 10), truncateRule("BACK", 1)));
        Assertions.assertEquals("", masker.mask("abc"));
    }

    @Test
    public void testTruncateInvalidDetail() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new TruncateMasker(Collections.emptyList()));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new TruncateMasker(null));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new TruncateMasker(truncateRules("FRONT", 0)));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new TruncateMasker(truncateRules("SIDE", 1)));
    }

    // ---------------- 偏移 ----------------

    @Test
    public void testOffsetForward() {
        // 前偏移(左移) 2 位: 前 2 字符转到尾部
        OffsetMasker masker = new OffsetMasker("FORWARD", 2);
        Assertions.assertEquals("345612", masker.mask("123456"));
    }

    @Test
    public void testOffsetBackward() {
        // 后偏移(右移) 2 位: 后 2 字符转到头部
        OffsetMasker masker = new OffsetMasker("BACKWARD", 2);
        Assertions.assertEquals("561234", masker.mask("123456"));
    }

    @Test
    public void testOffsetModulo() {
        // 位数按串长取模: 8 % 6 = 2, 等价于左移 2
        Assertions.assertEquals("345612", new OffsetMasker("FORWARD", 8).mask("123456"));
        // 取模后为 0 -> 原样
        Assertions.assertEquals("123456", new OffsetMasker("FORWARD", 6).mask("123456"));
    }

    @Test
    public void testOffsetShortString() {
        // 串长 <= 1 原样返回
        Assertions.assertEquals("a", new OffsetMasker("FORWARD", 3).mask("a"));
        Assertions.assertEquals("", new OffsetMasker("BACKWARD", 3).mask(""));
    }

    @Test
    public void testOffsetInvalidDetail() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new OffsetMasker("FORWARD", 0));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new OffsetMasker("UP", 1));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new OffsetMasker(null, 1));
    }

    // ---------------- 规整 ----------------

    @Test
    public void testNormalizeKeepOriginalType() {
        // target 全部可解析为 INT -> 保持原类型, 首个命中生效
        NormalizeMasker masker =
                new NormalizeMasker(
                        Arrays.asList(
                                normalizeRule("LT", "30", "20"), normalizeRule("GE", "30", "40")),
                        BasicType.INT_TYPE);
        Assertions.assertTrue(masker.isKeepOriginalType());
        Assertions.assertEquals(20, masker.mask(23));
        Assertions.assertEquals(40, masker.mask(35));
        Assertions.assertEquals(40, masker.mask(30));
    }

    @Test
    public void testNormalizeMissKeepsOriginalValue() {
        // 未命中任何条件行 -> 保留原值(评审确认)
        NormalizeMasker masker =
                new NormalizeMasker(
                        Collections.singletonList(normalizeRule("GT", "100", "100")),
                        BasicType.INT_TYPE);
        Assertions.assertEquals(50, masker.mask(50));
    }

    @Test
    public void testNormalizeStringOutput() {
        // target 含非数值 -> 输出 STRING, 未命中的原值也转字符串
        NormalizeMasker masker =
                new NormalizeMasker(
                        Collections.singletonList(normalizeRule("EQ", "1", "one")),
                        BasicType.INT_TYPE);
        Assertions.assertFalse(masker.isKeepOriginalType());
        Assertions.assertEquals("one", masker.mask(1));
        Assertions.assertEquals("2", masker.mask(2));
    }

    @Test
    public void testNormalizeDecimalCompare() {
        // BigDecimal 精确比较: EQ 用 compareTo, 1 与 1.0 判等
        NormalizeMasker masker =
                new NormalizeMasker(
                        Collections.singletonList(normalizeRule("EQ", "1.0", "9.9")),
                        BasicType.DOUBLE_TYPE);
        Assertions.assertEquals(9.9d, masker.mask(1.0d));
    }

    @Test
    public void testNormalizeInvalidDetail() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new NormalizeMasker(Collections.emptyList(), BasicType.INT_TYPE));
        // compare 非数值
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NormalizeMasker(
                                Collections.singletonList(normalizeRule("GT", "abc", "1")),
                                BasicType.INT_TYPE));
        // 运算符非法
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NormalizeMasker(
                                Collections.singletonList(normalizeRule("NE", "1", "1")),
                                BasicType.INT_TYPE));
    }

    // ---------------- 替换 ----------------

    @Test
    public void testReplaceAllOccurrences() {
        // 替换所有出现
        ReplaceMasker masker =
                new ReplaceMasker(Collections.singletonList(replaceRule("8", "*")));
        Assertions.assertEquals("13*1234567*", masker.mask("13812345678"));
    }

    @Test
    public void testReplaceChained() {
        // 多条链式: 后一条作用于前一条的结果
        ReplaceMasker masker =
                new ReplaceMasker(
                        Arrays.asList(replaceRule("@qq.com", "@***.com"), replaceRule("user", "u")));
        Assertions.assertEquals("u@***.com", masker.mask("user@qq.com"));
    }

    @Test
    public void testReplaceWithEmptyReplacement() {
        // replacement 为空 = 删除
        ReplaceMasker masker =
                new ReplaceMasker(Collections.singletonList(replaceRule("敏感", null)));
        Assertions.assertEquals("内容", masker.mask("敏感内容"));
    }

    @Test
    public void testReplaceInvalidDetail() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new ReplaceMasker(Collections.emptyList()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new ReplaceMasker(Collections.singletonList(replaceRule("", "*"))));
    }

    // ---------------- 构造辅助 ----------------

    private static List<TruncateRule> truncateRules(String position, int length) {
        return Collections.singletonList(truncateRule(position, length));
    }

    private static TruncateRule truncateRule(String position, int length) {
        TruncateRule rule = new TruncateRule();
        rule.setPosition(position);
        rule.setLength(length);
        return rule;
    }

    private static NormalizeRule normalizeRule(String operator, String compare, String target) {
        NormalizeRule rule = new NormalizeRule();
        rule.setOperator(operator);
        rule.setCompare(compare);
        rule.setTarget(target);
        return rule;
    }

    private static ReplaceRule replaceRule(String search, String replacement) {
        ReplaceRule rule = new ReplaceRule();
        rule.setSearch(search);
        rule.setReplacement(replacement);
        return rule;
    }
}
