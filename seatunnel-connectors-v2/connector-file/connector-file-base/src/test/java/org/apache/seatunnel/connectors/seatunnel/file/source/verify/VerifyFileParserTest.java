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

package org.apache.seatunnel.connectors.seatunnel.file.source.verify;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** 校验文件解析器单元测试。 */
public class VerifyFileParserTest {

    private static final String DELIMITER = "|";
    private static final String VERIFY_FILE = "/data/incoming/order/order_20260902.success";

    /** 需求给定的真实样例：包名 | 条数 | MD5 | 额外字段... */
    @Test
    void testParseRealSample() {
        String content =
                "order_20260902.tar.gz|100000|9E107D9D372BB6826BD81D3542A419D6|20260902|batch_a";
        List<VerifyFileMeta> metas = parse(content, 0, 1, 2);

        Assertions.assertEquals(1, metas.size());
        VerifyFileMeta meta = metas.get(0);
        Assertions.assertEquals("order_20260902.tar.gz", meta.getPackageName());
        Assertions.assertEquals(100000L, meta.getDeclaredCount());
        Assertions.assertTrue(meta.hasDeclaredCount());
        // MD5 统一转小写便于比对
        Assertions.assertEquals("9e107d9d372bb6826bd81d3542a419d6", meta.getExpectedMd5());
        Assertions.assertTrue(meta.hasExpectedMd5());
        // 末尾追加的额外字段保留在原始字段里，不影响既有下标解析
        Assertions.assertEquals(5, meta.getRawFields().length);
        Assertions.assertEquals("batch_a", meta.getRawFields()[4]);
        Assertions.assertEquals(VERIFY_FILE, meta.getVerifyFilePath());
    }

    /** 多行内容：每一非空行产出一条记录，空行与首尾空白被忽略。 */
    @Test
    void testParseMultipleLines() {
        String content =
                "a.tar.gz|10|md5a\r\n"
                        + "\n"
                        + "  b.tar.gz|20|md5b  \n"
                        + "c.tar.gz|30|md5c\n"
                        + "   \n";
        List<VerifyFileMeta> metas = parse(content, 0, 1, 2);

        Assertions.assertEquals(3, metas.size());
        Assertions.assertEquals("a.tar.gz", metas.get(0).getPackageName());
        Assertions.assertEquals(20L, metas.get(1).getDeclaredCount());
        Assertions.assertEquals("md5c", metas.get(2).getExpectedMd5());
    }

    /** 分隔符是正则元字符时也必须按字面量切分。 */
    @Test
    void testDelimiterIsRegexMetaCharacter() {
        List<VerifyFileMeta> metas =
                VerifyFileParser.parse("a.tar.gz.10.md5a", ".", 0, 1, 2, VERIFY_FILE);
        Assertions.assertEquals(1, metas.size());
        // "." 被当作字面分隔符：a / tar / gz / 10 / md5a
        Assertions.assertEquals("a", metas.get(0).getPackageName());
        Assertions.assertEquals(VerifyFileMeta.COUNT_ABSENT, metas.get(0).getDeclaredCount());
    }

    /** 条数字段非法（非数字、负数）时降级为缺失，不抛异常。 */
    @Test
    void testInvalidDeclaredCountFallsBackToAbsent() {
        List<VerifyFileMeta> notNumber = parse("a.tar.gz|abc|md5a", 0, 1, 2);
        Assertions.assertEquals(VerifyFileMeta.COUNT_ABSENT, notNumber.get(0).getDeclaredCount());
        Assertions.assertFalse(notNumber.get(0).hasDeclaredCount());

        List<VerifyFileMeta> negative = parse("a.tar.gz|-5|md5a", 0, 1, 2);
        Assertions.assertEquals(VerifyFileMeta.COUNT_ABSENT, negative.get(0).getDeclaredCount());

        List<VerifyFileMeta> blank = parse("a.tar.gz||md5a", 0, 1, 2);
        Assertions.assertEquals(VerifyFileMeta.COUNT_ABSENT, blank.get(0).getDeclaredCount());
    }

    /** 声明 0 条是合法值，必须与「缺失」区分开。 */
    @Test
    void testZeroDeclaredCountIsValid() {
        List<VerifyFileMeta> metas = parse("a.tar.gz|0|md5a", 0, 1, 2);
        Assertions.assertEquals(0L, metas.get(0).getDeclaredCount());
        Assertions.assertTrue(metas.get(0).hasDeclaredCount());
    }

    /** 下标配 -1 表示不取该字段。 */
    @Test
    void testIndexAbsent() {
        List<VerifyFileMeta> metas =
                parse("a.tar.gz|100|md5a", 0, VerifyFileParser.INDEX_ABSENT, -1);
        Assertions.assertEquals(VerifyFileMeta.COUNT_ABSENT, metas.get(0).getDeclaredCount());
        Assertions.assertNull(metas.get(0).getExpectedMd5());
        Assertions.assertFalse(metas.get(0).hasExpectedMd5());
    }

    /** 字段数不足导致下标越界时按缺失处理，不能抛异常。 */
    @Test
    void testIndexOutOfBounds() {
        List<VerifyFileMeta> metas = parse("a.tar.gz", 0, 1, 2);
        Assertions.assertEquals(1, metas.size());
        Assertions.assertEquals("a.tar.gz", metas.get(0).getPackageName());
        Assertions.assertEquals(VerifyFileMeta.COUNT_ABSENT, metas.get(0).getDeclaredCount());
        Assertions.assertNull(metas.get(0).getExpectedMd5());
    }

    /** 包名字段为空或空白的行直接跳过，因为无法与数据文件配对。 */
    @Test
    void testBlankPackageNameLineIsSkipped() {
        List<VerifyFileMeta> metas = parse("|100|md5a\nb.tar.gz|200|md5b\n   |300|md5c", 0, 1, 2);
        Assertions.assertEquals(1, metas.size());
        Assertions.assertEquals("b.tar.gz", metas.get(0).getPackageName());
    }

    /** 上游把包名写成全路径时，统一归一为文件名，保证能与扫描到的数据文件配对。 */
    @Test
    void testPackageNameWithDirectoryIsNormalized() {
        List<VerifyFileMeta> unixPath = parse("/data/incoming/a.tar.gz|100|md5a", 0, 1, 2);
        Assertions.assertEquals("a.tar.gz", unixPath.get(0).getPackageName());

        List<VerifyFileMeta> windowsPath = parse("D:\\data\\a.tar.gz|100|md5a", 0, 1, 2);
        Assertions.assertEquals("a.tar.gz", windowsPath.get(0).getPackageName());
    }

    /** 空内容返回空列表而不是抛异常。 */
    @Test
    void testEmptyContent() {
        Assertions.assertTrue(parse("", 0, 1, 2).isEmpty());
        Assertions.assertTrue(parse("   \n  \n", 0, 1, 2).isEmpty());
        Assertions.assertTrue(parse(null, 0, 1, 2).isEmpty());
    }

    /** 包名下标为负数属于配置错误，必须显式失败而不是静默降级。 */
    @Test
    void testNegativeNameIndexIsRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> parse("a.tar.gz|100|md5a", -1, 1, 2));
    }

    /** 字段顺序可以任意配置，不必是 名称/条数/MD5 的固定排列。 */
    @Test
    void testCustomFieldOrder() {
        List<VerifyFileMeta> metas = parse("100|md5a|a.tar.gz", 2, 0, 1);
        Assertions.assertEquals("a.tar.gz", metas.get(0).getPackageName());
        Assertions.assertEquals(100L, metas.get(0).getDeclaredCount());
        Assertions.assertEquals("md5a", metas.get(0).getExpectedMd5());
    }

    @Test
    void testExtractFileName() {
        Assertions.assertEquals("a.tar.gz", VerifyFileParser.extractFileName("a.tar.gz"));
        Assertions.assertEquals("a.tar.gz", VerifyFileParser.extractFileName("/tmp/dt=1/a.tar.gz"));
        Assertions.assertEquals(
                "a.tar.gz", VerifyFileParser.extractFileName("sftp://host:22/tmp/a.tar.gz"));
        Assertions.assertEquals("a.tar.gz", VerifyFileParser.extractFileName("  /tmp/a.tar.gz  "));
        Assertions.assertNull(VerifyFileParser.extractFileName(null));
        Assertions.assertNull(VerifyFileParser.extractFileName("   "));
        Assertions.assertNull(VerifyFileParser.extractFileName("/tmp/"));
    }

    /** rawFields 是防御性拷贝，外部修改不能影响已解析的结果。 */
    @Test
    void testRawFieldsIsDefensiveCopy() {
        VerifyFileMeta meta = parse("a.tar.gz|100|md5a", 0, 1, 2).get(0);
        String[] fields = meta.getRawFields();
        fields[0] = "tampered";
        Assertions.assertEquals("a.tar.gz", meta.getRawFields()[0]);
    }

    private static List<VerifyFileMeta> parse(
            String content, int nameIndex, int countIndex, int md5Index) {
        return VerifyFileParser.parse(
                content, DELIMITER, nameIndex, countIndex, md5Index, VERIFY_FILE);
    }
}
