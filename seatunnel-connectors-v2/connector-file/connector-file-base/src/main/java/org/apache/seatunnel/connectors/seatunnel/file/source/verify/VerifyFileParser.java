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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 校验文件解析器。
 *
 * <p>按可配分隔符切分单行，按可配下标取「压缩包名称 / 数据总条数 / MD5」三个字段；下标配 -1 表示不取该字段。末尾追加的额外字段不影响解析，
 * 因此上游后续扩展内容无需改动本实现。
 *
 * <p>解析过程对异常内容采取宽松策略：字段缺失或格式非法只告警并跳过，不抛异常中断作业。
 */
@Slf4j
public final class VerifyFileParser {

    /** 下标配置为该值时表示不取对应字段。 */
    public static final int INDEX_ABSENT = -1;

    private VerifyFileParser() {}

    /**
     * 解析校验文件内容。逐行解析，每一非空行产出一条记录，因此单行单包与多行多包两种格式都支持。
     *
     * @param content 校验文件全文内容
     * @param delimiter 字段分隔符（原样字符串，内部自动转义，无需调用方处理正则元字符）
     * @param nameIndex 「压缩包名称」字段下标
     * @param countIndex 「数据总条数」字段下标，-1 表示不取
     * @param md5Index 「MD5」字段下标，-1 表示不取
     * @param verifyFilePath 校验文件路径，仅用于日志定位
     * @return 解析出的记录列表，永不为 null
     */
    public static List<VerifyFileMeta> parse(
            String content,
            String delimiter,
            int nameIndex,
            int countIndex,
            int md5Index,
            String verifyFilePath) {
        List<VerifyFileMeta> metas = new ArrayList<>();
        if (StringUtils.isBlank(content)) {
            log.warn("Verify file [{}] is empty, no declaration parsed.", verifyFilePath);
            return metas;
        }
        if (nameIndex < 0) {
            throw new IllegalArgumentException(
                    "verify_field_index_name must not be negative, the package name field is required for pairing.");
        }

        String fieldSplitRegex = Pattern.quote(delimiter);
        // 同时兼容 \n 与 \r\n 换行
        String[] lines = content.split("\\r?\\n", -1);
        for (String rawLine : lines) {
            String line = rawLine == null ? null : rawLine.trim();
            if (StringUtils.isBlank(line)) {
                continue;
            }
            // -1 保留末尾空字段，避免下标错位
            String[] fields = line.split(fieldSplitRegex, -1);

            String packageName = extractPackageName(fields, nameIndex);
            if (StringUtils.isBlank(packageName)) {
                log.warn(
                        "Verify file [{}] line [{}] has no package name at index {}, this line is skipped.",
                        verifyFilePath,
                        line,
                        nameIndex);
                continue;
            }

            long declaredCount = extractDeclaredCount(fields, countIndex, verifyFilePath, line);
            String expectedMd5 = extractMd5(fields, md5Index);

            metas.add(
                    new VerifyFileMeta(
                            packageName, declaredCount, expectedMd5, fields, verifyFilePath));
        }
        return metas;
    }

    /**
     * 从路径中提取文件名。
     *
     * <p>数据文件与校验文件的配对以文件名为键，因此校验文件里写的是全路径还是纯文件名都能正确配对。 扫描侧与解析侧共用本方法，保证两边的键完全一致。
     */
    public static String extractFileName(String path) {
        if (StringUtils.isBlank(path)) {
            return null;
        }
        // 统一路径分隔符后取最后一段
        String normalized = path.trim().replace('\\', '/');
        int lastSlash = normalized.lastIndexOf('/');
        return lastSlash < 0
                ? StringUtils.trimToNull(normalized)
                : StringUtils.trimToNull(normalized.substring(lastSlash + 1));
    }

    /** 取压缩包名称。上游可能写了带目录的路径，这里统一归一为文件名，便于与扫描到的数据文件配对。 */
    private static String extractPackageName(String[] fields, int nameIndex) {
        if (nameIndex >= fields.length) {
            return null;
        }
        return extractFileName(fields[nameIndex]);
    }

    /** 取声明条数。该值仅用于指标上报，解析失败不影响读取，只降级为缺失并告警。 */
    private static long extractDeclaredCount(
            String[] fields, int countIndex, String verifyFilePath, String line) {
        if (countIndex == INDEX_ABSENT) {
            return VerifyFileMeta.COUNT_ABSENT;
        }
        if (countIndex < 0 || countIndex >= fields.length) {
            log.warn(
                    "Verify file [{}] line [{}] has no declared count at index {}, SourceSentCount will not be reported for it.",
                    verifyFilePath,
                    line,
                    countIndex);
            return VerifyFileMeta.COUNT_ABSENT;
        }
        String rawCount = StringUtils.trimToNull(fields[countIndex]);
        if (rawCount == null) {
            return VerifyFileMeta.COUNT_ABSENT;
        }
        try {
            long count = Long.parseLong(rawCount);
            if (count < 0) {
                log.warn(
                        "Verify file [{}] line [{}] declared a negative count [{}], treated as absent.",
                        verifyFilePath,
                        line,
                        rawCount);
                return VerifyFileMeta.COUNT_ABSENT;
            }
            return count;
        } catch (NumberFormatException e) {
            log.warn(
                    "Verify file [{}] line [{}] declared count [{}] is not a valid number, treated as absent.",
                    verifyFilePath,
                    line,
                    rawCount);
            return VerifyFileMeta.COUNT_ABSENT;
        }
    }

    /** 取 MD5 值，统一转小写便于比对。 */
    private static String extractMd5(String[] fields, int md5Index) {
        if (md5Index == INDEX_ABSENT || md5Index < 0 || md5Index >= fields.length) {
            return null;
        }
        String rawMd5 = StringUtils.trimToNull(fields[md5Index]);
        return rawMd5 == null ? null : rawMd5.toLowerCase(java.util.Locale.ROOT);
    }
}
