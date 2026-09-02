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

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 校验文件中一条记录解析后的元信息。
 *
 * <p>校验文件典型内容（单行、分隔符切分、字段按下标取值）：
 *
 * <pre>
 * order_20260902.tar.gz|100000|9e107d9d372bb6826bd81d3542a419d6|20260902|batch_a
 * </pre>
 *
 * <p>末尾追加的额外字段会保留在 {@link #rawFields} 中，不影响既有下标解析。
 */
@Getter
@ToString
public class VerifyFileMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 声明条数缺失时的取值。 */
    public static final long COUNT_ABSENT = -1L;

    /** 数据文件（压缩包）名称，仅文件名不含目录。 */
    private final String packageName;

    /** 压缩包内数据总条数，即「发送数据量」；缺失或无法解析时为 {@link #COUNT_ABSENT}。 */
    private final long declaredCount;

    /** 压缩包 MD5 值，未配置取值下标或字段缺失时为 null。 */
    private final String expectedMd5;

    /** 该行切分后的全部原始字段，便于后续扩展取用。 */
    private final String[] rawFields;

    /** 该记录来源的校验文件路径，用于日志定位。 */
    private final String verifyFilePath;

    public VerifyFileMeta(
            String packageName,
            long declaredCount,
            String expectedMd5,
            String[] rawFields,
            String verifyFilePath) {
        this.packageName = packageName;
        this.declaredCount = declaredCount;
        this.expectedMd5 = expectedMd5;
        this.rawFields = rawFields == null ? new String[0] : Arrays.copyOf(rawFields, rawFields.length);
        this.verifyFilePath = verifyFilePath;
    }

    /** 是否解析到了有效的声明条数。 */
    public boolean hasDeclaredCount() {
        return declaredCount >= 0;
    }

    /** 是否解析到了 MD5 值。 */
    public boolean hasExpectedMd5() {
        return expectedMd5 != null && !expectedMd5.isEmpty();
    }

    /** 返回原始字段的拷贝，避免外部修改影响已解析的结果。 */
    public String[] getRawFields() {
        return Arrays.copyOf(rawFields, rawFields.length);
    }
}
