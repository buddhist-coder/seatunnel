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

package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.Getter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;

public class FileSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    @Getter private final String tableId;
    @Getter private final String filePath;
    @Getter private long start = 0;
    @Getter private long length = -1;

    /**
     * 校验文件声明的数据条数，即「发送数据量」。
     *
     * <p>使用包装类型而非 long，是为了让旧版本 checkpoint 反序列化后天然为 null（表示无声明），与「真实声明了 0 条」区分开。
     */
    @Getter private Long declaredCount;

    /** 校验文件声明的 MD5 值，null 表示未声明。 */
    @Getter private String expectedMd5;

    public FileSourceSplit(String splitId) {
        this.filePath = splitId;
        this.tableId = null;
    }

    public FileSourceSplit(String tableId, String filePath) {
        this.tableId = tableId;
        this.filePath = filePath;
    }

    public FileSourceSplit(String tableId, String filePath, long start, long length) {
        this.tableId = tableId;
        this.filePath = filePath;
        this.start = start;
        this.length = length;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Compatibility: old checkpoints (before file-split fields) deserialize with
        // start=0/length=0.
        if (start == 0L && length == 0L) {
            length = -1L;
        }
        // 兼容性：旧 checkpoint 不含校验文件字段，反序列化后 declaredCount / expectedMd5 均为 null，
        // 语义即「无声明」，无需额外处理。
    }

    /**
     * 填充校验文件解析出的元信息。由 Enumerator 在生成 split 后调用。
     *
     * <p>这两个字段不参与 {@link #equals} 与 {@link #hashCode}，因此不会影响 addSplitsBack 的去重与 split 排序。
     */
    public void applyVerifyMeta(Long declaredCount, String expectedMd5) {
        this.declaredCount = declaredCount;
        this.expectedMd5 = expectedMd5;
    }

    /** 是否携带了有效的声明条数。 */
    public boolean hasDeclaredCount() {
        return declaredCount != null && declaredCount >= 0L;
    }

    /** 是否携带了声明的 MD5 值。 */
    public boolean hasExpectedMd5() {
        return expectedMd5 != null && !expectedMd5.isEmpty();
    }

    @Override
    public String splitId() {
        // In order to be compatible with the split before the upgrade, when tableId is null,
        // filePath is directly returned
        if (tableId == null) {
            return filePath;
        }
        if (start == 0L && length < 0L) {
            return tableId + "_" + filePath;
        }
        return tableId + "_" + filePath + "_" + start;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSourceSplit that = (FileSourceSplit) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(start, that.start)
                && Objects.equals(length, that.length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, filePath, start, length);
    }
}
