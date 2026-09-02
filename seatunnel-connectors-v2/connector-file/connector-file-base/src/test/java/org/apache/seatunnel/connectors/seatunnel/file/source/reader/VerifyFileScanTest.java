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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.verify.VerifyFileMeta;
import org.apache.seatunnel.connectors.seatunnel.file.writer.ParquetReadStrategyTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/** 校验文件扫描与过滤行为的测试：验证校验文件被剔除、未声明的数据文件被跳过。 */
public class VerifyFileScanTest {

    private static final String SAMPLE_LINE =
            "order_20260902.tar.gz|100000|9E107D9D372BB6826BD81D3542A419D6|20260902|batch_a";

    @TempDir java.nio.file.Path tempDir;

    /** 核心场景：校验文件自身不进数据列表，未被声明的数据包本轮跳过。 */
    @Test
    void testVerifyFileExcludedAndUndeclaredFileSkipped() throws Exception {
        writeFile("order_20260902.tar.gz", "fake-archive-content");
        // 该包还在传输中，尚未生成校验文件
        writeFile("order_20260903.tar.gz", "fake-archive-content");
        writeFile("order_20260902.success", SAMPLE_LINE);

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            Assertions.assertEquals(1, fileNames.size());
            Assertions.assertTrue(fileNames.get(0).endsWith("order_20260902.tar.gz"));

            Map<String, VerifyFileMeta> metaMap = strategy.getVerifyFileMetaMap();
            Assertions.assertEquals(1, metaMap.size());
            VerifyFileMeta meta = metaMap.get("order_20260902.tar.gz");
            Assertions.assertNotNull(meta);
            Assertions.assertEquals(100000L, meta.getDeclaredCount());
            Assertions.assertEquals("9e107d9d372bb6826bd81d3542a419d6", meta.getExpectedMd5());
        }
    }

    /** 总开关关闭时必须保持与改动前完全一致的行为：校验文件照旧被当作数据文件。 */
    @Test
    void testDisabledKeepsLegacyBehavior() throws Exception {
        writeFile("order_20260902.tar.gz", "fake-archive-content");
        writeFile("order_20260902.success", SAMPLE_LINE);

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(false, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            Assertions.assertEquals(2, fileNames.size());
            Assertions.assertTrue(strategy.getVerifyFileMetaMap().isEmpty());
        }
    }

    /** filename_extension 只用于筛数据文件，不能把校验文件一起挡掉。 */
    @Test
    void testVerifyFileRecognizedRegardlessOfFilenameExtension() throws Exception {
        writeFile("order_20260902.tar.gz", "fake-archive-content");
        writeFile("order_20260902.success", SAMPLE_LINE);

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, ".tar.gz"));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            Assertions.assertEquals(1, fileNames.size());
            Assertions.assertTrue(fileNames.get(0).endsWith("order_20260902.tar.gz"));
            Assertions.assertEquals(1, strategy.getVerifyFileMetaMap().size());
        }
    }

    /** 目录里一个校验文件都没有时，视为上游尚未投放完成，本轮不读任何数据文件。 */
    @Test
    void testAllFilesSkippedWhenNoVerifyFilePresent() throws Exception {
        writeFile("order_20260902.tar.gz", "fake-archive-content");

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            Assertions.assertTrue(strategy.getFileNamesByPath(tempDir.toString()).isEmpty());
            Assertions.assertTrue(strategy.getVerifyFileMetaMap().isEmpty());
        }
    }

    /** 一个校验文件声明多个包时，多行内容都要生效。 */
    @Test
    void testSingleVerifyFileDeclaringMultiplePackages() throws Exception {
        writeFile("a.tar.gz", "fake-archive-content");
        writeFile("b.tar.gz", "fake-archive-content");
        writeFile("c.tar.gz", "fake-archive-content");
        writeFile("batch.success", "a.tar.gz|10|md5a\nb.tar.gz|20|md5b");

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            // c.tar.gz 未被声明，本轮跳过
            Assertions.assertEquals(2, fileNames.size());
            Assertions.assertEquals(2, strategy.getVerifyFileMetaMap().size());
            Assertions.assertEquals(
                    20L, strategy.getVerifyFileMetaMap().get("b.tar.gz").getDeclaredCount());
        }
    }

    /** 同一目录下有多个校验文件时，每一个都要被读取解析，结果合并到同一个映射。 */
    @Test
    void testMultipleVerifyFilesInSameDirectory() throws Exception {
        writeFile("a.tar.gz", "fake-archive-content");
        writeFile("b.tar.gz", "fake-archive-content");
        writeFile("c.tar.gz", "fake-archive-content");
        writeFile("a.success", "a.tar.gz|10|md5a");
        writeFile("b.success", "b.tar.gz|20|md5b");
        writeFile("c.success", "c.tar.gz|30|md5c");

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            // 三个校验文件全部生效，三个数据包都被读取
            Assertions.assertEquals(3, fileNames.size());
            Map<String, VerifyFileMeta> metaMap = strategy.getVerifyFileMetaMap();
            Assertions.assertEquals(3, metaMap.size());
            Assertions.assertEquals(10L, metaMap.get("a.tar.gz").getDeclaredCount());
            Assertions.assertEquals(20L, metaMap.get("b.tar.gz").getDeclaredCount());
            Assertions.assertEquals(30L, metaMap.get("c.tar.gz").getDeclaredCount());
            // 每条声明都记录了来源校验文件，便于排查
            Assertions.assertTrue(
                    metaMap.get("b.tar.gz").getVerifyFilePath().endsWith("b.success"));
        }
    }

    /** 子目录中的校验文件同样会被递归收集。 */
    @Test
    void testVerifyFilesInSubDirectories() throws Exception {
        writeFile("dt=20260902/a.tar.gz", "fake-archive-content");
        writeFile("dt=20260902/a.success", "a.tar.gz|10|md5a");
        writeFile("dt=20260903/b.tar.gz", "fake-archive-content");
        writeFile("dt=20260903/b.success", "b.tar.gz|20|md5b");

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            Assertions.assertEquals(2, fileNames.size());
            Assertions.assertEquals(2, strategy.getVerifyFileMetaMap().size());
            Assertions.assertEquals(
                    10L, strategy.getVerifyFileMetaMap().get("a.tar.gz").getDeclaredCount());
        }
    }

    /** 多个校验文件声明了同一个包名时后者覆盖前者，只告警不抛异常。 */
    @Test
    void testDuplicateDeclarationAcrossVerifyFiles() throws Exception {
        writeFile("a.tar.gz", "fake-archive-content");
        writeFile("first.success", "a.tar.gz|10|md5a");
        writeFile("second.success", "a.tar.gz|20|md5b");

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.setPluginConfig(buildConfig(true, null));
            strategy.init(new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT));

            List<String> fileNames = strategy.getFileNamesByPath(tempDir.toString());

            Assertions.assertEquals(1, fileNames.size());
            Map<String, VerifyFileMeta> metaMap = strategy.getVerifyFileMetaMap();
            // 同一包名只保留一条；保留哪一条取决于文件系统的列举顺序，因此不断言具体值
            Assertions.assertEquals(1, metaMap.size());
            long declaredCount = metaMap.get("a.tar.gz").getDeclaredCount();
            Assertions.assertTrue(declaredCount == 10L || declaredCount == 20L);
        }
    }

    private void writeFile(String name, String content) throws IOException {
        java.nio.file.Path file = tempDir.resolve(name);
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    }

    private Config buildConfig(boolean verifyEnabled, String filenameExtension) {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), tempDir.toString());
        config.put(FileBaseSourceOptions.VERIFY_FILE_ENABLED.key(), verifyEnabled);
        if (filenameExtension != null) {
            config.put(FileBaseSourceOptions.FILENAME_EXTENSION.key(), filenameExtension);
        }
        return ConfigFactory.parseMap(config);
    }
}
