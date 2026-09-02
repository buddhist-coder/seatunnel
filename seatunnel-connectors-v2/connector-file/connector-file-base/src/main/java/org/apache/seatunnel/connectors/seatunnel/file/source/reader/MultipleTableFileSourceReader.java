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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode.FILE_READ_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode.FILE_READ_STRATEGY_NOT_SUPPORT;

@Slf4j
public class MultipleTableFileSourceReader implements SourceReader<SeaTunnelRow, FileSourceSplit> {

    private final Context context;
    private volatile boolean noMoreSplit;

    private final Deque<FileSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();

    private final Map<String, ReadStrategy> readStrategyMap;

    /**
     * 发送数据量计数器：累加上游校验文件声明的条数，与 SourceReceivedCount、SinkWriteCount 同级别。
     *
     * <p>该指标不是逐行累加的，而是每个压缩包上报一次；仅用于观测对账，不参与任何校验。
     */
    private Counter sentCountCounter;

    public MultipleTableFileSourceReader(
            Context context, BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig) {
        this.context = context;
        this.readStrategyMap =
                multipleTableFileSourceConfig.getFileSourceConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        fileSourceConfig ->
                                                fileSourceConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        BaseFileSourceConfig::createReadStrategy));
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        synchronized (output.getCheckpointLock()) {
            FileSourceSplit split = sourceSplits.poll();
            if (null != split) {
                ReadStrategy readStrategy = readStrategyMap.get(split.getTableId());
                if (readStrategy == null) {
                    throw new FileConnectorException(
                            FILE_READ_STRATEGY_NOT_SUPPORT,
                            "Cannot found the read strategy for this table: ["
                                    + split.getTableId()
                                    + "]");
                }
                // 读取前的前置校验（当前为 MD5，仅 verify_md5_enabled=true 时实际执行）。
                // 放在 try 之外，让校验失败的异常保留自身的明确原因，不被 FILE_READ_FAILED 二次包装
                verifyBeforeRead(readStrategy, split);
                try {
                    // 上报「发送数据量」：来自上游校验文件的声明，只上报、不参与校验
                    reportSentCount(split);
                    readStrategy.read(split, output);
                } catch (Exception e) {
                    String errorMsg =
                            String.format("Read data from this file [%s] failed", split.splitId());
                    throw new FileConnectorException(FILE_READ_FAILED, errorMsg, e);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info(
                        "There is no more element for the bounded MultipleTableLocalFileSourceReader");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<FileSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public void open() throws Exception {
        log.info("Opened the MultipleTableLocalFileSourceReader");
        // MetricsContext 在 Zeta 引擎下是真实实现；Flink/Spark 目前仍是空实现（社区 #3431），指标会被静默丢弃
        this.sentCountCounter = context.getMetricsContext().counter(MetricNames.SOURCE_SENT_COUNT);
    }

    /** 读取前的前置校验（当前为 MD5）。校验失败的异常已携带明确原因，直接抛出不再包装。 */
    private void verifyBeforeRead(ReadStrategy readStrategy, FileSourceSplit split) {
        try {
            readStrategy.verifyBeforeRead(split);
        } catch (FileConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new FileConnectorException(
                    FILE_READ_FAILED,
                    String.format("Verify file [%s] before read failed", split.splitId()),
                    e);
        }
    }

    /** 上报发送数据量。一个压缩包只上报一次；未携带声明条数的 split 不上报。 */
    private void reportSentCount(FileSourceSplit split) {
        if (sentCountCounter == null || !split.hasDeclaredCount()) {
            return;
        }
        long declaredCount = split.getDeclaredCount();
        sentCountCounter.inc(declaredCount);
        log.info(
                "Reported SourceSentCount {} declared by the verify file for split [{}].",
                declaredCount,
                split.splitId());
    }

    @Override
    public void close() throws IOException {
        // do nothing
        log.info("Closed the MultipleTableLocalFileSourceReader");
        for (ReadStrategy strategy : readStrategyMap.values()) {
            strategy.close();
        }
    }
}
