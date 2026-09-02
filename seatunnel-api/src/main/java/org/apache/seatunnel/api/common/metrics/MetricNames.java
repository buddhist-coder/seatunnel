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

package org.apache.seatunnel.api.common.metrics;

public final class MetricNames {

    private MetricNames() {}

    public static final String RECEIVED_COUNT = "receivedCount";

    public static final String RECEIVED_BATCHES = "receivedBatches";

    public static final String SOURCE_RECEIVED_COUNT = "SourceReceivedCount";
    public static final String SOURCE_RECEIVED_BYTES = "SourceReceivedBytes";
    public static final String SOURCE_RECEIVED_QPS = "SourceReceivedQPS";
    public static final String SOURCE_RECEIVED_BYTES_PER_SECONDS = "SourceReceivedBytesPerSeconds";

    /**
     * 发送数据量：由上游校验文件声明的数据总条数，与读数据量、写数据量同级别。
     *
     * <p>该指标不是逐行累加得来的，而是由 source 连接器解析校验文件后一次性上报，仅用于观测与对账，不参与任何一致性校验。
     */
    public static final String SOURCE_SENT_COUNT = "SourceSentCount";

    public static final String SINK_WRITE_COUNT = "SinkWriteCount";
    public static final String SINK_WRITE_BYTES = "SinkWriteBytes";
    public static final String SINK_WRITE_QPS = "SinkWriteQPS";
    public static final String SINK_WRITE_BYTES_PER_SECONDS = "SinkWriteBytesPerSeconds";
    public static final String SINK_COMMITTED_COUNT = "SinkCommittedCount";
    public static final String SINK_COMMITTED_BYTES = "SinkCommittedBytes";
    public static final String SINK_COMMITTED_QPS = "SinkCommittedQPS";
    public static final String SINK_COMMITTED_BYTES_PER_SECONDS = "SinkCommittedBytesPerSeconds";

    public static final String INTERMEDIATE_QUEUE_SIZE = "IntermediateQueueSize";
}
