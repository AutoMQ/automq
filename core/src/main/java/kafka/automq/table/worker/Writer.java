/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.table.worker;

import kafka.automq.table.events.PartitionMetric;
import kafka.automq.table.events.TopicMetric;

import org.apache.iceberg.io.WriteResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

public interface Writer {

    /**
     * Write record to memory
     */
    void write(int partition, org.apache.kafka.common.record.Record kafkaRecord) throws IOException;

    /**
     * Asynchronously flush the data from memory to persistence storage
     */
    CompletableFuture<Void> flush(FlushMode flushMode, ExecutorService flushExecutor, Executor eventLoop);

    /**
     * Abort the writer and clean up the inflight resources.
     */
    void abort() throws IOException;

    /**
     * Complete the writer and return the results
     */
    List<WriteResult> complete() throws IOException;

    /**
     * Get the results of the completed writer
     */
    List<WriteResult> results();

    /**
     * Check if the writer is completed
     */
    boolean isCompleted();

    /**
     * Check if the writer is full if full should switch to a new writer.
     */
    boolean isFull();

    /**
     * Get partition to the offset range map .
     */
    Map<Integer, OffsetRange> getOffsets();

    /**
     * Get partition offset range.
     */
    IcebergWriter.OffsetRange getOffset(int partition);

    /**
     * Set partition initial offset range [offset, offset).
     */
    void setOffset(int partition, long offset);

    /**
     * Set new end offset for the partition.
     */
    void setEndOffset(int partition, long offset);

    /**
     * Get in memory dirty bytes.
     */
    long dirtyBytes();

    void updateWatermark(int partition, long timestamp);

    TopicMetric topicMetric();

    Map<Integer, PartitionMetric> partitionMetrics();

    int targetFileSize();

    class OffsetRange {
        long start;
        long end;

        public OffsetRange(long start) {
            this.start = start;
            this.end = start;
        }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }

    }
}
