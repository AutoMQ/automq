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

import kafka.cluster.Partition;
import kafka.log.streamaspect.ReadHint;

import org.apache.kafka.common.record.PooledResource;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchIsolation;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.utils.AsyncSemaphore;
import com.automq.stream.utils.Systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
class PartitionWriteTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionWriteTask.class);
    private static final AsyncSemaphore READ_LIMITER = new AsyncSemaphore(Systems.CPU_CORES * 100L * 1024 * 1024);
    private final long startTimestamp = System.currentTimeMillis();
    final Partition partition;
    final long taskStartOffset;
    final long endOffset;
    final PartitionWriteTaskContext ctx;
    final CompletableFuture<Void> cf = new CompletableFuture<>();

    public PartitionWriteTask(Partition partition, long startOffset, long endOffset, PartitionWriteTaskContext ctx) {
        this.partition = partition;
        this.taskStartOffset = Math.max(startOffset, partition.log().get().logStartOffset());
        this.endOffset = endOffset;
        this.ctx = ctx;
    }

    public CompletableFuture<Void> run() {
        run0(taskStartOffset);
        return cf;
    }

    private void run0(long startOffset) {
        int readSize = 1024 * 1024;
        // limit the read direct memory usage.
        READ_LIMITER.acquire(readSize, () -> {
            CompletableFuture<Void> readReleaseCf = new CompletableFuture<>();
            readAsync(startOffset, readSize)
                .thenCompose(rst -> ctx.eventLoop.execute(() -> handleReadResult(startOffset, rst), ctx.priority))
                .exceptionally(ex -> {
                    ctx.requireReset = true;
                    LOGGER.error("Error in read task {}", this, ex);
                    cf.complete(null);
                    return null;
                }).whenComplete((nil, ex) -> readReleaseCf.complete(null));
            return readReleaseCf;
        }, ctx.eventLoop);
    }

    private CompletableFuture<FetchDataInfo> readAsync(long startOffset, int readSize) {
        try {
            ReadHint.markReadAll();
            // FIXME: wrap readAsync the exception in the future.
            return partition.log().get()
                .readAsync(startOffset, readSize, FetchIsolation.TXN_COMMITTED, true);
        } catch (Throwable ex) {
            // When the partition is closed, the readAsync will throw exception.
            return CompletableFuture.failedFuture(ex);
        }
    }

    private void handleReadResult(long startOffset, FetchDataInfo rst) {
        BufferSupplier bufferSupplier = BufferSupplier.create();
        try {
            if (ctx.requireReset) {
                cf.complete(null);
                return;
            }
            if (!ctx.lastFlushCf.isDone()) {
                // Avoid concurrent write and flush to the same writer
                ctx.lastFlushCf.whenComplete((nil, ex) -> {
                    if (ex != null) {
                        ctx.requireReset = true;
                        cf.complete(null);
                    } else {
                        ctx.eventLoop.execute(() -> handleReadResult(startOffset, rst), ctx.priority);
                    }
                });
                return;
            }
            TimerUtil timer = new TimerUtil();

            long writeSize = 0;
            RecordsIterator it = new RecordsIterator(startOffset, rst, bufferSupplier);
            long nextOffset = startOffset;
            while (it.hasNext()) {
                Record record = it.next();
                long recordOffset = record.offset();
                if (recordOffset >= startOffset && recordOffset < endOffset) {
                    ctx.writer.write(partition.partitionId(), record);
                    writeSize += record.sizeInBytes();
                    nextOffset = recordOffset + 1;
                } else if (recordOffset >= endOffset) {
                    nextOffset = endOffset;
                    // Abort transactions might occupy the offsets
                    ctx.writer.setEndOffset(partition.partitionId(), nextOffset);
                    break;
                }
            }
            if (!it.hasNext()) {
                nextOffset = Math.min(endOffset, it.nextOffset());
                // Abort transactions might occupy the offsets
                ctx.writer.setEndOffset(partition.partitionId(), nextOffset);
            }

            if (ctx.writer.dirtyBytes() >= ctx.writer.targetFileSize()) {
                ctx.lastFlushCf = ctx.writer.flush(FlushMode.FLUSH, ctx.flushExecutors, ctx.eventLoop);
            }
            if (nextOffset != startOffset && nextOffset < endOffset) {
                // launch next round read until read to the end.
                long finalNextOffset = nextOffset;
                ctx.eventLoop.execute(() -> run0(finalNextOffset));
            } else {
                if (nextOffset == partition.log().get().highWatermark()) {
                    // Update the timestamp to the start timestamp to avoid that situation low traffic topic record old timestamps.
                    ctx.writer.updateWatermark(partition.partitionId(), startTimestamp);
                }
                ctx.lastFlushCf.whenCompleteAsync((nil, ex) -> cf.complete(null), ctx.eventLoop);
            }
            if (nextOffset != startOffset) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("[HANDLE_READ_RESULT],{},{}-{},{}ms", this, startOffset, nextOffset, timer.elapsedAs(TimeUnit.MILLISECONDS));
                }
                ctx.recordWriteSize(writeSize);
            }
        } catch (Throwable e) {
            ctx.requireReset = true;
            LOGGER.error("[HANDLE_READ_RESULT_FAIL],{},{}-{}", this, startOffset, endOffset, e);
            cf.complete(null);
        } finally {
            if (rst.records instanceof PooledResource) {
                ((PooledResource) rst.records).release();
            }
            bufferSupplier.close();
        }
    }

    @Override
    public String toString() {
        return partition.topic() + "-" + partition.partitionId();
    }

}
