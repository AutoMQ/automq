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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns asynchronous batch disk I/O and its stream-offset index. Queue saturation and cache failures
 * reject writes or return read misses; index lookup and disk I/O run on the workers.
 * The byte budget covers queued and running writes and running reads, measured by encoded lengths.
 * Queued reads reserve their bytes after index lookup, before disk copying.
 * Admitted writes hold private copies, so the budget bounds their memory; returned read buffers
 * belong to the caller and are outside this budget. Snapshot eviction, fetch fallback and broker configuration belong
 * to the integrating owner.
 */
public final class AsyncStreamRecordBatchDiskCache implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncStreamRecordBatchDiskCache.class);
    private final StreamRecordBatchDiskCache diskCache;
    private final StreamRecordBatchDiskIndex index;
    private final ThreadPoolExecutor executor;
    private final long maxPendingBytes;
    private long pendingBytes;
    private boolean closed;

    /**
     * Creates standalone storage and workers. No runtime cache hooks are installed.
     *
     * @param path disposable backing file
     * @param capacityBytes disk capacity; zero disables storage
     * @param maxEntries maximum indexed batches
     * @param workerCount number of dedicated I/O workers, positive
     * @param queueCapacity maximum queued tasks, positive; running tasks are additional
     * @param maxPendingBytes maximum encoded bytes reserved by writes and running reads;
     *     queued reads reserve after lookup; nonpositive values reject all work
     * @throws IOException if the backing file cannot be created or mapped
     */
    public AsyncStreamRecordBatchDiskCache(String path, long capacityBytes, int maxEntries,
        int workerCount, int queueCapacity, long maxPendingBytes) throws IOException {
        this(new StreamRecordBatchDiskCache(path, capacityBytes), new StreamRecordBatchDiskIndex(maxEntries),
            workerCount, queueCapacity, maxPendingBytes);
    }

    AsyncStreamRecordBatchDiskCache(StreamRecordBatchDiskCache diskCache, StreamRecordBatchDiskIndex index,
        int workerCount, int queueCapacity, long maxPendingBytes) {
        this.diskCache = Objects.requireNonNull(diskCache, "diskCache");
        this.index = Objects.requireNonNull(index, "index");
        this.maxPendingBytes = maxPendingBytes;
        this.executor = new ThreadPoolExecutor(workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity), ThreadUtils.createThreadFactory("batch-disk-cache-%d", true),
            new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Copies an admitted batch before enqueueing and releases the copy after completion or rejection.
     * The caller keeps ownership of the batch and may release it as soon as this method returns.
     * An index entry is published only after a stored write.
     * Cancellation discards queued writes; an already running write may still populate the cache.
     *
     * @param batch complete encoded batch; caller ownership is unchanged
     * @return true after storage and index publication, or false on rejection, shutdown or cache failure;
     *     eviction can make a successfully admitted batch miss later
     */
    public CompletableFuture<Boolean> put(StreamRecordBatch batch) {
        return submit(new WriteTask(batch));
    }

    /**
     * Selects continuous whole-batch coverage from the index and reads it on the I/O workers.
     * Removes every reported stale range from the index. No coverage from startOffset,
     * overload and runtime I/O or decoding failures return a miss. Cancelling a queued task starts
     * no disk work; cancellation during a read releases its eventual buffers instead of handing
     * them to the caller.
     *
     * @param streamId stream to read
     * @param startOffset first requested offset, inclusive
     * @param endOffset last requested offset, exclusive
     * @param maxBytes soft payload limit, including the complete final batch
     * @return ordered caller-owned batches to release, or an empty list on a miss
     */
    public CompletableFuture<List<StreamRecordBatch>> get(long streamId, long startOffset,
        long endOffset, int maxBytes) {
        return submit(new ReadTask(streamId, startOffset, endOffset, maxBytes));
    }

    private <T> CompletableFuture<T> submit(CacheTask<T> task) {
        if (!reserveBytes(task.bytes)) {
            return CompletableFuture.completedFuture(task.miss);
        }
        task.future.whenComplete((result, error) -> {
            if (task.future.isCancelled()) {
                task.discard();
                executor.remove(task);
            }
        });
        try {
            task.retainSource();
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            task.discard();
        } catch (RuntimeException | Error e) {
            task.discard();
            throw e;
        }
        return task.future;
    }

    private synchronized boolean reserveBytes(long bytes) {
        if (closed || maxPendingBytes <= 0 || bytes > maxPendingBytes - pendingBytes) {
            return false;
        }
        pendingBytes += bytes;
        return true;
    }

    private synchronized void releaseBytes(long bytes) {
        pendingBytes -= bytes;
    }

    /**
     * Stops admissions and discards queued tasks, completing them as rejection or miss.
     * Interrupts workers without waiting for running I/O, which retains its resources until it exits.
     * This closes the asynchronous owner; mapped-file lifetime follows FileCache.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        for (Runnable task : executor.shutdownNow()) {
            ((CacheTask<?>) task).discard();
        }
    }

    private abstract class CacheTask<T> implements Runnable {
        long bytes;
        final T miss;
        final CompletableFuture<T> future = new CompletableFuture<>();
        private final AtomicBoolean claimed = new AtomicBoolean();

        CacheTask(long bytes, T miss) {
            this.bytes = bytes;
            this.miss = miss;
        }

        void retainSource() {
        }

        void releaseSource() {
        }

        void releaseResult(T result) {
        }

        abstract T execute();

        @Override
        public void run() {
            if (!claimed.compareAndSet(false, true)) {
                return;
            }
            T result = miss;
            try {
                if (!future.isDone()) {
                    result = execute();
                }
            } catch (RuntimeException e) {
                LOGGER.warn("Batch disk cache task failed; treating it as a cache miss", e);
            } catch (Error e) {
                future.completeExceptionally(e);
                throw e;
            } finally {
                cleanup();
            }
            if (!future.complete(result)) {
                releaseResult(result);
            }
        }

        void discard() {
            if (claimed.compareAndSet(false, true)) {
                cleanup();
                future.complete(miss);
            }
        }

        private void cleanup() {
            try {
                releaseSource();
            } finally {
                releaseBytes(bytes);
            }
        }
    }

    private final class WriteTask extends CacheTask<Boolean> {
        private final StreamRecordBatch source;
        private StreamRecordBatch batch;

        WriteTask(StreamRecordBatch source) {
            super(source.encoded().readableBytes(), false);
            this.source = source;
        }

        @Override
        void retainSource() {
            // A retained slice would keep the caller's whole allocator slab alive while the write is queued.
            batch = StreamRecordBatch.parse(source.encoded(), true,
                capacity -> ByteBufAlloc.byteBuffer(capacity, ByteBufAlloc.SNAPSHOT_READ_CACHE));
        }

        @Override
        void releaseSource() {
            if (batch != null) {
                batch.release();
            }
        }

        @Override
        Boolean execute() {
            // Keep write and publication in the same order even with multiple workers.
            synchronized (diskCache) {
                Optional<FileCache.ReadRange> range = diskCache.tryPut(batch);
                synchronized (AsyncStreamRecordBatchDiskCache.this) {
                    if (closed || range.isEmpty()) {
                        return false;
                    }
                }
                // Index contention must not hold up task admission on the submitting thread.
                index.put(batch, range.get());
                return true;
            }
        }
    }

    private final class ReadTask extends CacheTask<List<StreamRecordBatch>> {
        private final long streamId;
        private final long startOffset;
        private final long endOffset;
        private final int maxBytes;

        ReadTask(long streamId, long startOffset, long endOffset, int maxBytes) {
            super(0, List.of());
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.maxBytes = maxBytes;
        }

        @Override
        List<StreamRecordBatch> execute() {
            List<FileCache.ReadRange> ranges = index.get(streamId, startOffset, endOffset, maxBytes);
            if (ranges.isEmpty() || future.isDone()) {
                return List.of();
            }
            long encodedBytes = 0;
            for (FileCache.ReadRange range : ranges) {
                encodedBytes = Math.addExact(encodedBytes, range.length());
            }
            if (!reserveBytes(encodedBytes)) {
                return List.of();
            }
            bytes = encodedBytes;
            if (future.isDone()) {
                return List.of();
            }
            StreamRecordBatchDiskCache.BulkReadResult result = diskCache.get(ranges);
            if (!result.missingRanges().isEmpty()) {
                index.remove(result.missingRanges());
                return List.of();
            }
            return result.batches();
        }

        @Override
        void releaseResult(List<StreamRecordBatch> result) {
            result.forEach(StreamRecordBatch::release);
        }
    }
}
