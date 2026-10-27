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

package com.automq.stream.s3;

import com.automq.stream.RecyclingByteBufSeqAlloc;
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.utils.CloseableIterator;

import org.slf4j.Logger;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static com.automq.stream.s3.ByteBufAlloc.DECODE_RECORD;

/**
 * WAL recovery logic: reads records from the WAL iterator, groups them into
 * {@link LogCache.LogCacheBlock}s, filters invalid streams, decodes link records,
 * and passes each block to a handler for upload.
 */
public class WALRecovery {
    // Owned by this class for the process lifetime so recovery operations reuse the same slabs.
    private static final RecyclingByteBufSeqAlloc DECODE_LINK_RECORD_INSTANT_ALLOC =
        new RecyclingByteBufSeqAlloc(DECODE_RECORD, 1);

    /**
     * Recover records from the WAL iterator, putting them into {@link LogCache.LogCacheBlock}s.
     * <p>
     * It will filter out
     * <ul>
     *     <li>the records that are not in the opening streams</li>
     *     <li>the records that have been committed</li>
     *     <li>discontinuous records (gap between expected and actual offset), with a warning log</li>
     * </ul>
     * <p>
     * For example, if we recover following records from the WAL in a stream:
     * <pre>    1, 2, 3, 4, 5, 6</pre>
     * and the {@link com.automq.stream.s3.metadata.StreamMetadata#endOffset()} of this stream is 3.
     * Then the returned {@link LogCache.LogCacheBlock} will contain records
     * <pre>    3, 4, 5, 6</pre>
     * Here,
     * <ul>
     *     <li>The record 1 and 2 are discarded because they have been committed (less than 3, the end offset of the stream)</li>
     * </ul>
     * <p>
     * Records are processed in blocks, accounting for decoded link payloads before decoding them.
     * When a block is full (size threshold, upload byte count limit, or offset overflow),
     * it is passed to the {@code blockHandler} for upload, and a new block is started.
     * A link decode failure ends recovery at that WAL position: only the global successful prefix
     * is uploaded, and the failed record and all later records are discarded across all streams.
     *
     * @param it                      WAL recover iterator
     * @param openingStreamEndOffsets the end offset of each opening stream
     * @param maxCacheSize            the max size of each {@link LogCache.LogCacheBlock}
     * @param logger                  logger
     * @param blockHandler            called for each block to upload and release records
     */
    public static void recover(
        Iterator<RecoverResult> it,
        Map<Long, Long> openingStreamEndOffsets,
        long maxCacheSize,
        Logger logger,
        BlockHandler blockHandler
    ) {
        long blockSize = Math.min(maxCacheSize, Integer.MAX_VALUE);
        LogCache.LogCacheBlock block = null;
        RecoverResult pending = null;
        try (DecodedIterator decoded = new DecodedIterator(it, openingStreamEndOffsets, blockSize, logger)) {
            try {
                while (decoded.hasNext()) {
                    pending = decoded.next();
                    if (block == null) {
                        block = new LogCache.LogCacheBlock(blockSize);
                    }
                    if (!block.put(pending.record())) {
                        LogCache.LogCacheBlock full = block;
                        block = null;
                        blockHandler.handle(full);
                        block = new LogCache.LogCacheBlock(blockSize);
                        if (!block.put(pending.record())) {
                            throw new IllegalStateException("Deferred WAL record cannot fit in an empty recovery block");
                        }
                    }
                    openingStreamEndOffsets.put(pending.record().getStreamId(), pending.record().getLastOffset());
                    block.lastRecordOffset(pending.recordOffset());
                    pending = null;
                    if (block.isFull()) {
                        LogCache.LogCacheBlock full = block;
                        block = null;
                        blockHandler.handle(full);
                    }
                }
                if (block != null) {
                    block.lastRecordOffset(decoded.lastRecordOffset);
                    LogCache.LogCacheBlock last = block;
                    block = null;
                    blockHandler.handle(last);
                }
            } finally {
                if (block != null) {
                    releaseAllRecords(block.records().values());
                }
                if (pending != null) {
                    pending.record().release();
                }
            }
        }
    }

    /**
     * Filters and decodes bounded batches concurrently, yielding only the successful WAL prefix.
     * Owns prefetched records until next() transfers them to the caller; close() drains the rest.
     * Scan offsets are private so prefetch never advances the recovered stream ends.
     */
    private static class DecodedIterator implements CloseableIterator<RecoverResult> {
        // Bound per-record bookkeeping as well as payloads, matching the cache's 1 KiB allowance.
        private static final int RECORD_OVERHEAD = 1024;
        private final Iterator<RecoverResult> source;
        private final Map<Long, Long> scanOffsets;
        private final long maxBatchSize;
        private final Logger logger;
        private final Deque<RecoverResult> ready = new ArrayDeque<>();
        private RecoverResult next;
        private RecordOffset lastRecordOffset;
        private boolean first = true;
        private boolean sourceFinished;

        DecodedIterator(Iterator<RecoverResult> source, Map<Long, Long> endOffsets, long maxBatchSize, Logger logger) {
            this.source = source;
            this.scanOffsets = new HashMap<>(endOffsets);
            this.maxBatchSize = maxBatchSize;
            this.logger = logger;
        }

        @Override
        public boolean hasNext() {
            while (next == null) {
                if (ready.isEmpty() && !sourceFinished) {
                    loadBatch();
                }
                if (ready.isEmpty()) {
                    return false;
                }
                RecoverResult result = ready.removeFirst();
                if (result.record() == null) {
                    // Filtered records still advance the consumed WAL position.
                    lastRecordOffset = result.recordOffset();
                } else {
                    next = result;
                }
            }
            return true;
        }

        @Override
        public RecoverResult next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            RecoverResult result = next;
            next = null;
            lastRecordOffset = result.recordOffset();
            return result;
        }

        private void loadBatch() {
            Deque<DecodingRecord> batch = new ArrayDeque<>();
            long size = 0;
            try {
                // Test the budget first: source.hasNext() may allocate a prefetched WAL record.
                while (batch.isEmpty() || size < maxBatchSize) {
                    if (!source.hasNext()) {
                        sourceFinished = true;
                        break;
                    }
                    RecoverResult recovered = source.next();
                    StreamRecordBatch record = recovered.record();
                    boolean added = false;
                    try {
                        if (first) {
                            logger.info("recover start offset {}", recovered.recordOffset());
                            first = false;
                        }
                        size += record.size() + (long) RECORD_OVERHEAD;
                        if (shouldSkip(record)) {
                            batch.addLast(new DecodingRecord(recovered, CompletableFuture.completedFuture(null)));
                            added = true;
                            continue;
                        }
                        scanOffsets.put(record.getStreamId(), record.getLastOffset());
                        CompletableFuture<StreamRecordBatch> future;
                        if (record.getCount() < 0) {
                            int decodedSize;
                            try {
                                decodedSize = S3Storage.getLinkRecordDecoder().decodedSize(record.getPayload());
                                if (decodedSize < 0) {
                                    throw new IllegalArgumentException("Negative decoded link record size: " + decodedSize);
                                }
                            } catch (RuntimeException ex) {
                                batch.addLast(new DecodingRecord(recovered, CompletableFuture.failedFuture(ex)));
                                added = true;
                                sourceFinished = true;
                                break;
                            }
                            if (decodedSize > Constants.MAX_UPLOAD_STREAM_SIZE) {
                                throw new IllegalArgumentException("Record payload size exceeds the upload limit: " + decodedSize);
                            }
                            size += decodedSize - record.size();
                            future = S3Storage.getLinkRecordDecoder().decode(record, DECODE_LINK_RECORD_INSTANT_ALLOC);
                        } else {
                            future = CompletableFuture.completedFuture(record);
                        }
                        batch.addLast(new DecodingRecord(recovered, future));
                        added = true;
                    } finally {
                        if (!added) {
                            record.release();
                        }
                    }
                }
                collectDecodedRecords(batch);
            } finally {
                batch.forEach(DecodingRecord::release);
            }
        }

        private boolean shouldSkip(StreamRecordBatch record) {
            Long expectedOffset = scanOffsets.get(record.getStreamId());
            if (expectedOffset == null || expectedOffset != record.getBaseOffset()) {
                if (expectedOffset != null && expectedOffset < record.getBaseOffset()) {
                    logger.warn("[BUG] dropping discontinuous WAL record: streamId={}, expectedOffset={}, actualOffset={}",
                        record.getStreamId(), expectedOffset, record.getBaseOffset());
                }
                return true;
            }
            return false;
        }

        private void collectDecodedRecords(Deque<DecodingRecord> batch) {
            boolean truncated = false;
            while (!batch.isEmpty()) {
                DecodingRecord entry = batch.removeFirst();
                StreamRecordBatch src = entry.source().record();
                StreamRecordBatch record = entry.decoded().handle((rst, ex) -> rst).join();
                boolean retained = false;
                try {
                    if (!truncated && entry.decoded().isCompletedExceptionally()) {
                        Throwable cause = entry.decoded().handle((rst, ex) -> ex).join();
                        logger.error("[RECOVERY_TRUNCATED] Link decode failed at WAL offset {}, streamId={}, baseOffset={}; "
                            + "discarding this record and the remaining WAL suffix", entry.source().recordOffset(),
                            src.getStreamId(), src.getBaseOffset(), cause);
                        truncated = true;
                        sourceFinished = true;
                    }
                    if (!truncated) {
                        ready.addLast(new DecodedRecord(record, entry.source().recordOffset()));
                        retained = true;
                    }
                } finally {
                    if (record != src) {
                        src.release();
                    }
                    if (!retained && record != null) {
                        record.release();
                    }
                }
            }
        }

        @Override
        public void close() {
            sourceFinished = true;
            if (next != null) {
                next.record().release();
                next = null;
            }
            while (!ready.isEmpty()) {
                StreamRecordBatch record = ready.removeFirst().record();
                if (record != null) {
                    record.release();
                }
            }
        }
    }

    private record DecodingRecord(RecoverResult source, CompletableFuture<StreamRecordBatch> decoded) {
        void release() {
            StreamRecordBatch record = decoded.handle((rst, ex) -> rst).join();
            if (record != source.record()) {
                source.record().release();
            }
            if (record != null) {
                record.release();
            }
        }
    }

    private record DecodedRecord(StreamRecordBatch record, RecordOffset recordOffset) implements RecoverResult { }

    public static void releaseAllRecords(Collection<? extends Collection<StreamRecordBatch>> allRecords) {
        allRecords.forEach(WALRecovery::releaseRecords);
    }

    private static void releaseRecords(Collection<StreamRecordBatch> records) {
        records.forEach(StreamRecordBatch::release);
    }

    @FunctionalInterface
    public interface BlockHandler {
        void handle(LogCache.LogCacheBlock cacheBlock);
    }
}
