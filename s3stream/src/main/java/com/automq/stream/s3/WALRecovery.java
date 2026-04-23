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

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.automq.stream.s3.ByteBufAlloc.DECODE_RECORD;

/**
 * WAL recovery logic: reads records from the WAL iterator, groups them into
 * {@link LogCache.LogCacheBlock}s, filters invalid streams, decodes link records,
 * and passes each block to a handler for upload.
 */
public class WALRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALRecovery.class);
    private static final ByteBufSeqAlloc DECODE_LINK_RECORD_INSTANT_ALLOC = new ByteBufSeqAlloc(DECODE_RECORD, 1);

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
     * Records are processed in blocks. When a block is full (size threshold or offset overflow),
     * it is passed to the {@code blockHandler} for upload, and a new block is started.
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
        boolean first = true;
        RecoverResult pending = null;
        while (pending != null || it.hasNext()) {
            LogCache.LogCacheBlock cacheBlock = new LogCache.LogCacheBlock(maxCacheSize);
            RecordOffset blockLastOffset = null;
            try {
                if (pending != null) {
                    cacheBlock.put(pending.record());
                    openingStreamEndOffsets.put(pending.record().getStreamId(), pending.record().getLastOffset());
                    blockLastOffset = pending.recordOffset();
                    pending = null;
                }
                while (it.hasNext() && !cacheBlock.isFull()) {
                    RecoverResult recoverResult = it.next();
                    if (first) {
                        logger.info("recover start offset {}", recoverResult.recordOffset());
                        first = false;
                    }
                    if (!processRecord(recoverResult.record(), openingStreamEndOffsets, cacheBlock, logger)) {
                        pending = recoverResult;
                    } else {
                        blockLastOffset = recoverResult.recordOffset();
                    }
                }
            } catch (Throwable e) {
                releaseAllRecords(cacheBlock.records().values());
                if (pending != null) {
                    pending.record().release();
                }
                throw e;
            }
            if (blockLastOffset != null) {
                cacheBlock.lastRecordOffset(blockLastOffset);
            }
            decodeLinkRecords(cacheBlock);
            blockHandler.handle(cacheBlock);
        }
    }

    /**
     * @return true if the record was added or dropped, false if the block is full/overflow and the record was NOT added
     */
    private static boolean processRecord(
        StreamRecordBatch streamRecordBatch,
        Map<Long, Long> openingStreamEndOffsets,
        LogCache.LogCacheBlock cacheBlock,
        Logger logger
    ) {
        long streamId = streamRecordBatch.getStreamId();

        Long expectedNextOffset = openingStreamEndOffsets.get(streamId);
        if (expectedNextOffset == null) {
            // stream is already safe closed, skip it
            streamRecordBatch.release();
            return true;
        }
        if (expectedNextOffset > streamRecordBatch.getBaseOffset()) {
            // the record has been committed, skip it
            streamRecordBatch.release();
            return true;
        }
        if (expectedNextOffset < streamRecordBatch.getBaseOffset()) {
            // discontinuous record, drop it
            logger.warn("[BUG] dropping discontinuous WAL record: streamId={}, expectedOffset={}, actualOffset={}",
                streamId, expectedNextOffset, streamRecordBatch.getBaseOffset());
            streamRecordBatch.release();
            return true;
        }

        if (!cacheBlock.put(streamRecordBatch)) {
            return false;
        }
        openingStreamEndOffsets.put(streamId, streamRecordBatch.getLastOffset());
        return true;
    }

    private static void decodeLinkRecords(LogCache.LogCacheBlock cacheBlock) {
        int size = 0;
        for (List<StreamRecordBatch> l : cacheBlock.records().values()) {
            size += l.size();
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(size);
        for (Map.Entry<Long, List<StreamRecordBatch>> entry : cacheBlock.records().entrySet()) {
            List<StreamRecordBatch> records = entry.getValue();
            for (int i = 0; i < records.size(); i++) {
                StreamRecordBatch record = records.get(i);
                if (record.getCount() >= 0) {
                    continue;
                }
                int finalI = i;
                futures.add(S3Storage.getLinkRecordDecoder().decode(record, DECODE_LINK_RECORD_INSTANT_ALLOC).thenAccept(r -> {
                    records.set(finalI, r);
                }));
            }
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Throwable ex) {
            releaseAllRecords(cacheBlock.records().values());
            throw new RuntimeException(ex);
        }
    }

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
