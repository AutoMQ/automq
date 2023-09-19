/*
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

import com.automq.stream.s3.cache.LogCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.metadata.stream.StreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;


public class S3Storage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Storage.class);
    private final long maxWALCacheSize;
    private final Config config;
    private final WriteAheadLog log;
    private final LogCache logCache;
    private final WALCallbackSequencer callbackSequencer = new WALCallbackSequencer();
    private final Queue<WALObjectUploadTaskContext> walObjectPrepareQueue = new LinkedList<>();
    private final Queue<WALObjectUploadTaskContext> walObjectCommitQueue = new LinkedList<>();
    private CompletableFuture<Void> lastForceUploadCf = CompletableFuture.completedFuture(null);
    private final ScheduledExecutorService mainExecutor = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3-storage-main", false), LOGGER);
    private final ScheduledExecutorService backgroundExecutor = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3-storage-background", true), LOGGER);
    private final ScheduledExecutorService uploadWALExecutor = Threads.newFixedThreadPool(
            4, ThreadUtils.createThreadFactory("s3-storage-upload-wal", true), LOGGER);

    private final StreamManager streamManager;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final S3BlockCache blockCache;

    public S3Storage(Config config, WriteAheadLog log, StreamManager streamManager, ObjectManager objectManager,
                     S3BlockCache blockCache, S3Operator s3Operator) {
        this.config = config;
        this.maxWALCacheSize = config.s3WALCacheSize();
        this.log = log;
        this.blockCache = blockCache;
        this.logCache = new LogCache(config.s3WALObjectSize(), block -> blockCache.put(block.records()));
        this.streamManager = streamManager;
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    @Override
    public void startup() {
        try {
            LOGGER.info("S3Storage starting");
            recover();
            LOGGER.info("S3Storage start completed");
        } catch (Throwable e) {
            LOGGER.error("S3Storage start fail", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Upload WAL to S3 and close opening streams.
     */
    private void recover() throws Throwable {
        log.start();
        List<StreamMetadata> streams = streamManager.getOpeningStreams().get();

        LogCache.LogCacheBlock cacheBlock = recoverContinuousRecords(log.recover(), streams);
        Map<Long, Long> streamEndOffsets = new HashMap<>();
        cacheBlock.records().forEach((streamId, records) -> {
            if (!records.isEmpty()) {
                streamEndOffsets.put(streamId, records.get(records.size() - 1).getLastOffset());
            }
        });

        if (cacheBlock.size() != 0) {
            LOGGER.info("try recover from crash, recover records bytes size {}", cacheBlock.size());
            uploadWALObject(cacheBlock).get();
            cacheBlock.records().forEach((streamId, records) -> records.forEach(StreamRecordBatch::release));
        }
        log.reset().get();
        for (StreamMetadata stream : streams) {
            long newEndOffset = streamEndOffsets.getOrDefault(stream.getStreamId(), stream.getEndOffset());
            LOGGER.info("recover try close stream {} with new end offset {}", stream, newEndOffset);
        }
        CompletableFuture.allOf(
                streams
                        .stream()
                        .map(s -> streamManager.closeStream(s.getStreamId(), s.getEpoch()))
                        .toArray(CompletableFuture[]::new)
        ).get();
    }

    LogCache.LogCacheBlock recoverContinuousRecords(Iterator<WriteAheadLog.RecoverResult> it, List<StreamMetadata> openingStreams) {
        Map<Long, Long> openingStreamEndOffsets = openingStreams.stream().collect(Collectors.toMap(StreamMetadata::getStreamId, StreamMetadata::getEndOffset));
        LogCache.LogCacheBlock cacheBlock = new LogCache.LogCacheBlock(1024L * 1024 * 1024);
        long logEndOffset = -1L;
        Map<Long, Long> streamNextOffsets = new HashMap<>();
        while (it.hasNext()) {
            WriteAheadLog.RecoverResult recoverResult = it.next();
            logEndOffset = recoverResult.recordOffset();
            ByteBuf recordBuf = Unpooled.wrappedBuffer(recoverResult.record());
            StreamRecordBatch streamRecordBatch = StreamRecordBatchCodec.decode(recordBuf);
            long streamId = streamRecordBatch.getStreamId();
            Long openingStreamEndOffset = openingStreamEndOffsets.get(streamId);
            if (openingStreamEndOffset == null) {
                // stream is already safe closed. so skip the stream records.
                continue;
            }
            if (streamRecordBatch.getBaseOffset() < openingStreamEndOffset) {
                // filter committed records.
                continue;
            }
            Long expectNextOffset = streamNextOffsets.get(streamId);
            if (expectNextOffset == null || expectNextOffset == streamRecordBatch.getBaseOffset()) {
                cacheBlock.put(streamRecordBatch);
                streamNextOffsets.put(streamRecordBatch.getStreamId(), streamRecordBatch.getLastOffset());
            }
        }
        if (logEndOffset >= 0L) {
            cacheBlock.confirmOffset(logEndOffset);
        }
        cacheBlock.records().forEach((streamId, records) -> {
            if (!records.isEmpty()) {
                long startOffset = records.get(0).getBaseOffset();
                long expectedStartOffset = openingStreamEndOffsets.getOrDefault(streamId, startOffset);
                if (startOffset > expectedStartOffset) {
                    throw new IllegalStateException(String.format("[BUG] WAL data may lost, streamId %s endOffset=%s from controller" +
                            "but WAL recovered records startOffset=%s", streamId, expectedStartOffset, startOffset));
                }
            }

        });

        return cacheBlock;
    }

    @Override
    public void shutdown() {
        log.shutdownGracefully();
        mainExecutor.shutdown();
        backgroundExecutor.shutdown();
    }

    @Override
    public CompletableFuture<Void> append(StreamRecordBatch streamRecord) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        acquirePermit();
        WriteAheadLog.AppendResult appendResult;
        try {
            // fast path
            appendResult = log.append(streamRecord.encoded());
        } catch (WriteAheadLog.OverCapacityException e) {
            // slow path
            for (; ; ) {
                // TODO: check close.
                try {
                    appendResult = log.append(streamRecord.encoded());
                    break;
                } catch (WriteAheadLog.OverCapacityException e2) {
                    LOGGER.warn("log over capacity", e);
                    Threads.sleep(100);
                }
            }
        }
        WalWriteRequest writeRequest = new WalWriteRequest(streamRecord, appendResult.recordOffset(), cf);
        handleAppendRequest(writeRequest);
        appendResult.future().thenAccept(nil -> handleAppendCallback(writeRequest));
        return cf;
    }

    private void acquirePermit() {
        for (; ; ) {
            if (logCache.size() < maxWALCacheSize) {
                break;
            } else {
                // TODO: log limit
                LOGGER.warn("log cache size {} is larger than {}, wait 100ms", logCache.size(), maxWALCacheSize);
                try {
                    //noinspection BusyWait
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        CompletableFuture<ReadDataBlock> cf = new CompletableFuture<>();
        mainExecutor.execute(() -> FutureUtil.propagate(read0(streamId, startOffset, endOffset, maxBytes), cf));
        return cf;
    }

    private CompletableFuture<ReadDataBlock> read0(long streamId, long startOffset, long endOffset, int maxBytes) {
        List<StreamRecordBatch> records = logCache.get(streamId, startOffset, endOffset, maxBytes);
        if (!records.isEmpty() && records.get(0).getBaseOffset() <= startOffset) {
            return CompletableFuture.completedFuture(new ReadDataBlock(records));
        }
        if (!records.isEmpty()) {
            endOffset = records.get(0).getBaseOffset();
        }
        return blockCache.read(streamId, startOffset, endOffset, maxBytes).thenApplyAsync(readDataBlock -> {
            List<StreamRecordBatch> rst = new ArrayList<>(readDataBlock.getRecords());
            int remainingBytesSize = maxBytes - rst.stream().mapToInt(StreamRecordBatch::size).sum();
            for (int i = 0; i < records.size() && remainingBytesSize > 0; i++) {
                StreamRecordBatch record = records.get(i);
                rst.add(record);
                remainingBytesSize -= record.size();
            }
            continuousCheck(records);
            return new ReadDataBlock(rst);
        }, mainExecutor);
    }

    private void continuousCheck(List<StreamRecordBatch> records) {
        long expectStartOffset = -1L;
        for (StreamRecordBatch record : records) {
            if (expectStartOffset == -1L || record.getBaseOffset() == expectStartOffset) {
                expectStartOffset = record.getLastOffset();
            } else {
                throw new IllegalArgumentException("Continuous check fail" + records);
            }
        }
    }

    /**
     * Force upload stream WAL cache to S3. Use sequence&group upload to avoid generate too many S3 objects when broker shutdown.
     */
    @Override
    public synchronized CompletableFuture<Void> forceUpload(long streamId) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        lastForceUploadCf.whenComplete((nil, ex) -> mainExecutor.execute(() -> {
            logCache.setConfirmOffset(callbackSequencer.getWALConfirmOffset());
            Optional<LogCache.LogCacheBlock> blockOpt = logCache.archiveCurrentBlockIfContains(streamId);
            if (blockOpt.isPresent()) {
                blockOpt.ifPresent(logCacheBlock -> FutureUtil.propagate(uploadWALObject(logCacheBlock), cf));
            } else {
                cf.complete(null);
            }
            callbackSequencer.tryFree(streamId);
        }));
        this.lastForceUploadCf = cf;
        return cf;
    }

    private void handleAppendRequest(WalWriteRequest request) {
        mainExecutor.execute(() -> callbackSequencer.before(request));
    }

    private void handleAppendCallback(WalWriteRequest request) {
        mainExecutor.execute(() -> handleAppendCallback0(request));
    }

    private void handleAppendCallback0(WalWriteRequest request) {
        List<WalWriteRequest> waitingAckRequests = callbackSequencer.after(request);
        for (WalWriteRequest waitingAckRequest : waitingAckRequests) {
            if (logCache.put(waitingAckRequest.record)) {
                // cache block is full, trigger WAL object upload.
                logCache.setConfirmOffset(callbackSequencer.getWALConfirmOffset());
                LogCache.LogCacheBlock logCacheBlock = logCache.archiveCurrentBlock();
                uploadWALObject(logCacheBlock);
            }
            waitingAckRequest.cf.complete(null);
        }
    }

    /**
     * Upload cache block to S3. The earlier cache block will have smaller objectId and commit first.
     */
    CompletableFuture<Void> uploadWALObject(LogCache.LogCacheBlock logCacheBlock) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        backgroundExecutor.execute(() -> FutureUtil.exec(() -> uploadWALObject0(logCacheBlock, cf), cf, LOGGER, "uploadWALObject"));
        return cf;
    }

    private void uploadWALObject0(LogCache.LogCacheBlock logCacheBlock, CompletableFuture<Void> cf) {
        WALObjectUploadTask walObjectUploadTask = new WALObjectUploadTask(logCacheBlock.records(), objectManager, s3Operator,
                config.s3ObjectBlockSize(), config.s3ObjectPartSize(), config.s3StreamSplitSize(), uploadWALExecutor);
        WALObjectUploadTaskContext context = new WALObjectUploadTaskContext();
        context.task = walObjectUploadTask;
        context.cache = logCacheBlock;
        context.cf = cf;

        boolean walObjectPrepareQueueEmpty = walObjectPrepareQueue.isEmpty();
        walObjectPrepareQueue.add(context);
        if (!walObjectPrepareQueueEmpty) {
            // there is another WAL object upload task is preparing, just return.
            return;
        }
        prepareWALObject(context);
    }

    private void prepareWALObject(WALObjectUploadTaskContext context) {
        context.task.prepare().thenAcceptAsync(nil -> {
            // 1. poll out current task and trigger upload.
            WALObjectUploadTaskContext peek = walObjectPrepareQueue.poll();
            Objects.requireNonNull(peek).task.upload();
            // 2. add task to commit queue.
            boolean walObjectCommitQueueEmpty = walObjectCommitQueue.isEmpty();
            walObjectCommitQueue.add(peek);
            if (walObjectCommitQueueEmpty) {
                commitWALObject(peek);
            }
            // 3. trigger next task to prepare.
            WALObjectUploadTaskContext next = walObjectPrepareQueue.peek();
            if (next != null) {
                prepareWALObject(next);
            }
        }, backgroundExecutor);
    }

    private void commitWALObject(WALObjectUploadTaskContext context) {
        context.task.commit().thenAcceptAsync(nil -> {
            // 1. poll out current task
            walObjectCommitQueue.poll();
            if (context.cache.confirmOffset() != 0) {
                log.trim(context.cache.confirmOffset());
            }
            // transfer records ownership to block cache.
            freeCache(context.cache);
            context.cf.complete(null);

            // 2. trigger next task to commit.
            WALObjectUploadTaskContext next = walObjectCommitQueue.peek();
            if (next != null) {
                commitWALObject(next);
            }
        }, backgroundExecutor).exceptionally(ex -> {
            LOGGER.error("Unexpected exception when commit WAL object", ex);
            context.cf.completeExceptionally(ex);
            return null;
        });
    }

    private void freeCache(LogCache.LogCacheBlock cacheBlock) {
        mainExecutor.execute(() -> logCache.markFree(cacheBlock));
    }

    /**
     * WALCallbackSequencer is modified in single thread mainExecutor.
     */
    static class WALCallbackSequencer {
        public static final long NOOP_OFFSET = -1L;
        private final Map<Long, Queue<WalWriteRequest>> stream2requests = new HashMap<>();
        private final BlockingQueue<WalWriteRequest> walRequests = new ArrayBlockingQueue<>(4096);
        private long walConfirmOffset = NOOP_OFFSET;

        /**
         * Add request to stream sequence queue.
         */
        public void before(WalWriteRequest request) {
            try {
                walRequests.put(request);
                Queue<WalWriteRequest> streamRequests = stream2requests.computeIfAbsent(request.record.getStreamId(), s -> new LinkedBlockingQueue<>());
                streamRequests.add(request);
            } catch (Throwable ex) {
                request.cf.completeExceptionally(ex);
            }
        }

        /**
         * Try pop sequence persisted request from stream queue and move forward wal inclusive confirm offset.
         *
         * @return popped sequence persisted request.
         */
        public List<WalWriteRequest> after(WalWriteRequest request) {
            request.persisted = true;
            // move the WAL inclusive confirm offset.
            for (; ; ) {
                WalWriteRequest peek = walRequests.peek();
                if (peek == null || !peek.persisted) {
                    break;
                }
                walRequests.poll();
                walConfirmOffset = peek.offset;
            }

            // pop sequence success stream request.
            long streamId = request.record.getStreamId();
            Queue<WalWriteRequest> streamRequests = stream2requests.get(streamId);
            WalWriteRequest peek = streamRequests.peek();
            if (peek == null || peek.offset != request.offset) {
                return Collections.emptyList();
            }
            List<WalWriteRequest> rst = new ArrayList<>();
            rst.add(streamRequests.poll());
            for (; ; ) {
                peek = streamRequests.peek();
                if (peek == null || !peek.persisted) {
                    break;
                }
                rst.add(streamRequests.poll());
            }
            return rst;
        }

        /**
         * Get WAL inclusive confirm offset.
         *
         * @return inclusive confirm offset.
         */
        public long getWALConfirmOffset() {
            return walConfirmOffset;
        }

        /**
         * Try free stream related resources.
         */
        public void tryFree(long streamId) {
            Queue<?> queue = stream2requests.get(streamId);
            if (queue != null && queue.isEmpty()) {
                stream2requests.remove(streamId, queue);
            }
        }
    }

    static class WALObjectUploadTaskContext {
        WALObjectUploadTask task;
        LogCache.LogCacheBlock cache;
        CompletableFuture<Void> cf;
    }
}
