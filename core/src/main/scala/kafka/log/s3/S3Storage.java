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

package kafka.log.s3;

import kafka.log.es.FutureUtil;
import kafka.log.s3.cache.LogCache;
import kafka.log.s3.cache.ReadDataBlock;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.wal.WriteAheadLog;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

public class S3Storage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Storage.class);
    private final KafkaConfig config;
    private final WriteAheadLog log;
    private final LogCache logCache;
    private final WALCallbackSequencer callbackSequencer = new WALCallbackSequencer();
    private final Queue<WALObjectUploadTaskContext> walObjectPrepareQueue = new LinkedList<>();
    private final Queue<WALObjectUploadTaskContext> walObjectCommitQueue = new LinkedList<>();
    private final ScheduledExecutorService mainExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3-storage-main", false));
    private final ScheduledExecutorService backgroundExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("s3-storage-main", true));
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final S3BlockCache blockCache;

    public S3Storage(KafkaConfig config, WriteAheadLog log, ObjectManager objectManager, S3BlockCache blockCache, S3Operator s3Operator) {
        this.config = config;
        this.log = log;
        this.logCache = new LogCache(config.s3WALObjectSize());
        this.objectManager = objectManager;
        this.blockCache = blockCache;
        this.s3Operator = s3Operator;
    }

    @Override
    public void close() {
        mainExecutor.shutdown();
        backgroundExecutor.shutdown();
    }

    @Override
    public CompletableFuture<Void> append(StreamRecordBatch streamRecord) {
        //TODO: copy to pooled bytebuffer to reduce gc, convert to flat record
        FlatStreamRecordBatch flatStreamRecordBatch = FlatStreamRecordBatch.from(streamRecord);
        WriteAheadLog.AppendResult appendResult = log.append(flatStreamRecordBatch.encodedBuf());
        CompletableFuture<Void> cf = new CompletableFuture<>();
        WalWriteRequest writeRequest = new WalWriteRequest(flatStreamRecordBatch, appendResult.offset, cf);
        callbackSequencer.before(writeRequest);
        appendResult.future.thenAccept(nil -> handleAppendCallback(writeRequest));
        return cf;
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset, long endOffset, int maxBytes) {
        CompletableFuture<ReadDataBlock> cf = new CompletableFuture<>();
        mainExecutor.execute(() -> FutureUtil.propagate(read0(streamId, startOffset, endOffset, maxBytes), cf));
        return cf;
    }

    private CompletableFuture<ReadDataBlock> read0(long streamId, long startOffset, long endOffset, int maxBytes) {
        List<FlatStreamRecordBatch> records = logCache.get(streamId, startOffset, endOffset, maxBytes);
        if (!records.isEmpty()) {
            return CompletableFuture.completedFuture(new ReadDataBlock(StreamRecordBatchCodec.decode(records)));
        }
        return blockCache.read(streamId, startOffset, endOffset, maxBytes).thenApply(readDataBlock -> {
            long nextStartOffset = readDataBlock.endOffset().orElse(startOffset);
            int nextMaxBytes = maxBytes - Math.min(maxBytes, readDataBlock.sizeInBytes());
            if (nextStartOffset >= endOffset || nextMaxBytes == 0) {
                return readDataBlock;
            }
            List<StreamRecordBatch> finalRecords = new LinkedList<>(readDataBlock.getRecords());
            finalRecords.addAll(StreamRecordBatchCodec.decode(logCache.get(streamId, nextStartOffset, endOffset, maxBytes)));
            return new ReadDataBlock(finalRecords);
        });
    }

    @Override
    public CompletableFuture<Void> forceUpload(long streamId) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        mainExecutor.execute(() -> {
            Optional<LogCache.LogCacheBlock> blockOpt = logCache.archiveCurrentBlockIfContains(streamId);
            if (blockOpt.isPresent()) {
                blockOpt.ifPresent(logCacheBlock -> FutureUtil.propagate(uploadWALObject(logCacheBlock), cf));
            } else {
                cf.complete(null);
            }
        });
        return cf;
    }

    private void handleAppendCallback(WalWriteRequest request) {
        mainExecutor.execute(() -> {
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
        });
    }

    /**
     * Upload cache block to S3. The earlier cache block will have smaller objectId and commit first.
     */
    CompletableFuture<Void> uploadWALObject(LogCache.LogCacheBlock logCacheBlock) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        backgroundExecutor.execute(() -> uploadWALObject0(logCacheBlock, cf));
        return cf;
    }

    private void uploadWALObject0(LogCache.LogCacheBlock logCacheBlock, CompletableFuture<Void> cf) {
        WALObjectUploadTask walObjectUploadTask = new WALObjectUploadTask(logCacheBlock.records(), objectManager, s3Operator,
                config.s3ObjectBlockSize(), config.s3ObjectPartSize(), config.s3StreamSplitSize());
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
            log.trim(context.cache.confirmOffset());
            freeCache(context.cache.blockId());
            context.cf.complete(null);

            // 2. trigger next task to commit.
            WALObjectUploadTaskContext next = walObjectCommitQueue.peek();
            if (next != null) {
                commitWALObject(next);
            }
        }, backgroundExecutor);
    }

    private void freeCache(long blockId) {
        mainExecutor.execute(() -> logCache.free(blockId));
    }

    static class WALCallbackSequencer {
        public static final long NOOP_OFFSET = -1L;
        private final Map<Long, Queue<WalWriteRequest>> stream2requests = new ConcurrentHashMap<>();
        private final BlockingQueue<WalWriteRequest> walRequests = new ArrayBlockingQueue<>(4096);
        private long walConfirmOffset = NOOP_OFFSET;

        /**
         * Add request to stream sequence queue.
         */
        public void before(WalWriteRequest request) {
            try {
                walRequests.put(request);
                Queue<WalWriteRequest> streamRequests = stream2requests.computeIfAbsent(request.record.streamId, s -> new LinkedBlockingQueue<>());
                streamRequests.add(request);
            } catch (InterruptedException ex) {
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
            long streamId = request.record.streamId;
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
            if (streamRequests.isEmpty()) {
                stream2requests.computeIfPresent(streamId, (id, requests) -> requests.isEmpty() ? null : requests);
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

    }

    static class WALObjectUploadTaskContext {
        WALObjectUploadTask task;
        LogCache.LogCacheBlock cache;
        CompletableFuture<Void> cf;
    }
}
