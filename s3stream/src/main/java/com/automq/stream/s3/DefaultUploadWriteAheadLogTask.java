/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.AsyncRateLimiter;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

public class DefaultUploadWriteAheadLogTask implements UploadWriteAheadLogTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultUploadWriteAheadLogTask.class);
    final boolean forceSplit;
    private final Logger s3ObjectLogger;
    private final Map<Long, List<StreamRecordBatch>> streamRecordsMap;
    private final int objectBlockSize;
    private final int objectPartSize;
    private final int streamSplitSizeThreshold;
    private final ObjectManager objectManager;
    private final ObjectStorage objectStorage;
    private final CompletableFuture<Long> prepareCf = new CompletableFuture<>();
    private final CompletableFuture<CommitStreamSetObjectRequest> uploadCf = new CompletableFuture<>();
    private final ExecutorService executor;
    private final double rate;
    private final AsyncRateLimiter limiter;
    private long startTimestamp;
    private long uploadTimestamp;
    private long commitTimestamp;
    private volatile CommitStreamSetObjectRequest commitStreamSetObjectRequest;
    private volatile boolean burst = false;

    public DefaultUploadWriteAheadLogTask(Config config, Map<Long, List<StreamRecordBatch>> streamRecordsMap,
                              ObjectManager objectManager, ObjectStorage objectStorage,
                              ExecutorService executor, boolean forceSplit, double rate) {
        this.s3ObjectLogger = S3ObjectLogger.logger(String.format("[DeltaWALUploadTask id=%d] ", config.nodeId()));
        this.streamRecordsMap = streamRecordsMap;
        this.objectBlockSize = config.objectBlockSize();
        this.objectPartSize = config.objectPartSize();
        this.streamSplitSizeThreshold = config.streamSplitSize();
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
        this.forceSplit = forceSplit;
        this.executor = executor;
        this.rate = rate;
        this.limiter = new AsyncRateLimiter(rate);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public CompletableFuture<Long> prepare() {
        startTimestamp = System.currentTimeMillis();
        if (forceSplit) {
            prepareCf.complete(NOOP_OBJECT_ID);
        } else {
            objectManager
                .prepareObject(1, TimeUnit.MINUTES.toMillis(60))
                .thenAcceptAsync(prepareCf::complete, executor)
                .exceptionally(ex -> {
                    prepareCf.completeExceptionally(ex);
                    return null;
                });
        }
        return prepareCf;
    }

    /**
     * bypass the uploadTask rateLimit to make the task finish as fast as possible.
     */
    @Override
    public void burst() {
        if (this.burst) {
            return;
        }

        synchronized (this) {
            if (!this.burst) {
                this.limiter.burst();
                this.burst = true;
            }
        }
    }

    private CompletableFuture<Void> acquireLimiter(int size) {
        return limiter.acquire(size);
    }

    @Override
    public CompletableFuture<CommitStreamSetObjectRequest> upload() {
        prepareCf.thenAcceptAsync(objectId -> FutureUtil.exec(() -> upload0(objectId), uploadCf, LOGGER, "upload"), executor);
        return uploadCf;
    }

    void upload0(long objectId) {
        uploadTimestamp = System.currentTimeMillis();
        List<Long> streamIds = new ArrayList<>(streamRecordsMap.keySet());
        Collections.sort(streamIds);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        ObjectWriter streamSetObject;
        if (forceSplit) {
            // when only has one stream, we only need to write the stream data.
            streamSetObject = ObjectWriter.noop(objectId);
        } else {
            streamSetObject = ObjectWriter.writer(objectId, objectStorage, objectBlockSize, objectPartSize);
        }

        List<CompletableFuture<Void>> streamObjectCfList = new LinkedList<>();

        List<CompletableFuture<Void>> streamSetWriteCfList = new LinkedList<>();
        for (Long streamId : streamIds) {
            List<StreamRecordBatch> streamRecords = streamRecordsMap.get(streamId);
            int streamSize = streamRecords.stream().mapToInt(StreamRecordBatch::size).sum();
            if (forceSplit || streamSize >= streamSplitSizeThreshold) {
                streamObjectCfList.add(writeStreamObject(streamRecords, streamSize).thenAccept(so -> {
                    synchronized (request) {
                        request.addStreamObject(so);
                    }
                }));
            } else {
                streamSetWriteCfList.add(acquireLimiter(streamSize).thenAccept(nil -> streamSetObject.write(streamId, streamRecords)));
                long startOffset = streamRecords.get(0).getBaseOffset();
                long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
                request.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset, streamSize));
            }
        }
        request.setObjectId(objectId);
        request.setOrderId(objectId);
        CompletableFuture<Void> streamSetObjectCf = CompletableFuture.allOf(streamSetWriteCfList.toArray(new CompletableFuture[0]))
            .thenCompose(nil -> streamSetObject.close().thenAccept(nil2 -> {
                request.setObjectSize(streamSetObject.size());
                request.setAttributes(ObjectAttributes.builder().bucket(streamSetObject.bucketId()).build().attributes());
            }));
        List<CompletableFuture<?>> allCf = new LinkedList<>(streamObjectCfList);
        allCf.add(streamSetObjectCf);
        CompletableFuture.allOf(allCf.toArray(new CompletableFuture[0])).thenAccept(nil -> {
            commitStreamSetObjectRequest = request;
            uploadCf.complete(request);
        }).exceptionally(ex -> {
            uploadCf.completeExceptionally(ex);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> commit() {
        return uploadCf.thenCompose(request -> {
            commitTimestamp = System.currentTimeMillis();
            return objectManager.commitStreamSetObject(request).thenAccept(resp -> {
                long now = System.currentTimeMillis();
                long streamSetObjectSize = request.getObjectSize();
                long streamObjectsSize = request.getStreamObjects().stream().map(StreamObject::getObjectSize).reduce(0L, Long::sum);
                long totalSize = streamSetObjectSize + streamObjectsSize;
                LOGGER.info("Upload delta WAL finished, cost {}ms, prepare {}ms, upload {}ms, commit {}ms, rate limiter {}bytes/s; object id: {}, object size: {}bytes, stream ranges count: {}, size: {}bytes, stream objects count: {}, size: {}bytes",
                    now - startTimestamp,
                    uploadTimestamp - startTimestamp,
                    commitTimestamp - uploadTimestamp,
                    now - commitTimestamp,
                    (int) rate,
                    request.getObjectId(),
                    totalSize,
                    request.getStreamRanges().size(),
                    streamSetObjectSize,
                    request.getStreamObjects().size(),
                    streamObjectsSize
                );
                s3ObjectLogger.info("[UPLOAD_WAL] {}", request);
            }).whenComplete((nil, ex) -> limiter.close());
        });
    }

    private CompletableFuture<StreamObject> writeStreamObject(List<StreamRecordBatch> streamRecords, int streamSize) {
        CompletableFuture<Long> cf = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(60));
        cf = cf.thenCompose(objectId -> acquireLimiter(streamSize).thenApply(nil -> objectId));
        return cf.thenComposeAsync(objectId -> {
            ObjectWriter streamObjectWriter = ObjectWriter.writer(objectId, objectStorage, objectBlockSize, objectPartSize);
            long streamId = streamRecords.get(0).getStreamId();
            streamObjectWriter.write(streamId, streamRecords);
            long startOffset = streamRecords.get(0).getBaseOffset();
            long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
            StreamObject streamObject = new StreamObject();
            streamObject.setObjectId(objectId);
            streamObject.setStreamId(streamId);
            streamObject.setStartOffset(startOffset);
            streamObject.setEndOffset(endOffset);
            return streamObjectWriter.close().thenApply(nil -> {
                streamObject.setObjectSize(streamObjectWriter.size());
                streamObject.setAttributes(ObjectAttributes.builder().bucket(streamObjectWriter.bucketId()).build().attributes());
                return streamObject;
            });
        }, executor);
    }

    public static class Builder {
        private Config config;
        private Map<Long, List<StreamRecordBatch>> streamRecordsMap;
        private ObjectManager objectManager;
        private ObjectStorage objectStorage;
        private ExecutorService executor;
        private Boolean forceSplit;
        private double rate = Long.MAX_VALUE;

        public Builder config(Config config) {
            this.config = config;
            return this;
        }

        public Builder streamRecordsMap(Map<Long, List<StreamRecordBatch>> streamRecordsMap) {
            this.streamRecordsMap = streamRecordsMap;
            return this;
        }

        public Builder objectManager(ObjectManager objectManager) {
            this.objectManager = objectManager;
            return this;
        }

        public Builder objectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
            return this;
        }

        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public Builder forceSplit(boolean forceSplit) {
            this.forceSplit = forceSplit;
            return this;
        }

        public Builder rate(double rate) {
            this.rate = rate;
            return this;
        }

        public DefaultUploadWriteAheadLogTask build() {
            if (forceSplit == null) {
                boolean forceSplit = streamRecordsMap.size() == 1;
                if (!forceSplit) {
                    Optional<Boolean> hasStreamSetData = streamRecordsMap.values()
                        .stream()
                        .map(records -> records.stream().mapToLong(StreamRecordBatch::size).sum() >= config.streamSplitSize())
                        .filter(split -> !split)
                        .findAny();
                    if (hasStreamSetData.isEmpty()) {
                        forceSplit = true;
                    }
                }
                this.forceSplit = forceSplit;
            }
            return new DefaultUploadWriteAheadLogTask(config, streamRecordsMap, objectManager, objectStorage, executor, forceSplit, rate);
        }
    }

}
