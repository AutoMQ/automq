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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

public class DefaultUploadWriteAheadLogTask implements UploadWriteAheadLogTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultUploadWriteAheadLogTask.class);
    private final Logger s3ObjectLogger;
    private final Map<Long, List<StreamRecordBatch>> streamObjectMap;
    private final Map<Long, List<StreamRecordBatch>> streamSetObjectMap;
    private final int objectBlockSize;
    private final int objectPartSize;
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

    public DefaultUploadWriteAheadLogTask(Config config, Map<Long, List<StreamRecordBatch>> streamSetObjectMap,
                                          Map<Long, List<StreamRecordBatch>> streamObjectMap,
                                          ObjectManager objectManager, ObjectStorage objectStorage,
                                          ExecutorService executor, double rate) {
        this.s3ObjectLogger = S3ObjectLogger.logger(String.format("[DeltaWALUploadTask id=%d] ", config.nodeId()));
        this.streamSetObjectMap = streamSetObjectMap;
        this.streamObjectMap = streamObjectMap;
        this.objectBlockSize = config.objectBlockSize();
        this.objectPartSize = config.objectPartSize();
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
        this.executor = executor;
        this.rate = rate;
        this.limiter = new AsyncRateLimiter(rate);
    }

    public static Builder builder() {
        return new Builder();
    }

    // test only
    Map<Long, List<StreamRecordBatch>> getStreamObjectMap() {
        return streamObjectMap;
    }

    // test only
    Map<Long, List<StreamRecordBatch>> getStreamSetObjectMap() {
        return streamSetObjectMap;
    }

    int objectCount() {
        return streamObjectMap.size() + (streamSetObjectMap.isEmpty() ? 0 : 1);
    }

    @Override
    public CompletableFuture<Long> prepare() {
        startTimestamp = System.currentTimeMillis();
        objectManager
            .prepareObject(objectCount(), TimeUnit.MINUTES.toMillis(60))
            .thenAcceptAsync(prepareCf::complete, executor)
            .exceptionally(ex -> {
                prepareCf.completeExceptionally(ex);
                return null;
            });
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
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        ObjectWriter ssoWriter;
        long streamSetObjectId = NOOP_OBJECT_ID;
        if (streamSetObjectMap.isEmpty()) {
            ssoWriter = ObjectWriter.noop(objectId);
        } else {
            streamSetObjectId = objectId++;
            ssoWriter = ObjectWriter.writer(streamSetObjectId, objectStorage, objectBlockSize, objectPartSize);
        }

        List<CompletableFuture<Void>> streamObjectCfList = new LinkedList<>();
        List<Long> streamObjectIds = new ArrayList<>(streamObjectMap.keySet());
        Collections.sort(streamObjectIds);
        for (Long streamId : streamObjectIds) {
            List<StreamRecordBatch> streamRecords = streamObjectMap.get(streamId);
            streamObjectCfList.add(writeStreamObject(objectId++, streamRecords).thenAccept(so -> {
                synchronized (request) {
                    request.addStreamObject(so);
                }
            }));
        }

        List<CompletableFuture<Void>> streamSetObjectCfList = new LinkedList<>();
        List<Long> streamSetIds = new ArrayList<>(streamSetObjectMap.keySet());
        Collections.sort(streamSetIds);
        for (Long streamId : streamSetIds) {
            List<StreamRecordBatch> streamRecords = streamSetObjectMap.get(streamId);
            int streamSize = streamSize(streamRecords);
            long startOffset = streamRecords.get(0).getBaseOffset();
            long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
            streamSetObjectCfList.add(acquireLimiter(streamSize).thenAccept(nil -> ssoWriter.write(streamId, streamRecords)));
            request.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset, streamSize));
        }

        request.setObjectId(streamSetObjectId);
        request.setOrderId(streamSetObjectId);

        CompletableFuture<Void> streamSetObjectCf = CompletableFuture.allOf(streamSetObjectCfList.toArray(new CompletableFuture[0]))
            .thenCompose(nil -> ssoWriter.close().thenAccept(nil2 -> {
                request.setObjectSize(ssoWriter.size());
                request.setAttributes(ObjectAttributes.builder().bucket(ssoWriter.bucketId()).build().attributes());
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

    private CompletableFuture<StreamObject> writeStreamObject(long objectId, List<StreamRecordBatch> streamRecords) {
        int streamSize = streamSize(streamRecords);
        return acquireLimiter(streamSize).thenComposeAsync(nil -> {
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
            return streamObjectWriter.close().thenApply(ignored -> {
                streamObject.setObjectSize(streamObjectWriter.size());
                streamObject.setAttributes(ObjectAttributes.builder().bucket(streamObjectWriter.bucketId()).build().attributes());
                return streamObject;
            });
        }, executor);
    }

    static int streamSize(List<StreamRecordBatch> streamRecords) {
        return streamRecords.stream().mapToInt(StreamRecordBatch::size).sum();
    }

    public static class Builder {
        private Config config;
        private Map<Long, List<StreamRecordBatch>> streamRecordsMap;
        private ObjectManager objectManager;
        private ObjectStorage objectStorage;
        private ExecutorService executor;
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

        public Builder rate(double rate) {
            this.rate = rate;
            return this;
        }

        public DefaultUploadWriteAheadLogTask build() {
            Map<Long, List<StreamRecordBatch>> streamSetObjectMap = new HashMap<>();
            Map<Long, List<StreamRecordBatch>> streamObjectMap = new HashMap<>();
            // when only has one stream, we only need to write the stream data.
            if (streamRecordsMap.size() == 1) {
                streamObjectMap.putAll(streamRecordsMap);
                return new DefaultUploadWriteAheadLogTask(config, streamSetObjectMap, streamObjectMap,
                    objectManager, objectStorage, executor, rate);
            }

            List<Long> streamIds = new ArrayList<>(streamRecordsMap.keySet());
            for (Long streamId : streamIds) {
                List<StreamRecordBatch> streamRecords = streamRecordsMap.get(streamId);
                if (streamSize(streamRecords) >= config.streamSplitSize()) {
                    streamObjectMap.put(streamId, streamRecords);
                } else {
                    streamSetObjectMap.put(streamId, streamRecords);
                }
            }
            return new DefaultUploadWriteAheadLogTask(config, streamSetObjectMap, streamObjectMap,
                objectManager, objectStorage, executor, rate);
        }
    }

}
