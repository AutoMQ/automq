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

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitWALObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.S3Operator;
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

public class WALObjectUploadTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALObjectUploadTask.class);
    private long startTimestamp;
    private final Logger s3ObjectLogger;
    private final Map<Long, List<StreamRecordBatch>> streamRecordsMap;
    private final int objectBlockSize;
    private final int objectPartSize;
    private final int streamSplitSizeThreshold;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    final boolean forceSplit;
    private final boolean s3ObjectLogEnable;
    private final CompletableFuture<Long> prepareCf = new CompletableFuture<>();
    private volatile CommitWALObjectRequest commitWALObjectRequest;
    private final CompletableFuture<CommitWALObjectRequest> uploadCf = new CompletableFuture<>();
    private final ExecutorService executor;

    public WALObjectUploadTask(Config config, Map<Long, List<StreamRecordBatch>> streamRecordsMap, ObjectManager objectManager, S3Operator s3Operator,
                               ExecutorService executor, boolean forceSplit) {
        this.s3ObjectLogger = S3ObjectLogger.logger(String.format("[WALObjectUploadTask id=%d] ", config.brokerId()));
        this.streamRecordsMap = streamRecordsMap;
        this.objectBlockSize = config.s3ObjectBlockSize();
        this.objectPartSize = config.s3ObjectPartSize();
        this.streamSplitSizeThreshold = config.s3StreamSplitSize();
        this.s3ObjectLogEnable = config.s3ObjectLogEnable();
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.forceSplit = forceSplit;
        this.executor = executor;
    }

    public static WALObjectUploadTask of(Config config, Map<Long, List<StreamRecordBatch>> streamRecordsMap, ObjectManager objectManager, S3Operator s3Operator,
                                         ExecutorService executor) {
        boolean forceSplit = streamRecordsMap.size() == 1;
        if (!forceSplit) {
            Optional<Boolean> hasWALData = streamRecordsMap.values()
                    .stream()
                    .map(records -> records.stream().mapToLong(StreamRecordBatch::size).sum() >= config.s3StreamSplitSize())
                    .filter(split -> !split)
                    .findAny();
            if (hasWALData.isEmpty()) {
                forceSplit = true;
            }
        }
        return new WALObjectUploadTask(config, streamRecordsMap, objectManager, s3Operator, executor, forceSplit);
    }

    public CompletableFuture<Long> prepare() {
        startTimestamp = System.currentTimeMillis();
        if (forceSplit) {
            prepareCf.complete(NOOP_OBJECT_ID);
        } else {
            objectManager
                    .prepareObject(1, TimeUnit.MINUTES.toMillis(30))
                    .thenAcceptAsync(prepareCf::complete, executor)
                    .exceptionally(ex -> {
                        prepareCf.completeExceptionally(ex);
                        return null;
                    });
        }
        return prepareCf;
    }

    public CompletableFuture<CommitWALObjectRequest> upload() {
        prepareCf.thenAcceptAsync(objectId -> FutureUtil.exec(() -> upload0(objectId), uploadCf, LOGGER, "upload"), executor);
        return uploadCf;
    }

    private void upload0(long objectId) {
        List<Long> streamIds = new ArrayList<>(streamRecordsMap.keySet());
        Collections.sort(streamIds);
        CommitWALObjectRequest request = new CommitWALObjectRequest();

        ObjectWriter walObject;
        if (forceSplit) {
            // when only has one stream, we only need to write the stream data.
            walObject = ObjectWriter.noop(objectId);
        } else {
            walObject = ObjectWriter.writer(objectId, s3Operator, objectBlockSize, objectPartSize);
        }

        List<CompletableFuture<Void>> streamObjectCfList = new LinkedList<>();

        for (Long streamId : streamIds) {
            List<StreamRecordBatch> streamRecords = streamRecordsMap.get(streamId);
            long streamSize = streamRecords.stream().mapToLong(StreamRecordBatch::size).sum();
            if (forceSplit || streamSize >= streamSplitSizeThreshold) {
                streamObjectCfList.add(writeStreamObject(streamRecords).thenAccept(so -> {
                    synchronized (request) {
                        request.addStreamObject(so);
                    }
                }));
            } else {
                walObject.write(streamId, streamRecords);
                long startOffset = streamRecords.get(0).getBaseOffset();
                long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
                request.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset));
            }
        }
        request.setObjectId(objectId);
        request.setOrderId(objectId);
        CompletableFuture<Void> walObjectCf = walObject.close().thenAccept(nil -> request.setObjectSize(walObject.size()));
        List<CompletableFuture<?>> allCf = new LinkedList<>(streamObjectCfList);
        allCf.add(walObjectCf);
        CompletableFuture.allOf(allCf.toArray(new CompletableFuture[0])).thenAccept(nil -> {
            commitWALObjectRequest = request;
            uploadCf.complete(request);
        }).exceptionally(ex -> {
            uploadCf.completeExceptionally(ex);
            return null;
        });
    }

    public CompletableFuture<Void> commit() {
        return uploadCf.thenCompose(request -> objectManager.commitWALObject(request).thenApply(resp -> {
            LOGGER.info("Commit WAL object {}, cost {}ms", commitWALObjectRequest, System.currentTimeMillis() - startTimestamp);
            if (s3ObjectLogEnable) {
                s3ObjectLogger.trace("{}", commitWALObjectRequest);
            }
            return null;
        }));
    }

    private CompletableFuture<StreamObject> writeStreamObject(List<StreamRecordBatch> streamRecords) {
        CompletableFuture<Long> objectIdCf = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30));
        return objectIdCf.thenComposeAsync(objectId -> {
            ObjectWriter streamObjectWriter = ObjectWriter.writer(objectId, s3Operator, objectBlockSize, objectPartSize);
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
                return streamObject;
            });
        }, executor);
    }
}
