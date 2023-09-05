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

import kafka.log.s3.objects.CommitWALObjectRequest;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.S3Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static kafka.log.es.FutureUtil.exec;

public class WALObjectUploadTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALObjectUploadTask.class);
    private final Map<Long, List<FlatStreamRecordBatch>> streamRecordsMap;
    private final int objectBlockSize;
    private final int objectPartSize;
    private final int streamSplitSizeThreshold;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final CompletableFuture<Long> prepareCf = new CompletableFuture<>();
    private volatile CommitWALObjectRequest commitWALObjectRequest;
    private final CompletableFuture<CommitWALObjectRequest> uploadCf = new CompletableFuture<>();

    public WALObjectUploadTask(Map<Long, List<FlatStreamRecordBatch>> streamRecordsMap, ObjectManager objectManager, S3Operator s3Operator,
                               int objectBlockSize, int objectPartSize, int streamSplitSizeThreshold) {
        this.streamRecordsMap = streamRecordsMap;
        this.objectBlockSize = objectBlockSize;
        this.objectPartSize = objectPartSize;
        this.streamSplitSizeThreshold = streamSplitSizeThreshold;
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    public CompletableFuture<Long> prepare() {
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(prepareCf::complete).exceptionally(ex -> {
            prepareCf.completeExceptionally(ex);
            return null;
        });
        // TODO: retry when fail or prepareObject inner retry
        return prepareCf;
    }

    public CompletableFuture<CommitWALObjectRequest> upload() {
        prepareCf.thenAccept(objectId -> exec(() -> upload0(objectId), uploadCf, LOGGER, "upload"));
        return uploadCf;
    }

    private void upload0(long objectId) {
        List<Long> streamIds = new ArrayList<>(streamRecordsMap.keySet());
        Collections.sort(streamIds);
        CommitWALObjectRequest request = new CommitWALObjectRequest();

        ObjectWriter walObject = new ObjectWriter(objectId, s3Operator, objectBlockSize, objectPartSize);

        List<CompletableFuture<StreamObject>> streamObjectCfList = new LinkedList<>();

        for (Long streamId : streamIds) {
            List<FlatStreamRecordBatch> streamRecords = streamRecordsMap.get(streamId);
            long streamSize = streamRecords.stream().mapToLong(FlatStreamRecordBatch::size).sum();
            if (streamSize >= streamSplitSizeThreshold) {
                streamObjectCfList.add(writeStreamObject(streamRecords));
            } else {
                for (FlatStreamRecordBatch record : streamRecords) {
                    walObject.write(record);
                }
                long startOffset = streamRecords.get(0).baseOffset;
                long endOffset = streamRecords.get(streamRecords.size() - 1).lastOffset();
                request.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset));
                // log object block only contain single stream's data.
                walObject.closeCurrentBlock();
            }
        }
        request.setObjectId(objectId);
        request.setOrderId(objectId);
        CompletableFuture<Void> walObjectCf = walObject.close().thenAccept(nil -> request.setObjectSize(walObject.size()));
        for (CompletableFuture<StreamObject> streamObjectCf : streamObjectCfList) {
            streamObjectCf.thenAccept(request::addStreamObject);
        }
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
            LOGGER.debug("Commit WAL object {}", commitWALObjectRequest);
            return null;
        }));
    }

    private CompletableFuture<StreamObject> writeStreamObject(List<FlatStreamRecordBatch> streamRecords) {
        CompletableFuture<Long> objectIdCf = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30));
        // TODO: retry until success
        return objectIdCf.thenCompose(objectId -> {
            ObjectWriter streamObjectWriter = new ObjectWriter(objectId, s3Operator, objectBlockSize, objectPartSize);
            for (FlatStreamRecordBatch record : streamRecords) {
                streamObjectWriter.write(record);
            }
            long streamId = streamRecords.get(0).streamId;
            long startOffset = streamRecords.get(0).baseOffset;
            long endOffset = streamRecords.get(streamRecords.size() - 1).lastOffset();
            StreamObject streamObject = new StreamObject();
            streamObject.setObjectId(objectId);
            streamObject.setStreamId(streamId);
            streamObject.setStartOffset(startOffset);
            streamObject.setEndOffset(endOffset);
            return streamObjectWriter.close().thenApply(nil -> {
                streamObject.setObjectSize(streamObjectWriter.size());
                return streamObject;
            });
        });
    }
}
