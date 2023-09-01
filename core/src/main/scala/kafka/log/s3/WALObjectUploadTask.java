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

import kafka.log.s3.objects.CommitCompactObjectRequest;
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

public class WALObjectUploadTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(WALObjectUploadTask.class);
    private final Map<Long, List<FlatStreamRecordBatch>> streamRecordsMap;
    private final int streamSplitSizeThreshold;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final CompletableFuture<Long> prepareCf = new CompletableFuture<>();
    private final CompletableFuture<CommitCompactObjectRequest> uploadCf = new CompletableFuture<>();

    public WALObjectUploadTask(Map<Long, List<FlatStreamRecordBatch>> streamRecordsMap, int streamSplitSizeThreshold, ObjectManager objectManager, S3Operator s3Operator) {
        this.streamRecordsMap = streamRecordsMap;
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

    public CompletableFuture<CommitCompactObjectRequest> upload() {
        prepareCf.thenAccept(objectId -> {
            List<Long> streamIds = new ArrayList<>(streamRecordsMap.keySet());
            Collections.sort(streamIds);
            CommitCompactObjectRequest compactRequest = new CommitCompactObjectRequest();
            compactRequest.setCompactedObjectIds(Collections.emptyList());

            ObjectWriter minorCompactObject = new ObjectWriter(objectId, s3Operator);

            List<CompletableFuture<StreamObject>> streamObjectCfList = new LinkedList<>();

            for (Long streamId : streamIds) {
                List<FlatStreamRecordBatch> streamRecords = streamRecordsMap.get(streamId);
                long streamSize = streamRecords.stream().mapToLong(r -> r.encodedBuf.readableBytes()).sum();
                if (streamSize >= streamSplitSizeThreshold) {
                    streamObjectCfList.add(writeStreamObject(streamRecords));
                } else {
                    for (FlatStreamRecordBatch record : streamRecords) {
                        minorCompactObject.write(record);
                    }
                    long startOffset = streamRecords.get(0).baseOffset;
                    long endOffset = streamRecords.get(streamRecords.size() - 1).lastOffset();
                    compactRequest.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset));
                    // minor compact object block only contain single stream's data.
                    minorCompactObject.closeCurrentBlock();
                }
            }
            compactRequest.setObjectId(objectId);
            CompletableFuture<Void> minorCompactObjectCf = minorCompactObject.close().thenAccept(nil -> {
                compactRequest.setObjectSize(minorCompactObject.size());
            });
            compactRequest.setObjectSize(minorCompactObject.size());
            for (CompletableFuture<StreamObject> streamObjectCf : streamObjectCfList) {
                streamObjectCf.thenAccept(compactRequest::addStreamObject);
            }
            List<CompletableFuture<?>> allCf = new LinkedList<>(streamObjectCfList);
            allCf.add(minorCompactObjectCf);

            CompletableFuture.allOf(allCf.toArray(new CompletableFuture[0])).thenAccept(nil -> {
                uploadCf.complete(compactRequest);
            }).exceptionally(ex -> {
                uploadCf.completeExceptionally(ex);
                return null;
            });
        });
        return uploadCf;
    }

    public CompletableFuture<Void> commit() {
        return uploadCf.thenCompose(objectManager::commitMinorCompactObject);
    }

    private CompletableFuture<StreamObject> writeStreamObject(List<FlatStreamRecordBatch> streamRecords) {
        CompletableFuture<Long> objectIdCf = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30));
        // TODO: retry until success
        return objectIdCf.thenCompose(objectId -> {
            ObjectWriter streamObjectWriter = new ObjectWriter(objectId, s3Operator);
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
