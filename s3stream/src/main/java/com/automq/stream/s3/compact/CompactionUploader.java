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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.s3.compact.operator.DataBlockWriter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionUploader {
    private final static Logger LOGGER = LoggerFactory.getLogger(CompactionUploader.class);
    private final ObjectManager objectManager;
    private final ExecutorService streamObjectUploadPool;
    private final ExecutorService streamSetObjectUploadPool;
    private final S3Operator s3Operator;
    private final Config config;
    private CompletableFuture<Long> streamSetObjectIdCf = null;
    private DataBlockWriter streamSetObjectWriter = null;

    public CompactionUploader(ObjectManager objectManager, S3Operator s3Operator, Config config) {
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.config = config;
        this.streamObjectUploadPool = Threads.newFixedThreadPool(config.streamSetObjectCompactionUploadConcurrency(),
                ThreadUtils.createThreadFactory("compaction-stream-object-uploader-%d", true), LOGGER);
        this.streamSetObjectUploadPool = Threads.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("compaction-stream-set-object-uploader-%d", true), LOGGER);
    }

    public void stop() {
        this.streamSetObjectUploadPool.shutdown();
        this.streamObjectUploadPool.shutdown();
    }

    public CompletableFuture<Void> chainWriteStreamSetObject(CompletableFuture<Void> prev, CompactedObject compactedObject) {
        if (compactedObject.type() != CompactionType.COMPACT) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong compacted object type, expected COMPACT"));
        }
        if (prev == null) {
            return CompletableFuture.allOf(compactedObject.streamDataBlocks()
                            .stream()
                            .map(StreamDataBlock::getDataCf)
                            .toArray(CompletableFuture[]::new))
                    .thenComposeAsync(v -> prepareObjectAndWrite(compactedObject), streamSetObjectUploadPool);
        }
        return prev.thenComposeAsync(v ->
                CompletableFuture.allOf(compactedObject.streamDataBlocks()
                        .stream()
                        .map(StreamDataBlock::getDataCf)
                        .toArray(CompletableFuture[]::new))
                .thenComposeAsync(vv -> prepareObjectAndWrite(compactedObject), streamSetObjectUploadPool));
    }

    private CompletableFuture<Void> prepareObjectAndWrite(CompactedObject compactedObject) {
        if (streamSetObjectIdCf == null) {
            streamSetObjectIdCf = this.objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(CompactionConstants.S3_OBJECT_TTL_MINUTES));
        }
        return streamSetObjectIdCf.thenAcceptAsync(objectId -> {
            if (streamSetObjectWriter == null) {
                streamSetObjectWriter = new DataBlockWriter(objectId, s3Operator, config.objectPartSize());
            }
            for (StreamDataBlock streamDataBlock : compactedObject.streamDataBlocks()) {
                streamSetObjectWriter.write(streamDataBlock);
            }
        }, streamObjectUploadPool).exceptionally(ex -> {
            LOGGER.error("prepare and write stream set object failed", ex);
            return null;
        });
    }

    public CompletableFuture<StreamObject> writeStreamObject(CompactedObject compactedObject) {
        if (compactedObject.type() != CompactionType.SPLIT) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong compacted object type, expected SPLIT"));
        }
        return CompletableFuture.allOf(compactedObject.streamDataBlocks()
                        .stream()
                        .map(StreamDataBlock::getDataCf)
                        .toArray(CompletableFuture[]::new))
                .thenComposeAsync(v -> objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(CompactionConstants.S3_OBJECT_TTL_MINUTES))
                                .thenComposeAsync(objectId -> {
                                    DataBlockWriter dataBlockWriter = new DataBlockWriter(objectId, s3Operator, config.objectPartSize());
                                    for (StreamDataBlock streamDataBlock : compactedObject.streamDataBlocks()) {
                                        dataBlockWriter.write(streamDataBlock);
                                    }
                                    long streamId = compactedObject.streamDataBlocks().get(0).getStreamId();
                                    long startOffset = compactedObject.streamDataBlocks().get(0).getStartOffset();
                                    long endOffset = compactedObject.streamDataBlocks().get(compactedObject.streamDataBlocks().size() - 1).getEndOffset();
                                    StreamObject streamObject = new StreamObject();
                                    streamObject.setObjectId(objectId);
                                    streamObject.setStreamId(streamId);
                                    streamObject.setStartOffset(startOffset);
                                    streamObject.setEndOffset(endOffset);
                                    return dataBlockWriter.close().thenApply(nil -> {
                                        streamObject.setObjectSize(dataBlockWriter.size());
                                        return streamObject;
                                    });
                                }, streamObjectUploadPool),
                        streamObjectUploadPool)
                .exceptionally(ex -> {
                    LOGGER.error("stream object write failed", ex);
                    return null;
                });
    }

    public CompletableFuture<Void> forceUploadStreamSetObject() {
        if (streamSetObjectWriter == null) {
            return CompletableFuture.completedFuture(null);
        }
        return streamSetObjectWriter.forceUpload();
    }

    public long complete() {
        if (streamSetObjectWriter == null) {
            return 0L;
        }
        streamSetObjectWriter.close().join();
        return streamSetObjectWriter.size();
    }

    public void reset() {
        streamSetObjectIdCf = null;
        streamSetObjectWriter = null;
    }

    public long getStreamSetObjectId() {
        if (streamSetObjectIdCf == null) {
            return -1;
        }
        return streamSetObjectIdCf.getNow(-1L);
    }
}
