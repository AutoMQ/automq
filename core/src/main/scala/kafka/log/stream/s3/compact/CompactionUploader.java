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

package kafka.log.stream.s3.compact;

import kafka.log.stream.s3.objects.ObjectManager;
import kafka.log.stream.s3.objects.StreamObject;
import kafka.log.stream.s3.operator.S3Operator;
import kafka.log.stream.utils.Threads;
import kafka.log.stream.s3.compact.objects.CompactedObject;
import kafka.log.stream.s3.compact.objects.CompactionType;
import kafka.log.stream.s3.compact.objects.StreamDataBlock;
import kafka.log.stream.s3.compact.operator.DataBlockWriter;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionUploader {
    private final static Logger LOGGER = LoggerFactory.getLogger(CompactionUploader.class);
    private final ObjectManager objectManager;
//    private final TokenBucketThrottle throttle;
    private final ExecutorService streamObjectUploadPool;
    private final ExecutorService walObjectUploadPool;
    private final S3Operator s3Operator;
    private final KafkaConfig kafkaConfig;
    private CompletableFuture<Long> walObjectIdCf = null;
    private DataBlockWriter walObjectWriter = null;

    // TODO: add network outbound throttle
    public CompactionUploader(ObjectManager objectManager, S3Operator s3Operator, KafkaConfig kafkaConfig) {
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.kafkaConfig = kafkaConfig;
//        this.throttle = new TokenBucketThrottle(kafkaConfig.s3ObjectCompactionNWOutBandwidth());
        this.streamObjectUploadPool = Threads.newFixedThreadPool(kafkaConfig.s3ObjectCompactionUploadConcurrency(),
                ThreadUtils.createThreadFactory("compaction-stream-object-uploader-%d", true), LOGGER);
        this.walObjectUploadPool = Threads.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("compaction-wal-object-uploader-%d", true), LOGGER);
    }

    public void stop() {
        this.walObjectUploadPool.shutdown();
        this.streamObjectUploadPool.shutdown();
//        this.throttle.stop();
    }

    public CompletableFuture<Void> chainWriteWALObject(CompletableFuture<Void> prev, CompactedObject compactedObject) {
        if (compactedObject.type() != CompactionType.COMPACT) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong compacted object type, expected COMPACT"));
        }
        if (prev == null) {
            return CompletableFuture.allOf(compactedObject.streamDataBlocks()
                            .stream()
                            .map(StreamDataBlock::getDataCf)
                            .toArray(CompletableFuture[]::new))
                    .thenComposeAsync(v -> prepareObjectAndWrite(compactedObject), walObjectUploadPool);
        }
        return prev.thenComposeAsync(v ->
                CompletableFuture.allOf(compactedObject.streamDataBlocks()
                        .stream()
                        .map(StreamDataBlock::getDataCf)
                        .toArray(CompletableFuture[]::new))
                .thenComposeAsync(vv -> prepareObjectAndWrite(compactedObject), walObjectUploadPool));
    }

    private CompletableFuture<Void> prepareObjectAndWrite(CompactedObject compactedObject) {
        if (walObjectIdCf == null) {
            walObjectIdCf = this.objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30));
        }
        return walObjectIdCf.thenAcceptAsync(objectId -> {
            if (walObjectWriter == null) {
                walObjectWriter = new DataBlockWriter(objectId, s3Operator, kafkaConfig.s3ObjectPartSize());
            }
            for (StreamDataBlock streamDataBlock : compactedObject.streamDataBlocks()) {
                walObjectWriter.write(streamDataBlock);
            }
        }, streamObjectUploadPool).exceptionally(ex -> {
            LOGGER.error("prepare and write wal object failed", ex);
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
                .thenComposeAsync(v -> objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30))
                                .thenComposeAsync(objectId -> {
                                    DataBlockWriter dataBlockWriter = new DataBlockWriter(objectId, s3Operator, kafkaConfig.s3ObjectPartSize());
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

    public CompletableFuture<Void> forceUploadWAL() {
        if (walObjectWriter == null) {
            return CompletableFuture.completedFuture(null);
        }
        return walObjectWriter.forceUpload();
    }

    public long completeWAL() {
        if (walObjectWriter == null) {
            return 0L;
        }
        walObjectWriter.close().join();
        return walObjectWriter.size();
    }

    public void reset() {
        walObjectIdCf = null;
        walObjectWriter = null;
    }

    public long getWALObjectId() {
        if (walObjectIdCf == null) {
            return -1;
        }
        return walObjectIdCf.getNow(-1L);
    }
}
