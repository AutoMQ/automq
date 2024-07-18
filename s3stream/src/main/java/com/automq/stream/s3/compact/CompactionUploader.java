/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.compact;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactionType;
import com.automq.stream.s3.compact.operator.DataBlockWriter;
import com.automq.stream.s3.compact.utils.CompactionUtils;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionUploader {
    private final static Logger LOGGER = LoggerFactory.getLogger(CompactionUploader.class);
    private final ObjectManager objectManager;
    private final ExecutorService streamObjectUploadPool;
    private final ExecutorService streamSetObjectUploadPool;
    private final ObjectStorage objectStorage;
    private final Config config;
    private CompletableFuture<Long> streamSetObjectIdCf = null;
    private DataBlockWriter streamSetObjectWriter = null;
    private volatile boolean isAborted = false;
    private volatile boolean isShutdown = false;
    private volatile short bucketId;

    public CompactionUploader(ObjectManager objectManager, ObjectStorage objectStorage, Config config) {
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
        this.config = config;
        this.streamObjectUploadPool = Threads.newFixedThreadPool(config.streamSetObjectCompactionUploadConcurrency(),
            ThreadUtils.createThreadFactory("compaction-stream-object-uploader-%d", true), LOGGER);
        this.streamSetObjectUploadPool = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("compaction-stream-set-object-uploader-%d", true), LOGGER);
    }

    public void shutdown() {
        this.isShutdown = true;
        this.streamSetObjectUploadPool.shutdown();
        try {
            if (!this.streamSetObjectUploadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                this.streamSetObjectUploadPool.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }

        this.streamObjectUploadPool.shutdown();
        try {
            if (!this.streamObjectUploadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                this.streamObjectUploadPool.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }
    }

    public CompletableFuture<Void> chainWriteStreamSetObject(CompletableFuture<Void> prev,
        CompactedObject compactedObject) {
        if (compactedObject.type() != CompactionType.COMPACT) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong compacted object type, expected COMPACT"));
        }
        if (compactedObject.streamDataBlocks().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (prev == null) {
            return prepareObjectAndWrite(compactedObject);
        }
        return prev.thenCompose(v -> prepareObjectAndWrite(compactedObject));
    }

    private CompletableFuture<Void> prepareObjectAndWrite(CompactedObject compactedObject) {
        if (streamSetObjectIdCf == null) {
            streamSetObjectIdCf = this.objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(CompactionConstants.S3_OBJECT_TTL_MINUTES));
        }
        return streamSetObjectIdCf.thenComposeAsync(objectId -> {
            if (streamSetObjectWriter == null) {
                streamSetObjectWriter = new DataBlockWriter(objectId, objectStorage, config.objectPartSize());
            }
            return CompactionUtils.chainWriteDataBlock(streamSetObjectWriter, compactedObject.streamDataBlocks(), streamSetObjectUploadPool);
        }, streamSetObjectUploadPool);
    }

    public CompletableFuture<StreamObject> writeStreamObject(CompactedObject compactedObject) {
        if (compactedObject.type() != CompactionType.SPLIT) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("wrong compacted object type, expected SPLIT"));
        }
        if (compactedObject.streamDataBlocks().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(CompactionConstants.S3_OBJECT_TTL_MINUTES))
            .thenComposeAsync(objectId -> {
                if (isAborted) {
                    // release data that has not been uploaded
                    compactedObject.streamDataBlocks().forEach(StreamDataBlock::release);
                    return CompletableFuture.completedFuture(null);
                }
                DataBlockWriter dataBlockWriter = new DataBlockWriter(objectId, objectStorage, config.objectPartSize());
                CompletableFuture<Void> cf = CompactionUtils.chainWriteDataBlock(dataBlockWriter, compactedObject.streamDataBlocks(), streamObjectUploadPool);
                return cf.thenCompose(nil -> dataBlockWriter.close()).thenApply(nil -> {
                    StreamObject streamObject = new StreamObject();
                    streamObject.setObjectId(objectId);
                    streamObject.setStreamId(compactedObject.streamDataBlocks().get(0).getStreamId());
                    streamObject.setStartOffset(compactedObject.streamDataBlocks().get(0).getStartOffset());
                    streamObject.setEndOffset(compactedObject.streamDataBlocks().get(compactedObject.streamDataBlocks().size() - 1).getEndOffset());
                    streamObject.setObjectSize(dataBlockWriter.size());
                    streamObject.setAttributes(ObjectAttributes.builder().bucket(dataBlockWriter.bucketId()).build().attributes());
                    return streamObject;
                }).whenComplete((ret, ex) -> {
                    if (ex != null) {
                        if (!isShutdown) {
                            LOGGER.error("write to stream object {} failed", objectId, ex);
                        }
                        dataBlockWriter.release();
                        compactedObject.streamDataBlocks().forEach(StreamDataBlock::release);
                    }
                });
            }, streamObjectUploadPool);
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
        bucketId = streamSetObjectWriter.bucketId();
        long writeSize = streamSetObjectWriter.size();
        reset();
        return writeSize;
    }

    public CompletableFuture<Void> release() {
        isAborted = true;
        CompletableFuture<Void> cf = CompletableFuture.completedFuture(null);
        if (streamSetObjectWriter != null) {
            cf = streamSetObjectWriter.release();
        }
        return cf.thenAccept(nil -> reset());
    }

    private void reset() {
        streamSetObjectIdCf = null;
        streamSetObjectWriter = null;
        isAborted = false;
    }

    public long getStreamSetObjectId() {
        if (streamSetObjectIdCf == null) {
            return -1;
        }
        return streamSetObjectIdCf.getNow(-1L);
    }

    public short bucketId() {
        return bucketId;
    }
}
