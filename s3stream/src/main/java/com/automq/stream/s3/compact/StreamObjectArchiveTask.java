/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.compact;

import com.automq.stream.api.Stream;
import com.automq.stream.s3.CompositeObjectReader;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.CompositeObject.OBJECT_BLOCK_HEADER_SIZE;
import static com.automq.stream.s3.CompositeObject.OBJECT_UNIT_SIZE;
import static com.automq.stream.s3.objects.ObjectAttributes.Type.Composite;

/**
 * Owns one bounded Broker ARCHIVE round: recover or prepare a range, copy its Composite manifests, and publish it.
 * Source metadata and linked objects remain untouched for Controller cleanup in a later lifecycle stage.
 */
public final class StreamObjectArchiveTask {
    static final int MAX_COMPOSITES_PER_BATCH = 100;
    static final long COMPOSITE_TARGET_SIZE = 512L * 1024 * 1024;
    static final long TASK_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);

    private final ObjectManager objectManager;
    private final StreamManager streamManager;
    private final ObjectStorage objectStorage;
    private final Stream stream;
    private final LongSupplier nanoTime;
    private final long taskTimeoutNanos;

    private StreamObjectArchiveTask(Builder builder) {
        objectManager = Objects.requireNonNull(builder.objectManager, "objectManager");
        streamManager = Objects.requireNonNull(builder.streamManager, "streamManager");
        objectStorage = Objects.requireNonNull(builder.objectStorage, "objectStorage");
        stream = Objects.requireNonNull(builder.stream, "stream");
        nanoTime = builder.nanoTime;
        taskTimeoutNanos = builder.taskTimeoutNanos;
    }

    /**
     * Create a task builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Run one ARCHIVE round. A prepared range is always recovered before selecting new work.
     */
    public void archive() throws ExecutionException, InterruptedException, TimeoutException {
        long deadlineNanos = nanoTime.getAsLong() + taskTimeoutNanos;
        StreamArchiveState state = await(streamManager.getStreamArchive(stream.streamId(), stream.streamEpoch()),
            deadlineNanos);
        if (state.archivePreparedEndOffset() > state.archiveEndOffset()) {
            List<S3ObjectMetadata> prepared = getRange(state.archiveEndOffset(), state.archivePreparedEndOffset(),
                deadlineNanos);
            copyAndPublish(state, prepared, deadlineNanos);
            return;
        }
        List<S3ObjectMetadata> online = null;
        if (state.archiveEndOffset() < stream.startOffset()) {
            if (!isEmptyAndMetadataCaughtUp(state)) {
                return;
            }
            online = getRange(stream.startOffset(), stream.confirmOffset(), deadlineNanos);
            long newCursor = online.stream()
                .filter(object -> object.endOffset() > stream.startOffset())
                .mapToLong(S3ObjectMetadata::startOffset)
                .filter(offset -> offset >= state.archiveEndOffset())
                .min()
                .orElse(stream.startOffset());
            if (newCursor > state.archiveEndOffset()) {
                StreamArchiveState advanced = new StreamArchiveState(state.streamId(), state.streamEpoch(),
                    newCursor, newCursor, newCursor, newCursor, 0L, newCursor, 0L, List.of());
                await(streamManager.updateStreamArchive(advanced), deadlineNanos);
                return;
            }
        }

        if (online == null) {
            online = getRange(state.archiveEndOffset(), stream.confirmOffset(), deadlineNanos);
        }
        List<ArchiveComposite> parsed = parseConsecutiveComposites(online, state.archiveEndOffset(), deadlineNanos);
        List<ArchiveComposite> selected = selectTerminal(parsed, stream.startOffset());
        if (selected.isEmpty()) {
            return;
        }
        List<S3ObjectMetadata> objects = selected.stream().map(ArchiveComposite::metadata).toList();
        long preparedEndOffset = objects.get(objects.size() - 1).endOffset();
        StreamArchiveState preparedState = new StreamArchiveState(state.streamId(), state.streamEpoch(),
            state.archiveStartOffset(), state.archiveMetadataEndOffset(), state.archiveEndOffset(), preparedEndOffset,
            state.archiveSize(), state.archiveCleanupEndOffset(), state.archiveCleanupSize(),
            objects.stream().map(S3ObjectMetadata::objectId).toList());
        await(streamManager.updateStreamArchive(preparedState), deadlineNanos);
        copyAndPublish(preparedState, objects, deadlineNanos);
    }

    private List<S3ObjectMetadata> getRange(long startOffset, long endOffset, long deadlineNanos)
        throws ExecutionException, InterruptedException, TimeoutException {
        return await(objectManager.getStreamObjects(stream.streamId(), startOffset, endOffset, Integer.MAX_VALUE),
            deadlineNanos);
    }

    private List<ArchiveComposite> parseConsecutiveComposites(List<S3ObjectMetadata> objects, long cursor,
        long deadlineNanos) throws ExecutionException, InterruptedException, TimeoutException {
        List<ArchiveComposite> parsed = new ArrayList<>();
        long nextOffset = cursor;
        for (S3ObjectMetadata object : objects) {
            if (parsed.size() > MAX_COMPOSITES_PER_BATCH || object.startOffset() != nextOffset
                || ObjectAttributes.from(object.attributes()).type() != Composite) {
                break;
            }
            parsed.add(new ArchiveComposite(object, readManifestInfo(object, deadlineNanos)));
            nextOffset = object.endOffset();
        }
        return parsed;
    }

    private static List<ArchiveComposite> selectTerminal(List<ArchiveComposite> composites, long streamStartOffset) {
        List<ArchiveComposite> selected = new ArrayList<>();
        int limit = Math.min(MAX_COMPOSITES_PER_BATCH, composites.size());
        for (int i = 0; i < limit; i++) {
            ArchiveComposite current = composites.get(i);
            boolean retentionBoundaryTerminal = current.metadata().startOffset() < streamStartOffset
                && current.metadata().endOffset() > streamStartOffset;
            boolean retainedSizeTerminal = current.manifestInfo().logicalSize() >= COMPOSITE_TARGET_SIZE;
            boolean nextMergeExceedsTarget = i + 1 < composites.size()
                && composites.get(i + 1).manifestInfo().logicalSize()
                > COMPOSITE_TARGET_SIZE - current.manifestInfo().logicalSize();
            boolean nextMergeExceedsOffsetDelta = i + 1 < composites.size()
                && composites.get(i + 1).metadata().endOffset() - current.metadata().startOffset()
                > Integer.MAX_VALUE;
            boolean nextMergeExceedsPartCount = i + 1 < composites.size()
                && partCount(current.manifestInfo().logicalSize())
                + partCount(composites.get(i + 1).manifestInfo().logicalSize()) > Writer.MAX_PART_COUNT;
            boolean nextMergeExceedsFormat = i + 1 < composites.size()
                && exceedsCompositeFormatLimits(current.manifestInfo(), composites.get(i + 1).manifestInfo());
            if (!retentionBoundaryTerminal && !retainedSizeTerminal && !nextMergeExceedsTarget
                && !nextMergeExceedsOffsetDelta
                && !nextMergeExceedsPartCount && !nextMergeExceedsFormat) {
                break;
            }
            selected.add(current);
        }
        return selected;
    }

    private static boolean isEmptyAndMetadataCaughtUp(StreamArchiveState state) {
        return state.archiveStartOffset() == state.archiveMetadataEndOffset()
            && state.archiveMetadataEndOffset() == state.archiveEndOffset()
            && state.archiveEndOffset() == state.archivePreparedEndOffset()
            && state.archiveSize() == 0 && state.archiveCleanupSize() == 0;
    }

    private void copyAndPublish(StreamArchiveState preparedState, List<S3ObjectMetadata> objects, long deadlineNanos)
        throws ExecutionException, InterruptedException, TimeoutException {
        if (objects.isEmpty() || objects.get(0).startOffset() != preparedState.archiveEndOffset()
            || objects.get(objects.size() - 1).endOffset() != preparedState.archivePreparedEndOffset()) {
            return;
        }
        List<CompletableFuture<Long>> copies = objects.stream()
            .map(object -> copyManifest(object, deadlineNanos))
            .toList();
        await(CompletableFuture.allOf(copies.toArray(CompletableFuture[]::new)), deadlineNanos);
        long batchLogicalSize = 0L;
        for (CompletableFuture<Long> copy : copies) {
            batchLogicalSize = Math.addExact(batchLogicalSize, copy.join());
        }
        StreamArchiveState published = new StreamArchiveState(preparedState.streamId(), preparedState.streamEpoch(),
            preparedState.archiveStartOffset(), preparedState.archiveMetadataEndOffset(),
            preparedState.archivePreparedEndOffset(), preparedState.archivePreparedEndOffset(),
            Math.addExact(preparedState.archiveSize(), batchLogicalSize), preparedState.archiveCleanupEndOffset(),
            preparedState.archiveCleanupSize(), List.of());
        await(streamManager.updateStreamArchive(published), deadlineNanos);
    }

    private CompletableFuture<Long> copyManifest(S3ObjectMetadata object, long deadlineNanos) {
        ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions().bucket(object.bucket());
        return objectStorage.rangeRead(readOptions, object.key(), 0L, ObjectStorage.RANGE_READ_TO_END)
            .thenCompose(manifest -> logicalSize(manifest, object).handle((logicalSize, exception) -> {
                if (exception != null) {
                    manifest.release();
                    throw new java.util.concurrent.CompletionException(exception);
                }
                return logicalSize;
            }).thenCompose(logicalSize -> {
                long remainingMillis = remainingMillis(deadlineNanos);
                if (remainingMillis <= 0) {
                    manifest.release();
                    return CompletableFuture.failedFuture(new TimeoutException("ARCHIVE task deadline expired"));
                }
                String archiveKey = ArchiveObjectKey.manifestKey(stream.streamId(), object.startOffset(),
                    object.endOffset(), object.objectId(), logicalSize);
                return objectStorage.write(new ObjectStorage.WriteOptions().timeout(remainingMillis), archiveKey,
                    manifest).thenApply(ignored -> logicalSize);
            }));
    }

    private CompositeManifestInfo readManifestInfo(S3ObjectMetadata object, long deadlineNanos)
        throws ExecutionException, InterruptedException, TimeoutException {
        ByteBuf manifest = await(objectStorage.rangeRead(new ObjectStorage.ReadOptions().bucket(object.bucket()),
            object.key(), 0L, ObjectStorage.RANGE_READ_TO_END), deadlineNanos);
        try {
            return await(manifestInfo(manifest, object), deadlineNanos);
        } finally {
            manifest.release();
        }
    }

    private static CompletableFuture<Long> logicalSize(ByteBuf manifest, S3ObjectMetadata object) {
        return manifestInfo(manifest, object).thenApply(CompositeManifestInfo::logicalSize);
    }

    private static CompletableFuture<CompositeManifestInfo> manifestInfo(ByteBuf manifest,
        S3ObjectMetadata object) {
        CompositeObjectReader reader = new CompositeObjectReader(object,
            (readOptions, metadata, start, end) -> CompletableFuture.completedFuture(manifest.retainedDuplicate()));
        return reader.basicObjectInfo().thenApply(info -> {
            CompositeObjectReader.BasicObjectInfoExt compositeInfo =
                (CompositeObjectReader.BasicObjectInfoExt) info;
            long logicalSize = info.indexBlock().indexes().stream().mapToLong(DataBlockIndex::size).sum();
            return new CompositeManifestInfo(logicalSize, info.indexBlock().count(),
                compositeInfo.objectsBlock().indexes().size(), 0);
        }).whenComplete((ignored, exception) -> reader.close());
    }

    private static long partCount(long size) {
        return size / Writer.MAX_PART_SIZE + (size % Writer.MAX_PART_SIZE == 0 ? 0 : 1);
    }

    static boolean exceedsCompositeFormatLimits(CompositeManifestInfo current,
        CompositeManifestInfo next) {
        if (current.formatVersion() != next.formatVersion()) {
            return true;
        }
        long indexCount = current.dataBlockIndexCount() + next.dataBlockIndexCount();
        long linkedObjectCount = current.linkedObjectCount() + next.linkedObjectCount();
        return indexCount > Integer.MAX_VALUE / DataBlockIndex.BLOCK_INDEX_SIZE
            || linkedObjectCount > (Integer.MAX_VALUE - OBJECT_BLOCK_HEADER_SIZE) / OBJECT_UNIT_SIZE;
    }

    private <T> T await(CompletableFuture<T> future, long deadlineNanos)
        throws ExecutionException, InterruptedException, TimeoutException {
        long remainingNanos = deadlineNanos - nanoTime.getAsLong();
        if (remainingNanos <= 0) {
            throw new TimeoutException("ARCHIVE task deadline expired");
        }
        return future.get(remainingNanos, TimeUnit.NANOSECONDS);
    }

    private long remainingMillis(long deadlineNanos) {
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - nanoTime.getAsLong()));
    }

    private record ArchiveComposite(S3ObjectMetadata metadata, CompositeManifestInfo manifestInfo) {
    }

    record CompositeManifestInfo(long logicalSize, long dataBlockIndexCount, long linkedObjectCount,
                                 int formatVersion) {
    }

    /**
     * Builder for one Stream-owned ARCHIVE task.
     */
    public static final class Builder {
        private ObjectManager objectManager;
        private StreamManager streamManager;
        private ObjectStorage objectStorage;
        private Stream stream;
        private LongSupplier nanoTime = System::nanoTime;
        private long taskTimeoutNanos = TASK_TIMEOUT_NANOS;

        private Builder() {
        }

        /**
         * Set the online object metadata registry.
         */
        public Builder objectManager(ObjectManager objectManager) {
            this.objectManager = objectManager;
            return this;
        }

        /**
         * Set the Stream Archive state manager.
         */
        public Builder streamManager(StreamManager streamManager) {
            this.streamManager = streamManager;
            return this;
        }

        /**
         * Set the object-storage boundary used for manifest reads and writes.
         */
        public Builder objectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
            return this;
        }

        /**
         * Set the currently owned Stream.
         */
        public Builder stream(Stream stream) {
            this.stream = stream;
            return this;
        }

        Builder nanoTime(LongSupplier nanoTime) {
            this.nanoTime = nanoTime;
            return this;
        }

        Builder taskTimeoutNanos(long taskTimeoutNanos) {
            this.taskTimeoutNanos = taskTimeoutNanos;
            return this;
        }

        /**
         * Build one bounded ARCHIVE task.
         */
        public StreamObjectArchiveTask build() {
            return new StreamObjectArchiveTask(this);
        }
    }
}
