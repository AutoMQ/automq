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
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamArchiveOperation;
import com.automq.stream.s3.streams.StreamArchivePhase;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.utils.Systems;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Owns one bounded Broker Archive round for Normal and Composite Stream Objects.
 *
 * <p>Archive first submits the selected object IDs for validation and durably advances
 * {@code archivePreparedEndOffset}, then copies that frozen range, and finally advances {@code archiveEndOffset}
 * while accounting its object size. A prepared range is therefore durable retry state: a later round re-enumerates
 * and recopies the whole range idempotently before publishing it.</p>
 */
public final class StreamObjectArchiveTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamObjectArchiveTask.class);
    @VisibleForTesting
    static final int MAX_OBJECTS_PER_BATCH = 100;
    @VisibleForTesting
    static final int MAX_OBJECTS_WITH_LOOKAHEAD = MAX_OBJECTS_PER_BATCH + 1;
    static final long COMPOSITE_TARGET_SIZE = Systems.getEnvLong(
        "AUTOMQ_STREAM_ARCHIVE_COMPOSITE_TARGET_SIZE", 512L * 1024 * 1024);
    private final ObjectManager objectManager;
    private final StreamManager streamManager;
    private final ObjectStorage objectStorage;
    private final Stream stream;
    private final LongSupplier currentTimeMillis;
    private final Pressure pressure;

    private StreamObjectArchiveTask(Builder builder) {
        objectManager = Objects.requireNonNull(builder.objectManager, "objectManager");
        streamManager = Objects.requireNonNull(builder.streamManager, "streamManager");
        objectStorage = Objects.requireNonNull(builder.objectStorage, "objectStorage");
        stream = Objects.requireNonNull(builder.stream, "stream");
        currentTimeMillis = builder.currentTimeMillis;
        pressure = builder.pressure;
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
    public void archive() throws ExecutionException, InterruptedException {
        StreamArchiveState state = streamManager.getStreamArchive(stream.streamId(), stream.streamEpoch()).get();
        if (state.phase() == StreamArchivePhase.CLEANUP_PREPARED) {
            // Retention cleanup owns the durable intent. Let the Archive cleanup task recover it before ARCHIVE
            // selects or recovers work so only one prepare state is active at a time.
            return;
        }
        if (state.phase() == StreamArchivePhase.ARCHIVE_PREPARED) {
            // Recover a batch whose prepare was committed but whose copies or publish were interrupted. Recopying the
            // complete batch overwrites the same deterministic keys and avoids per-object recovery state.
            List<S3ObjectMetadata> prepared = consecutiveObjects(
                getRange(state.archiveEndOffset(), state.archivePreparedEndOffset(), MAX_OBJECTS_PER_BATCH),
                state.archiveEndOffset());
            LOGGER.info("[ARCHIVE_RECOVER] streamId={}, streamEpoch={}, range=[{}, {}), objectCount={}",
                state.streamId(), state.streamEpoch(), state.archiveEndOffset(), state.archivePreparedEndOffset(),
                prepared.size());
            copyAndPublish(state, prepared);
            return;
        }
        List<S3ObjectMetadata> online = null;
        if (state.archiveEndOffset() < stream.startOffset()) {
            // Retention has overtaken the Archive cursor. Do not archive new history until every published Archive
            // object has been deleted and Controller metadata cleanup has removed the corresponding online metadata.
            // Empty-cursor advance starts from this fully drained state; the Controller then owns the new metadata
            // cleanup backlog created by advancing the Broker-owned boundaries.
            if (!isEmptyAndMetadataCaughtUp(state)) {
                return;
            }
            online = getRange(stream.startOffset(), stream.confirmOffset(), 1);
            S3ObjectMetadata firstLivingStreamObject = online.stream()
                .filter(object -> object.getType() == S3ObjectType.STREAM
                    || object.getType() == S3ObjectType.COMPOSITE)
                .filter(object -> object.startOffset() <= stream.startOffset())
                .filter(object -> object.endOffset() > stream.startOffset())
                .findFirst()
                .orElse(null);
            // Wait for Stream Set Object compaction or force-split instead of making the Archive cursor depend on
            // node-owned SSO metadata. An empty Stream can likewise wait until it has a living Stream Object.
            if (firstLivingStreamObject == null
                || firstLivingStreamObject.startOffset() < state.archiveEndOffset()) {
                return;
            }
            long newCursor = firstLivingStreamObject.startOffset();
            if (newCursor > state.archiveEndOffset()) {
                LOGGER.info("[ARCHIVE_ADVANCE] streamId={}, streamEpoch={}, oldEndOffset={}, newEndOffset={}",
                    state.streamId(), state.streamEpoch(), state.archiveEndOffset(), newCursor);
                streamManager.updateStreamArchive(new StreamArchiveOperation.AdvanceEmptyCursor(state.streamId(),
                    state.streamEpoch(), state.archiveMetadataEndOffset(), newCursor)).get();
                return;
            }
        }

        if (online == null) {
            online = getRange(state.archiveEndOffset(), stream.confirmOffset(), MAX_OBJECTS_WITH_LOOKAHEAD);
        }
        List<S3ObjectMetadata> parsed = consecutiveObjects(online, state.archiveEndOffset());
        List<S3ObjectMetadata> selected = selectCandidates(parsed, stream.startOffset());
        if (selected.isEmpty()) {
            return;
        }
        long preparedEndOffset = selected.get(selected.size() - 1).endOffset();
        StreamArchiveState preparedState = state.toBuilder().archivePreparedEndOffset(preparedEndOffset).build();
        LOGGER.info("[ARCHIVE_PREPARE] streamId={}, streamEpoch={}, range=[{}, {}), objectCount={}, objectIds={}",
            state.streamId(), state.streamEpoch(), state.archiveEndOffset(), preparedEndOffset, selected.size(),
            selected.stream().map(S3ObjectMetadata::objectId).toList());
        // The Controller validates that these IDs still describe the requested consecutive range. Once accepted,
        // compaction cannot replace the prepared objects while they are being copied.
        streamManager.updateStreamArchive(new StreamArchiveOperation.ArchivePrepare(state.streamId(), state.streamEpoch(),
            state.archiveEndOffset(), preparedEndOffset,
            selected.stream().map(S3ObjectMetadata::objectId).toList())).get();
        copyAndPublish(preparedState, selected);
    }

    private List<S3ObjectMetadata> getRange(long startOffset, long endOffset, int limit)
        throws ExecutionException, InterruptedException {
        return objectManager.getStreamObjects(stream.streamId(), startOffset, endOffset, limit).get();
    }

    private List<S3ObjectMetadata> consecutiveObjects(List<S3ObjectMetadata> objects, long cursor) {
        List<S3ObjectMetadata> parsed = new ArrayList<>();
        long nextOffset = cursor;
        for (S3ObjectMetadata object : objects) {
            // Candidate 101 is lookahead for deciding whether candidate 100 is a stable merge boundary.
            if (parsed.size() > MAX_OBJECTS_PER_BATCH || object.startOffset() != nextOffset) {
                break;
            }
            parsed.add(object);
            nextOffset = object.endOffset();
        }
        return parsed;
    }

    private List<S3ObjectMetadata> selectCandidates(List<S3ObjectMetadata> objects, long streamStartOffset) {
        List<S3ObjectMetadata> selected = new ArrayList<>();
        int limit = Math.min(MAX_OBJECTS_PER_BATCH, objects.size());
        long now = currentTimeMillis.getAsLong();
        for (int i = 0; i < limit; i++) {
            S3ObjectMetadata current = objects.get(i);
            S3ObjectMetadata next = i + 1 < objects.size() ? objects.get(i + 1) : null;
            if (!isArchivable(current, next, streamStartOffset, pressure, now)) {
                break;
            }
            selected.add(current);
        }
        return selected;
    }

    private static boolean isArchivable(S3ObjectMetadata current, S3ObjectMetadata next, long streamStartOffset,
        Pressure pressure, long now) {
        if (current.startOffset() < streamStartOffset && current.endOffset() > streamStartOffset) {
            return true;
        }
        if (current.objectSize() >= COMPOSITE_TARGET_SIZE
            || pressure.isOldEnough(current.committedTimestamp(), now)) {
            return true;
        }
        if (next == null) {
            return false;
        }
        return StreamObjectCompactor.cannotMergeIntoGroup(current.objectSize(), current.startOffset(), 1,
            StreamObjectCompactor.objectPartCount(current.objectSize()), next, COMPOSITE_TARGET_SIZE, false);
    }

    private static boolean isEmptyAndMetadataCaughtUp(StreamArchiveState state) {
        return state.archiveStartOffset() == state.archiveMetadataEndOffset()
            && state.archiveMetadataEndOffset() == state.archiveEndOffset()
            && state.archiveEndOffset() == state.archivePreparedEndOffset()
            && state.archiveSize() == 0 && state.archiveCleanupSize() == 0;
    }

    private void copyAndPublish(StreamArchiveState preparedState, List<S3ObjectMetadata> objects)
        throws ExecutionException, InterruptedException {
        if (objects.isEmpty() || objects.get(0).startOffset() != preparedState.archiveEndOffset()
            || objects.get(objects.size() - 1).endOffset() != preparedState.archivePreparedEndOffset()) {
            return;
        }
        List<CompletableFuture<Long>> copies = objects.stream()
            .map(this::copyObject)
            .toList();
        CompletableFuture.allOf(copies.toArray(CompletableFuture[]::new)).get();
        long batchObjectSize = 0L;
        for (CompletableFuture<Long> copy : copies) {
            batchObjectSize = Math.addExact(batchObjectSize, copy.join());
        }
        long archiveSize = Math.addExact(preparedState.archiveSize(), batchObjectSize);
        LOGGER.info("[ARCHIVE_PUBLISH] streamId={}, streamEpoch={}, range=[{}, {}), objectCount={}, objectSize={}, archiveSize={}",
            preparedState.streamId(), preparedState.streamEpoch(), preparedState.archiveEndOffset(),
            preparedState.archivePreparedEndOffset(), objects.size(), batchObjectSize, archiveSize);
        streamManager.updateStreamArchive(new StreamArchiveOperation.ArchivePublish(preparedState.streamId(),
            preparedState.streamEpoch(), preparedState.archiveEndOffset(), preparedState.archivePreparedEndOffset(),
            archiveSize)).get();
    }

    private CompletableFuture<Long> copyObject(S3ObjectMetadata object) {
        ObjectAttributes.Type type = ObjectAttributes.from(object.attributes()).type();
        String archiveKey = ArchiveObjectKey.manifestKey(stream.streamId(), object.startOffset(), object.endOffset(),
            type, object.objectId(), object.objectSize());
        return objectStorage.primary().copy(objectStorage.bucketURI(object.bucket()).bucket(), object.key(), archiveKey)
            .thenApply(ignored -> object.objectSize());
    }

    public enum Pressure {
        LOW(Long.MAX_VALUE),
        MEDIUM(TimeUnit.HOURS.toMillis(24)),
        HIGH(TimeUnit.HOURS.toMillis(1));

        private final long minimumAgeMillis;

        Pressure(long minimumAgeMillis) {
            this.minimumAgeMillis = minimumAgeMillis;
        }

        @VisibleForTesting
        boolean isOldEnough(long committedTimestamp, long now) {
            return committedTimestamp >= 0 && minimumAgeMillis != Long.MAX_VALUE
                && now - committedTimestamp >= minimumAgeMillis;
        }
    }

    /**
     * Builder for one Stream-owned ARCHIVE task.
     */
    public static final class Builder {
        private ObjectManager objectManager;
        private StreamManager streamManager;
        private ObjectStorage objectStorage;
        private Stream stream;
        private LongSupplier currentTimeMillis = System::currentTimeMillis;
        private Pressure pressure = Pressure.LOW;

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

        public Builder pressure(Pressure pressure) {
            this.pressure = Objects.requireNonNull(pressure, "pressure");
            return this;
        }

        @VisibleForTesting
        Builder currentTimeMillis(LongSupplier currentTimeMillis) {
            this.currentTimeMillis = currentTimeMillis;
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
