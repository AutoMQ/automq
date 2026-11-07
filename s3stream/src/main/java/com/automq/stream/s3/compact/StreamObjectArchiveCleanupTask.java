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
import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.ArchiveObjectKey.ManifestKey;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ListOptions;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import com.automq.stream.s3.streams.StreamArchiveOperation;
import com.automq.stream.s3.streams.StreamArchivePhase;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Runs one bounded Broker retention-cleanup round for fully expired archived Stream Objects.
 *
 * <p>Cleanup uses a durable two-step transition so object deletion can be retried safely:</p>
 * <ol>
 *     <li>Idle: {@code archiveCleanupSize == 0} and {@code archiveCleanupEndOffset == archiveStartOffset}.</li>
 *     <li>Prepared: persist the exact cleanup end offset and object-size total before deleting selected objects.</li>
 *     <li>Committed: advance {@code archiveStartOffset}, subtract the prepared size exactly once, and return the
 *     cleanup fields to the idle form.</li>
 * </ol>
 *
 * <p>A failure after prepare leaves the prepared state durable. The next round reconstructs that same range from
 * object keys, repeats deletion idempotently, and commits it instead of selecting a new batch.</p>
 */
public final class StreamObjectArchiveCleanupTask {
    static final int MAX_OBJECTS_PER_ROUND = 100;
    private static final long MALFORMED_KEY_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamObjectArchiveCleanupTask.class);

    private final StreamManager streamManager;
    private final ObjectStorage objectStorage;
    private final Stream stream;
    private final LeftBoundaryCache cache;

    private StreamObjectArchiveCleanupTask(Builder builder) {
        streamManager = Objects.requireNonNull(builder.streamManager, "streamManager");
        objectStorage = Objects.requireNonNull(builder.objectStorage, "objectStorage");
        stream = Objects.requireNonNull(builder.stream, "stream");
        cache = Objects.requireNonNull(builder.cache, "cache");
    }

    /**
     * Create a cleanup task builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Run one retention cleanup round, recovering a durable cleanup intent before selecting new work.
     *
     * @return false only when malformed Archive metadata requires the current Archive phase to stop this Stream
     * @throws ExecutionException if object storage or the Controller rejects the operation
     * @throws InterruptedException if the synchronous cleanup scheduler is interrupted
     */
    public boolean cleanup() throws ExecutionException, InterruptedException {
        StreamArchiveState state = streamManager.getStreamArchive(stream.streamId(), stream.streamEpoch()).get();
        if (state.phase() == StreamArchivePhase.ARCHIVE_PREPARED) {
            // ARCHIVE owns the durable intent. Its recovery must publish or fail before retention cleanup prepares a
            // batch, keeping the two recovery state machines mutually exclusive.
            return true;
        }
        if (state.archiveStartOffset() == state.archiveEndOffset()) {
            cache.invalidate();
            return true;
        }

        if (state.phase() == StreamArchivePhase.CLEANUP_PREPARED) {
            // A previous round persisted its intent but did not commit it. Re-run that exact deletion; selecting new
            // work here could subtract the prepared size from a different range.
            List<ArchivedObject> prepared;
            try {
                prepared = listPrepared(state);
            } catch (IllegalArgumentException exception) {
                cache.reportMalformed(stream.streamId(), exception);
                return false;
            }
            LOGGER.info("[ARCHIVE_CLEANUP_RECOVER] streamId={}, streamEpoch={}, range=[{}, {}), objectCount={}, cleanupSize={}",
                state.streamId(), state.streamEpoch(), state.archiveStartOffset(), state.archiveCleanupEndOffset(),
                prepared.size(), state.archiveCleanupSize());
            delete(prepared);
            commit(state);
            return true;
        }

        long streamStartOffset = stream.startOffset();
        if (cache.provesNoWork(state.archiveStartOffset(), streamStartOffset)) {
            return true;
        }
        List<ArchivedObject> selected;
        try {
            selected = listExpired(state, streamStartOffset);
        } catch (IllegalArgumentException exception) {
            cache.reportMalformed(stream.streamId(), exception);
            return false;
        }
        if (selected.isEmpty()) {
            return true;
        }
        // Persist the range and its object-size total before touching object storage. The non-zero cleanup size is the
        // recovery marker used at the beginning of a later round if deletion or commit fails.
        List<ArchivedObject> expired = selected.stream()
            .filter(object -> object.key().endOffset() > state.archiveStartOffset())
            .toList();
        if (expired.isEmpty()) {
            // Orphaned manifests fully before the durable cursor were already accounted for by an earlier commit.
            // Delete them without advancing or charging the cursor again.
            delete(selected);
            cache.invalidate();
            return true;
        }
        long cleanupEndOffset = expired.get(expired.size() - 1).key().endOffset();
        long cleanupSize = expired.stream().mapToLong(object -> object.key().objectSize())
            .reduce(0L, Math::addExact);
        StreamArchiveState prepared = state.toBuilder().archiveCleanupEndOffset(cleanupEndOffset)
            .archiveCleanupSize(cleanupSize).build();
        LOGGER.info("[ARCHIVE_CLEANUP_PREPARE] streamId={}, streamEpoch={}, range=[{}, {}), objectCount={}, cleanupSize={}",
            state.streamId(), state.streamEpoch(), state.archiveStartOffset(), cleanupEndOffset, expired.size(), cleanupSize);
        streamManager.updateStreamArchive(new StreamArchiveOperation.CleanupPrepare(state.streamId(), state.streamEpoch(),
            state.archiveStartOffset(), cleanupEndOffset, cleanupSize)).get();
        // Physical deletion is idempotent. A crash from this point through commit is recovered by the prepared-state
        // branch above, which repeats deletion and applies the size change only in commit.
        delete(selected);
        LOGGER.info("[ARCHIVE_CLEANUP_DELETE] streamId={}, streamEpoch={}, range=[{}, {}), objectCount={}, objectIds={}",
            prepared.streamId(), prepared.streamEpoch(), prepared.archiveStartOffset(),
            prepared.archiveCleanupEndOffset(), selected.size(),
            selected.stream().map(object -> object.key().objectId()).toList());
        commit(prepared);
        return true;
    }

    private List<ArchivedObject> listPrepared(StreamArchiveState state)
        throws ExecutionException, InterruptedException {
        List<ObjectInfo> objects = listFromArchiveStart(state);
        List<ArchivedObject> prepared = new ArrayList<>();
        long previousEndOffset = state.archiveStartOffset();
        for (ObjectInfo object : objects) {
            ManifestKey key = ArchiveObjectKey.parseManifestKey(object.key());
            if (key.streamId() != state.streamId()) {
                throw new IllegalArgumentException("Invalid Archive manifest key for cleanup: " + object.key());
            }
            // A previous cleanup may have committed while the corresponding DELETE was only eventually visible.
            // These objects are safe to retry and must not block recovery of the prepared range.
            if (key.endOffset() <= state.archiveStartOffset()) {
                prepared.add(new ArchivedObject(key, ArchiveObjectKey.objectMetadata(object, key)));
                continue;
            }
            if (key.startOffset() < previousEndOffset) {
                throw new IllegalArgumentException("Overlapping Archive manifest key for cleanup: " + object.key());
            }
            if (key.endOffset() > state.archiveCleanupEndOffset()) {
                break;
            }
            prepared.add(new ArchivedObject(key, ArchiveObjectKey.objectMetadata(object, key)));
            previousEndOffset = key.endOffset();
        }
        return prepared;
    }

    private List<ArchivedObject> listExpired(StreamArchiveState state, long streamStartOffset)
        throws ExecutionException, InterruptedException {
        List<ObjectInfo> objects = listFromArchiveStart(state);
        List<ArchivedObject> selected = new ArrayList<>();
        ArchivedObject firstPublished = null;
        long nextOffset = state.archiveStartOffset();
        for (ObjectInfo object : objects) {
            ManifestKey key = ArchiveObjectKey.parseManifestKey(object.key());
            if (key.streamId() != state.streamId()) {
                throw new IllegalArgumentException("Invalid Archive manifest key for cleanup: " + object.key());
            }
            if (key.endOffset() <= state.archiveStartOffset()) {
                selected.add(new ArchivedObject(key, ArchiveObjectKey.objectMetadata(object, key)));
                continue;
            }
            if (key.endOffset() > state.archiveEndOffset()) {
                break;
            }
            if (key.startOffset() != nextOffset) {
                throw new IllegalArgumentException("Discontinuous Archive manifest key for cleanup: " + object.key());
            }
            ArchivedObject published = new ArchivedObject(key, ArchiveObjectKey.objectMetadata(object, key));
            if (firstPublished == null) {
                firstPublished = published;
            }
            nextOffset = key.endOffset();
            if (key.endOffset() > streamStartOffset) {
                break;
            }
            selected.add(published);
        }
        if (firstPublished != null) {
            cache.update(state.archiveStartOffset(), firstPublished.key().endOffset());
        }
        return selected;
    }

    private List<ObjectInfo> listFromArchiveStart(StreamArchiveState state)
        throws ExecutionException, InterruptedException {
        ListOptions options = new ListOptions(ArchiveObjectKey.manifestPrefix(state.streamId()))
            .maxKeys(MAX_OBJECTS_PER_ROUND);
        return objectStorage.primary().list(options).get();
    }

    private void delete(List<ArchivedObject> selected) throws ExecutionException, InterruptedException {
        List<CompletableFuture<Void>> deletes = selected.stream()
            .map(object -> object.key().type() == ObjectAttributes.Type.Composite
                ? CompositeObject.delete(object.metadata(), objectStorage)
                : objectStorage.delete(List.of(new ObjectStorage.ObjectPath(
                    object.metadata().bucket(), object.metadata().key()))))
            .toList();
        CompletableFuture.allOf(deletes.toArray(CompletableFuture[]::new)).get();
    }

    private void commit(StreamArchiveState prepared) throws ExecutionException, InterruptedException {
        // Atomically publish the new left boundary, account for the prepared batch exactly once, and clear the
        // durable cleanup intent. Keeping cleanupEndOffset equal to the new start offset is the canonical idle form.
        streamManager.updateStreamArchive(new StreamArchiveOperation.CleanupCommit(prepared.streamId(),
            prepared.streamEpoch(), prepared.archiveStartOffset(), prepared.archiveCleanupEndOffset())).get();
        LOGGER.info("[ARCHIVE_CLEANUP_COMMIT] streamId={}, streamEpoch={}, oldStartOffset={}, newStartOffset={}, cleanupSize={}",
            prepared.streamId(), prepared.streamEpoch(), prepared.archiveStartOffset(),
            prepared.archiveCleanupEndOffset(), prepared.archiveCleanupSize());
        cache.invalidate();
    }

    private record ArchivedObject(ManifestKey key, S3ObjectMetadata metadata) {
    }

    /**
     * Per-owned-Stream cache for the current Archive left boundary. One {@code StreamWrapper} owns each instance and
     * accesses it only from the Broker's single-thread compaction scheduler; it is not thread-safe. Archive boundary
     * advance clears it during cleanup commit, while close or destroy clears it when Stream ownership is lost.
     */
    public static final class LeftBoundaryCache {
        private Boundary boundary;
        private String lastMalformedDiagnostic;
        private long lastMalformedLogNanos = Long.MIN_VALUE;

        boolean provesNoWork(long archiveStartOffset, long streamStartOffset) {
            if (boundary == null) {
                return false;
            }
            if (boundary.archiveStartOffset() != archiveStartOffset) {
                invalidate();
                return false;
            }
            return boundary.endOffset() > streamStartOffset;
        }

        void update(long archiveStartOffset, long endOffset) {
            boundary = new Boundary(archiveStartOffset, endOffset);
        }

        /**
         * Discard the cached boundary when Archive progress or Stream ownership changes.
         */
        public void invalidate() {
            boundary = null;
        }

        void reportMalformed(long streamId, IllegalArgumentException exception) {
            String diagnostic = exception.getMessage();
            long now = System.nanoTime();
            if (!Objects.equals(lastMalformedDiagnostic, diagnostic)
                || now - lastMalformedLogNanos >= MALFORMED_KEY_LOG_INTERVAL_NANOS) {
                LOGGER.error("Stop Archive retention cleanup for stream {}: {}", streamId, diagnostic);
                lastMalformedDiagnostic = diagnostic;
                lastMalformedLogNanos = now;
            }
        }

        private record Boundary(long archiveStartOffset, long endOffset) {
        }
    }

    /**
     * Builder for one Stream-owned cleanup task.
     */
    public static final class Builder {
        private StreamManager streamManager;
        private ObjectStorage objectStorage;
        private Stream stream;
        private LeftBoundaryCache cache;

        private Builder() {
        }

        /**
         * Set the Stream Archive state manager.
         */
        public Builder streamManager(StreamManager streamManager) {
            this.streamManager = streamManager;
            return this;
        }

        /**
         * Set the Archive object-storage boundary.
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

        /**
         * Set the cache whose lifetime matches Stream ownership.
         */
        public Builder cache(LeftBoundaryCache cache) {
            this.cache = cache;
            return this;
        }

        /**
         * Build one bounded cleanup task.
         */
        public StreamObjectArchiveCleanupTask build() {
            return new StreamObjectArchiveCleanupTask(this);
        }
    }
}
