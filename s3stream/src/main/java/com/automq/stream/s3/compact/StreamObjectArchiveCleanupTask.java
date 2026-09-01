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
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ListOptions;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
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
 * Runs one bounded Broker retention-cleanup round for fully expired Archived Composite objects.
 */
public final class StreamObjectArchiveCleanupTask {
    static final int MAX_COMPOSITES_PER_ROUND = 100;
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
     * @return false only when malformed Archive metadata requires CLEANUP_V1 to stop this Stream
     * @throws ExecutionException if object storage or the Controller rejects the operation
     * @throws InterruptedException if the synchronous cleanup scheduler is interrupted
     */
    public boolean cleanup() throws ExecutionException, InterruptedException {
        StreamArchiveState state = streamManager.getStreamArchive(stream.streamId(), stream.streamEpoch()).get();
        if (state.archiveStartOffset() == state.archiveEndOffset()) {
            cache.invalidate();
            return true;
        }

        if (state.archiveCleanupSize() > 0) {
            List<ArchivedComposite> prepared;
            try {
                prepared = listPrepared(state);
            } catch (IllegalArgumentException exception) {
                cache.reportMalformed(stream.streamId(), exception);
                return false;
            }
            delete(prepared);
            commit(state);
            return true;
        }

        long streamStartOffset = stream.startOffset();
        if (cache.provesNoWork(state.archiveStartOffset(), streamStartOffset)) {
            return true;
        }
        List<ArchivedComposite> selected;
        try {
            selected = listExpired(state, streamStartOffset);
        } catch (IllegalArgumentException exception) {
            cache.reportMalformed(stream.streamId(), exception);
            return false;
        }
        if (selected.isEmpty()) {
            return true;
        }
        long cleanupEndOffset = selected.get(selected.size() - 1).key().endOffset();
        long cleanupSize = selected.stream().mapToLong(composite -> composite.key().logicalSize())
            .reduce(0L, Math::addExact);
        StreamArchiveState prepared = new StreamArchiveState(state.streamId(), state.streamEpoch(),
            state.archiveStartOffset(), state.archiveMetadataEndOffset(), state.archiveEndOffset(),
            state.archivePreparedEndOffset(), state.archiveSize(), cleanupEndOffset, cleanupSize, List.of());
        streamManager.updateStreamArchive(prepared).get();
        delete(selected);
        commit(prepared);
        return true;
    }

    private List<ArchivedComposite> listPrepared(StreamArchiveState state)
        throws ExecutionException, InterruptedException {
        List<ObjectInfo> objects = listFromArchiveStart(state);
        List<ArchivedComposite> prepared = new ArrayList<>();
        long previousEndOffset = state.archiveStartOffset();
        for (ObjectInfo object : objects) {
            ManifestKey key = ArchiveObjectKey.parseManifestKey(object.key());
            if (key.streamId() != state.streamId() || key.startOffset() < previousEndOffset) {
                throw new IllegalArgumentException("Invalid Archive manifest key for cleanup: " + object.key());
            }
            if (key.endOffset() > state.archiveCleanupEndOffset()) {
                continue;
            }
            prepared.add(new ArchivedComposite(key, metadata(object, key)));
            previousEndOffset = key.endOffset();
        }
        return prepared;
    }

    private List<ArchivedComposite> listExpired(StreamArchiveState state, long streamStartOffset)
        throws ExecutionException, InterruptedException {
        List<ObjectInfo> objects = listFromArchiveStart(state);
        List<ArchivedComposite> published = new ArrayList<>();
        long nextOffset = state.archiveStartOffset();
        for (ObjectInfo object : objects) {
            ManifestKey key = ArchiveObjectKey.parseManifestKey(object.key());
            if (key.streamId() != state.streamId()) {
                throw new IllegalArgumentException("Invalid Archive manifest key for cleanup: " + object.key());
            }
            if (key.endOffset() > state.archiveEndOffset()) {
                continue;
            }
            if (key.startOffset() != nextOffset) {
                throw new IllegalArgumentException("Discontinuous Archive manifest key for cleanup: " + object.key());
            }
            published.add(new ArchivedComposite(key, metadata(object, key)));
            nextOffset = key.endOffset();
        }
        if (published.isEmpty()) {
            return List.of();
        }
        cache.update(state.archiveStartOffset(), published.get(0));
        List<ArchivedComposite> selected = new ArrayList<>();
        for (ArchivedComposite composite : published) {
            if (composite.key().endOffset() > streamStartOffset) {
                break;
            }
            selected.add(composite);
        }
        return selected;
    }

    private List<ObjectInfo> listFromArchiveStart(StreamArchiveState state)
        throws ExecutionException, InterruptedException {
        ListOptions options = new ListOptions(ArchiveObjectKey.manifestPrefix(state.streamId()))
            .startAfter(ArchiveObjectKey.startAfter(state.streamId(), state.archiveStartOffset()))
            .maxKeys(MAX_COMPOSITES_PER_ROUND);
        return objectStorage.list(options).get();
    }

    private S3ObjectMetadata metadata(ObjectInfo object, ManifestKey key) {
        int attributes = ObjectAttributes.builder().bucket(object.bucketId()).type(ObjectAttributes.Type.Composite)
            .build().attributes();
        return new S3ObjectMetadata(key.objectId(), S3ObjectType.COMPOSITE,
            List.of(new StreamOffsetRange(key.streamId(), key.startOffset(), key.endOffset())),
            S3StreamConstant.INVALID_TS, object.timestamp(), object.size(), S3StreamConstant.INVALID_ORDER_ID,
            attributes, object.key());
    }

    private void delete(List<ArchivedComposite> selected) throws ExecutionException, InterruptedException {
        List<CompletableFuture<Void>> deletes = selected.stream()
            .map(composite -> CompositeObject.delete(composite.metadata(), objectStorage))
            .toList();
        CompletableFuture.allOf(deletes.toArray(CompletableFuture[]::new)).get();
    }

    private void commit(StreamArchiveState prepared) throws ExecutionException, InterruptedException {
        StreamArchiveState committed = new StreamArchiveState(prepared.streamId(), prepared.streamEpoch(),
            prepared.archiveCleanupEndOffset(), prepared.archiveMetadataEndOffset(), prepared.archiveEndOffset(),
            prepared.archivePreparedEndOffset(), Math.subtractExact(prepared.archiveSize(), prepared.archiveCleanupSize()),
            prepared.archiveCleanupEndOffset(), 0L, List.of());
        streamManager.updateStreamArchive(committed).get();
        cache.invalidate();
    }

    private record ArchivedComposite(ManifestKey key, S3ObjectMetadata metadata) {
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

        private boolean provesNoWork(long archiveStartOffset, long streamStartOffset) {
            if (boundary == null) {
                return false;
            }
            if (boundary.archiveStartOffset() != archiveStartOffset) {
                invalidate();
                return false;
            }
            return boundary.endOffset() > streamStartOffset;
        }

        private void update(long archiveStartOffset, ArchivedComposite composite) {
            ManifestKey key = composite.key();
            boundary = new Boundary(archiveStartOffset, key.startOffset(), key.endOffset(), key.objectId(),
                key.logicalSize(), composite.metadata().key());
        }

        /**
         * Discard the cached boundary when Archive progress or Stream ownership changes.
         */
        public void invalidate() {
            boundary = null;
        }

        private void reportMalformed(long streamId, IllegalArgumentException exception) {
            String diagnostic = exception.getMessage();
            long now = System.nanoTime();
            if (!Objects.equals(lastMalformedDiagnostic, diagnostic)
                || now - lastMalformedLogNanos >= MALFORMED_KEY_LOG_INTERVAL_NANOS) {
                LOGGER.error("Stop Archive retention cleanup for stream {}: {}", streamId, diagnostic);
                lastMalformedDiagnostic = diagnostic;
                lastMalformedLogNanos = now;
            }
        }

        private record Boundary(long archiveStartOffset, long startOffset, long endOffset, long objectId,
                                long logicalSize, String key) {
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
