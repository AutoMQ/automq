/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.kafka.controller.stream;

import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.AdvanceEmptyCursor;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.ArchivePrepare;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.ArchivePublish;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.CleanupCommit;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.CleanupPrepare;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveOperation;
import org.apache.kafka.common.message.UpdateStreamArchiveResponseData.UpdateStreamResponse;
import org.apache.kafka.common.metadata.RemoveS3StreamArchiveRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.StreamArchiveOperationType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.ControllerResult;
import org.apache.kafka.controller.QuorumController;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.apache.kafka.timeline.TimelineLong;

import com.automq.stream.s3.CompositeObject;
import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamArchivePhase;
import com.automq.stream.utils.AsyncLogger;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Owns Controller-side Stream Archive state validation, metadata reclamation, and deleted-Stream cleanup.
 */
final class StreamArchiveControlManager {
    private static final int MAX_ARCHIVE_OBJECTS_PER_BATCH = 100;
    private static final int MAX_METADATA_CLEANUP_OBJECTS = 1_000;
    private static final int MAX_DELETED_STREAM_OBJECTS = 100;
    private static final long DELETED_STREAM_QUIESCENCE_MS = TimeUnit.MINUTES.toMillis(5);

    private final Logger log;
    private final QuorumController quorumController;
    private final S3ObjectControlManager objectControlManager;
    private final ObjectStorage objectStorage;
    private final Time time;
    private final Supplier<AutoMQVersion> versionSupplier;
    private final NodeEpochValidator nodeEpochValidator;
    private final TimelineHashMap<Long, S3StreamArchiveMetadata> archives;
    private final TimelineLong totalSize;
    private final StreamMetadataProvider streams;
    private final ArchivePrepareHandler archivePrepareHandler;
    private final ArchivePublishHandler archivePublishHandler;
    private final CleanupPrepareHandler cleanupPrepareHandler;
    private final CleanupCommitHandler cleanupCommitHandler;
    private final AdvanceEmptyCursorHandler advanceEmptyCursorHandler;
    private final MetadataCleanup metadataCleanup;
    private final DeletedCleanup deletedCleanup;

    StreamArchiveControlManager(LogContext logContext, QuorumController quorumController,
        S3ObjectControlManager objectControlManager, ObjectStorage objectStorage, Time time,
        Supplier<AutoMQVersion> versionSupplier, NodeEpochValidator nodeEpochValidator,
        StreamMetadataProvider streams, SnapshotRegistry snapshotRegistry) {
        this.log = AsyncLogger.wrap(logContext.logger(StreamArchiveControlManager.class));
        this.quorumController = quorumController;
        this.objectControlManager = objectControlManager;
        this.objectStorage = objectStorage;
        this.time = time;
        this.versionSupplier = versionSupplier;
        this.nodeEpochValidator = nodeEpochValidator;
        this.streams = streams;
        this.archives = new TimelineHashMap<>(snapshotRegistry, 100000);
        this.totalSize = new TimelineLong(snapshotRegistry);
        this.archivePrepareHandler = new ArchivePrepareHandler();
        this.archivePublishHandler = new ArchivePublishHandler();
        this.cleanupPrepareHandler = new CleanupPrepareHandler();
        this.cleanupCommitHandler = new CleanupCommitHandler();
        this.advanceEmptyCursorHandler = new AdvanceEmptyCursorHandler();
        this.metadataCleanup = new MetadataCleanup(snapshotRegistry);
        this.deletedCleanup = new DeletedCleanup(snapshotRegistry);
    }

    /**
     * Validates and persists one typed Broker-owned Archive operation.
     *
     * <p>The accepted transitions are:</p>
     * <ul>
     *     <li>Archive prepare: advance {@code archivePreparedEndOffset} and freeze the matching object IDs.</li>
     *     <li>Archive publish: advance {@code archiveEndOffset} to the prepared boundary, increase
     *         {@code archiveSize}, and clear the object IDs.</li>
     *     <li>Cleanup prepare: advance {@code archiveCleanupEndOffset} and set {@code archiveCleanupSize} for a
     *         non-empty prefix of published Archive objects.</li>
     *     <li>Cleanup commit: advance {@code archiveStartOffset} to the cleanup boundary, subtract the prepared size,
     *         and clear the cleanup fields.</li>
     *     <li>Empty cursor advance: when retention has overtaken a fully drained Archive, move all Broker-owned
     *         offsets to an online object boundary no later than the Stream start offset.</li>
     * </ul>
     *
     * <p>An exact retry is accepted as an idempotent no-op. Every other state change is rejected. The wire request
     * carries exactly one payload matching its operation discriminator; the dispatcher rejects a payload assembled
     * for a different operation before invoking its handler.</p>
     */
    @SuppressWarnings("NPathComplexity")
    ControllerResult<UpdateStreamResponse> update(int nodeId, long nodeEpoch, StreamArchiveOperation request) {
        UpdateStreamResponse response = new UpdateStreamResponse();
        if (!versionSupplier.get().isStreamArchiveSupported()) {
            return error(response, Errors.UNSUPPORTED_VERSION);
        }
        Errors nodeError = nodeEpochValidator.validate(nodeId, nodeEpoch);
        if (nodeError != Errors.NONE) {
            return error(response, nodeError);
        }
        StreamRuntimeMetadata stream = streams.get(request.streamId());
        if (stream == null) {
            return error(response, Errors.STREAM_NOT_EXIST);
        }
        RangeMetadata currentRange = stream.currentRangeMetadata();
        if (stream.currentEpoch() != request.streamEpoch()
            || currentRange == null || currentRange.nodeId() != nodeId) {
            return error(response, Errors.STREAM_FENCED);
        }
        S3StreamArchiveMetadata current = get(request.streamId());
        if (request.operation() < 0 || request.operation() > StreamArchiveOperationType.ADVANCE_EMPTY_CURSOR.value()) {
            return error(response, Errors.INVALID_REQUEST);
        }
        StreamArchiveOperationType operation = StreamArchiveOperationType.fromValue(request.operation());
        if (!hasPayloadForOperation(request, operation)) {
            return error(response, Errors.INVALID_REQUEST);
        }
        return switch (operation) {
            case ARCHIVE_PREPARE -> archivePrepareHandler.handle(
                stream, current, request.archivePrepare(), response);
            case ARCHIVE_PUBLISH -> archivePublishHandler.handle(current, request.archivePublish(), response);
            case CLEANUP_PREPARE -> cleanupPrepareHandler.handle(current, request.cleanupPrepare(), response);
            case CLEANUP_COMMIT -> cleanupCommitHandler.handle(current, request.cleanupCommit(), response);
            case ADVANCE_EMPTY_CURSOR -> advanceEmptyCursorHandler.handle(
                stream, current, request.advanceEmptyCursor(), response);
        };
    }

    ControllerResult<Void> cleanupMetadata(long streamId) {
        return metadataCleanup.cleanupStream(streamId);
    }

    void reconcile() {
        metadataCleanup.schedule();
        deletedCleanup.schedule();
    }

    long totalSize() {
        return totalSize.get();
    }

    void replay(S3StreamArchiveRecord record) {
        S3StreamArchiveMetadata archive = S3StreamArchiveMetadata.fromRecord(record);
        S3StreamArchiveMetadata previous = archives.put(record.streamId(), archive);
        long previousSize = previous == null ? 0L : previous.archiveSize();
        totalSize.set(Math.addExact(Math.subtractExact(totalSize.get(), previousSize), archive.archiveSize()));
        // Size follows Archive records alone. Stream presence is consulted only to recover which
        // cleanup owns this durable state, including deleted-Stream tasks restored from a snapshot.
        if (streams.contains(record.streamId())) {
            deletedCleanup.onStreamRemoved(record.streamId());
            metadataCleanup.onArchiveChanged(archive);
        } else {
            metadataCleanup.onStreamRemoved(record.streamId());
            deletedCleanup.onStreamDeleted(record.streamId());
        }
    }

    void onStreamCreated(long streamId) {
        S3StreamArchiveMetadata archive = archives.get(streamId);
        metadataCleanup.onArchiveChanged(archive);
        deletedCleanup.onStreamRemoved(streamId);
    }

    void replay(RemoveS3StreamArchiveRecord record) {
        S3StreamArchiveMetadata removed = archives.remove(record.streamId());
        if (removed != null) {
            totalSize.set(Math.subtractExact(totalSize.get(), removed.archiveSize()));
        }
        metadataCleanup.onStreamRemoved(record.streamId());
        deletedCleanup.onStreamRemoved(record.streamId());
    }

    void onStreamDeleted(long streamId) {
        metadataCleanup.onStreamRemoved(streamId);
        if (archives.containsKey(streamId)) {
            deletedCleanup.onStreamDeleted(streamId);
        }
    }

    S3StreamArchiveMetadata get(long streamId) {
        S3StreamArchiveMetadata archive = archives.get(streamId);
        if (archive != null) {
            return archive;
        }
        StreamRuntimeMetadata stream = streams.get(streamId);
        return stream == null ? null : S3StreamArchiveMetadata.defaultAt(streamId, stream.startOffset());
    }

    private static ControllerResult<UpdateStreamResponse> error(UpdateStreamResponse response, Errors error) {
        response.setErrorCode(error.code());
        return ControllerResult.of(Collections.emptyList(), response);
    }

    private static ControllerResult<UpdateStreamResponse> noOp(UpdateStreamResponse response) {
        return ControllerResult.of(Collections.emptyList(), response);
    }

    /**
     * Ensures the typed operation envelope contains exactly its declared payload.
     */
    private static boolean hasPayloadForOperation(StreamArchiveOperation request,
        StreamArchiveOperationType operation) {
        int payloadCount = (request.archivePrepare() != null ? 1 : 0)
                + (request.archivePublish() != null ? 1 : 0)
                + (request.cleanupPrepare() != null ? 1 : 0)
                + (request.cleanupCommit() != null ? 1 : 0)
                + (request.advanceEmptyCursor() != null ? 1 : 0);
        if (payloadCount != 1) {
            return false;
        }
        return switch (operation) {
            case ARCHIVE_PREPARE -> request.archivePrepare() != null;
            case ARCHIVE_PUBLISH -> request.archivePublish() != null;
            case CLEANUP_PREPARE -> request.cleanupPrepare() != null;
            case CLEANUP_COMMIT -> request.cleanupCommit() != null;
            case ADVANCE_EMPTY_CURSOR -> request.advanceEmptyCursor() != null;
        };
    }

    private final class ArchivePrepareHandler {
        ControllerResult<UpdateStreamResponse> handle(StreamRuntimeMetadata stream,
            S3StreamArchiveMetadata current, ArchivePrepare desired, UpdateStreamResponse response) {
            if (!isValid(desired)) {
                return error(response, Errors.INVALID_REQUEST);
            }
            if (isRetry(current, desired)) {
                return noOp(response);
            }
            if (!canApply(stream, current, desired)) {
                return error(response, Errors.STREAM_ARCHIVE_STATE_CONFLICT);
            }
            S3StreamArchiveMetadata next = current.prepareArchive(desired.archivePreparedEndOffset());
            return ControllerResult.atomicOf(List.of(next.toRecord()), response);
        }

        private boolean isValid(ArchivePrepare desired) {
            return desired.expectedArchiveEndOffset() >= 0
                && desired.archivePreparedEndOffset() > desired.expectedArchiveEndOffset()
                && !desired.archiveObjectIds().isEmpty()
                && desired.archiveObjectIds().size() <= MAX_ARCHIVE_OBJECTS_PER_BATCH
                && desired.archiveObjectIds().stream().allMatch(objectId -> objectId >= 0)
                && desired.archiveObjectIds().stream().distinct().count() == desired.archiveObjectIds().size();
        }

        private boolean isRetry(S3StreamArchiveMetadata current, ArchivePrepare desired) {
            return current.phase() == StreamArchivePhase.ARCHIVE_PREPARED
                && current.archiveEndOffset() == desired.expectedArchiveEndOffset()
                && current.archivePreparedEndOffset() == desired.archivePreparedEndOffset();
        }

        private boolean canApply(StreamRuntimeMetadata stream, S3StreamArchiveMetadata current,
            ArchivePrepare desired) {
            return current.phase() == StreamArchivePhase.IDLE
                && current.archiveEndOffset() == desired.expectedArchiveEndOffset()
                && isCurrentObjectSequence(stream, desired);
        }

        private boolean isCurrentObjectSequence(StreamRuntimeMetadata stream, ArchivePrepare request) {
            long nextOffset = request.expectedArchiveEndOffset();
            List<S3StreamObject> objects = stream.streamObjects().values().stream()
                .filter(object -> overlaps(object.startOffset(), object.endOffset(),
                    request.expectedArchiveEndOffset(), request.archivePreparedEndOffset()))
                .sorted(Comparator.comparingLong(S3StreamObject::startOffset)
                    .thenComparingLong(S3StreamObject::objectId))
                .collect(Collectors.toList());
            if (objects.size() != request.archiveObjectIds().size()) {
                return false;
            }
            for (int i = 0; i < objects.size(); i++) {
                S3StreamObject object = objects.get(i);
                if (object.objectId() != request.archiveObjectIds().get(i) || object.startOffset() != nextOffset
                    || object.endOffset() <= nextOffset) {
                    return false;
                }
                S3Object metadata = objectControlManager.getObject(object.objectId());
                if (metadata == null || metadata.getS3ObjectState() != S3ObjectState.COMMITTED) {
                    return false;
                }
                nextOffset = object.endOffset();
            }
            return nextOffset == request.archivePreparedEndOffset();
        }

        private boolean overlaps(long firstStart, long firstEnd, long secondStart, long secondEnd) {
            return firstStart < secondEnd && secondStart < firstEnd;
        }
    }

    private static final class ArchivePublishHandler {
        ControllerResult<UpdateStreamResponse> handle(S3StreamArchiveMetadata current,
            ArchivePublish desired, UpdateStreamResponse response) {
            if (!isValid(desired)) {
                return error(response, Errors.INVALID_REQUEST);
            }
            if (isRetry(current, desired)) {
                return noOp(response);
            }
            if (!canApply(current, desired)) {
                return error(response, Errors.STREAM_ARCHIVE_STATE_CONFLICT);
            }
            S3StreamArchiveMetadata next = current.publishArchive(desired.archiveEndOffset(), desired.archiveSize());
            return ControllerResult.atomicOf(List.of(next.toRecord()), response);
        }

        private boolean isValid(ArchivePublish desired) {
            return desired.expectedArchiveEndOffset() >= 0
                && desired.archiveEndOffset() >= desired.expectedArchiveEndOffset()
                && desired.archiveSize() >= 0;
        }

        private boolean isRetry(S3StreamArchiveMetadata current, ArchivePublish desired) {
            return current.archiveEndOffset() == desired.archiveEndOffset()
                && current.archiveSize() == desired.archiveSize();
        }

        private boolean canApply(S3StreamArchiveMetadata current, ArchivePublish desired) {
            return current.phase() == StreamArchivePhase.ARCHIVE_PREPARED
                && current.archiveEndOffset() == desired.expectedArchiveEndOffset()
                && current.archivePreparedEndOffset() == desired.archiveEndOffset();
        }
    }

    private static final class CleanupPrepareHandler {
        ControllerResult<UpdateStreamResponse> handle(S3StreamArchiveMetadata current,
            CleanupPrepare desired, UpdateStreamResponse response) {
            if (!isValid(desired)) {
                return error(response, Errors.INVALID_REQUEST);
            }
            if (isRetry(current, desired)) {
                return noOp(response);
            }
            if (!canApply(current, desired)) {
                return error(response, Errors.STREAM_ARCHIVE_STATE_CONFLICT);
            }
            S3StreamArchiveMetadata next = current.prepareCleanup(
                desired.archiveCleanupEndOffset(), desired.archiveCleanupSize());
            return ControllerResult.atomicOf(List.of(next.toRecord()), response);
        }

        private boolean isValid(CleanupPrepare desired) {
            return desired.expectedArchiveStartOffset() >= 0
                && desired.archiveCleanupEndOffset() > desired.expectedArchiveStartOffset()
                && desired.archiveCleanupSize() > 0;
        }

        private boolean isRetry(S3StreamArchiveMetadata current, CleanupPrepare desired) {
            return current.phase() == StreamArchivePhase.CLEANUP_PREPARED
                && current.archiveStartOffset() == desired.expectedArchiveStartOffset()
                && current.archiveCleanupEndOffset() == desired.archiveCleanupEndOffset()
                && current.archiveCleanupSize() == desired.archiveCleanupSize();
        }

        private boolean canApply(S3StreamArchiveMetadata current, CleanupPrepare desired) {
            return current.phase() == StreamArchivePhase.IDLE
                && current.archiveStartOffset() == desired.expectedArchiveStartOffset();
        }
    }

    private static final class CleanupCommitHandler {
        ControllerResult<UpdateStreamResponse> handle(S3StreamArchiveMetadata current,
            CleanupCommit desired, UpdateStreamResponse response) {
            if (!isValid(desired)) {
                return error(response, Errors.INVALID_REQUEST);
            }
            if (isRetry(current, desired)) {
                return noOp(response);
            }
            if (!canApply(current, desired)) {
                return error(response, Errors.STREAM_ARCHIVE_STATE_CONFLICT);
            }
            S3StreamArchiveMetadata next = current.commitCleanup();
            return ControllerResult.atomicOf(List.of(next.toRecord()), response);
        }

        private boolean isValid(CleanupCommit desired) {
            return desired.expectedArchiveStartOffset() >= 0
                && desired.archiveCleanupEndOffset() > desired.expectedArchiveStartOffset();
        }

        private boolean isRetry(S3StreamArchiveMetadata current, CleanupCommit desired) {
            return current.phase() == StreamArchivePhase.IDLE
                && current.archiveStartOffset() == desired.archiveCleanupEndOffset();
        }

        private boolean canApply(S3StreamArchiveMetadata current, CleanupCommit desired) {
            return current.phase() == StreamArchivePhase.CLEANUP_PREPARED
                && current.archiveStartOffset() == desired.expectedArchiveStartOffset()
                && current.archiveCleanupEndOffset() == desired.archiveCleanupEndOffset();
        }
    }

    private static final class AdvanceEmptyCursorHandler {
        ControllerResult<UpdateStreamResponse> handle(StreamRuntimeMetadata stream,
            S3StreamArchiveMetadata current, AdvanceEmptyCursor desired, UpdateStreamResponse response) {
            if (!isValid(desired)) {
                return error(response, Errors.INVALID_REQUEST);
            }
            if (isRetry(current, desired)) {
                return noOp(response);
            }
            if (!canApply(stream, current, desired)) {
                return error(response, Errors.STREAM_ARCHIVE_STATE_CONFLICT);
            }
            S3StreamArchiveMetadata next = current.advanceEmptyCursor(desired.newArchiveOffset());
            return ControllerResult.atomicOf(List.of(next.toRecord()), response);
        }

        private boolean isValid(AdvanceEmptyCursor desired) {
            return desired.expectedArchiveOffset() >= 0
                && desired.newArchiveOffset() >= desired.expectedArchiveOffset();
        }

        private boolean isRetry(S3StreamArchiveMetadata current, AdvanceEmptyCursor desired) {
            return current.phase() == StreamArchivePhase.IDLE
                && current.archiveStartOffset() == desired.newArchiveOffset()
                && current.archiveEndOffset() == desired.newArchiveOffset()
                && current.archiveSize() == 0;
        }

        private boolean canApply(StreamRuntimeMetadata stream, S3StreamArchiveMetadata current,
            AdvanceEmptyCursor desired) {
            long expected = desired.expectedArchiveOffset();
            boolean isExpectedEmptyArchive = current.phase() == StreamArchivePhase.IDLE
                && current.archiveStartOffset() == expected
                && current.archiveMetadataEndOffset() == expected
                && current.archiveEndOffset() == expected
                && current.archiveSize() == 0;
            boolean isValidNewOffset = desired.newArchiveOffset() >= expected
                && desired.newArchiveOffset() <= stream.startOffset()
                && isLivingStreamObjectBoundary(stream, desired.newArchiveOffset());
            return isExpectedEmptyArchive && isValidNewOffset;
        }

        private boolean isLivingStreamObjectBoundary(StreamRuntimeMetadata stream, long offset) {
            return stream.streamObjects().values().stream().anyMatch(object -> object.startOffset() == offset
                && object.startOffset() <= stream.startOffset() && object.endOffset() > stream.startOffset());
        }
    }

    /** Reclaims online object metadata after the corresponding Archive range has been published. */
    private final class MetadataCleanup {
        private final TimelineHashSet<Long> pending;
        private final AtomicBoolean inFlight = new AtomicBoolean();

        MetadataCleanup(SnapshotRegistry snapshotRegistry) {
            pending = new TimelineHashSet<>(snapshotRegistry, 1000);
        }

        // Called by the outer manager when a direct cleanup write event is requested.
        ControllerResult<Void> cleanupStream(long streamId) {
            StreamRuntimeMetadata stream = streams.get(streamId);
            S3StreamArchiveMetadata archive = get(streamId);
            if (stream == null || archive == null
                || archive.archiveMetadataEndOffset() >= archive.archiveEndOffset()) {
                return ControllerResult.of(Collections.emptyList(), null);
            }
            List<S3StreamObject> ordered = stream.streamObjects().values().stream()
                .filter(object -> object.endOffset() <= archive.archiveEndOffset())
                .sorted(Comparator.comparingLong(S3StreamObject::startOffset))
                .limit(MAX_METADATA_CLEANUP_OBJECTS)
                .collect(Collectors.toList());
            List<Long> objectIds = new ArrayList<>(MAX_METADATA_CLEANUP_OBJECTS);
            long cleanupEndOffset = archive.archiveMetadataEndOffset();
            for (S3StreamObject candidate : ordered) {
                if (candidate.startOffset() != cleanupEndOffset || candidate.endOffset() <= cleanupEndOffset) {
                    break;
                }
                S3Object object = objectControlManager.getObject(candidate.objectId());
                if (object == null || object.getS3ObjectState() != S3ObjectState.COMMITTED) {
                    break;
                }
                objectIds.add(candidate.objectId());
                cleanupEndOffset = candidate.endOffset();
            }
            if (objectIds.isEmpty()) {
                return ControllerResult.of(Collections.emptyList(), null);
            }
            ControllerResult<Boolean> shallowDelete = objectControlManager.markDestroyObjects(
                objectIds, Collections.nCopies(objectIds.size(), CompactOperations.DELETE));
            if (!shallowDelete.response()) {
                return ControllerResult.of(Collections.emptyList(), null);
            }
            List<ApiMessageAndVersion> records = new ArrayList<>(shallowDelete.records());
            objectIds.forEach(objectId -> records.add(new ApiMessageAndVersion(
                new RemoveS3StreamObjectRecord().setStreamId(streamId).setObjectId(objectId), (short) 0)));
            records.add(archive.advanceMetadataCleanup(cleanupEndOffset).toRecord());
            return ControllerResult.atomicOf(records, null);
        }

        // Called by replay and archive-state lifecycle callbacks.
        synchronized void onArchiveChanged(S3StreamArchiveMetadata archive) {
            if (archive == null) {
                return;
            }
            if (archive.archiveMetadataEndOffset() < archive.archiveEndOffset()) {
                pending.add(archive.streamId());
            } else {
                pending.remove(archive.streamId());
            }
        }

        // Called when the stream or archive record is removed from the image.
        synchronized void onStreamRemoved(long streamId) {
            pending.remove(streamId);
        }

        // Called by the outer manager's periodic reconcile and by completion callbacks.
        void schedule() {
            if (!quorumController.isActive() || !inFlight.compareAndSet(false, true)) {
                return;
            }
            quorumController.appendWriteEvent("cleanupStreamArchiveMetadata", OptionalLong.empty(), () -> {
                if (pending.isEmpty()) {
                    return ControllerResult.of(Collections.emptyList(), false);
                }
                ControllerResult<Void> result = cleanupStream(pending.iterator().next());
                return ControllerResult.of(result.records(), !result.records().isEmpty());
            }).whenComplete((progress, exception) -> {
                inFlight.set(false);
                if (exception != null) {
                    log.error("Failed to clean up published online Stream Archive metadata", exception);
                }
                if (exception == null && Boolean.TRUE.equals(progress)) {
                    // The callback is not guaranteed to run on the Controller event thread.
                    schedule();
                }
            });
        }
    }

    /** Deletes archived objects and the durable Archive record after a Stream has been deleted. */
    private final class DeletedCleanup {
        private final TimelineHashSet<Long> pending;
        private final AtomicBoolean inFlight = new AtomicBoolean();
        private final Map<Long, Long> emptySinceMs = new java.util.HashMap<>();

        DeletedCleanup(SnapshotRegistry snapshotRegistry) {
            pending = new TimelineHashSet<>(snapshotRegistry, 1000);
        }

        // Called when replay or stream deletion discovers a deleted stream archive.
        synchronized void onStreamDeleted(long streamId) {
            S3StreamArchiveMetadata archive = archives.get(streamId);
            if (archive != null && !streams.contains(streamId)) {
                pending.add(streamId);
            }
        }

        // Called when the stream/archive is present again or its tombstone is replayed.
        synchronized void onStreamRemoved(long streamId) {
            pending.remove(streamId);
            emptySinceMs.remove(streamId);
        }

        /**
         * Runs one deleted-stream cleanup page inside a Controller write event.
         *
         * <p>The object-storage chain deliberately handles at most {@value #MAX_DELETED_STREAM_OBJECTS} objects
         * per LIST page and schedules the next page only after the current delete future completes. The chain is
         * asynchronous, so the Controller event thread is released between pages and a large deleted Stream cannot
         * monopolize the KRaft event loop.</p>
         */
        private CompletableFuture<Boolean> runOnce() {
            // schedule() claims the execution slot before enqueueing this Controller event.
            if (pending.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }
            long now = time.milliseconds();
            Long streamId = null;
            for (Long candidate : pending) {
                Long emptySince = emptySinceMs.get(candidate);
                if (emptySince == null || now - emptySince >= DELETED_STREAM_QUIESCENCE_MS) {
                    streamId = candidate;
                    break;
                }
            }
            if (streamId == null) {
                return CompletableFuture.completedFuture(false);
            }
            long selectedStreamId = streamId;
            return cleanup(selectedStreamId).thenApply(ignored -> true);
        }

        // Called by the outer manager's periodic reconcile and by completion callbacks.
        void schedule() {
            if (!quorumController.isActive() || !inFlight.compareAndSet(false, true)) {
                return;
            }
            quorumController.appendWriteEvent("scheduleDeletedStreamArchiveCleanup", OptionalLong.empty(), () -> {
                runOnce().whenComplete((progress, exception) -> {
                    inFlight.set(false);
                    if (exception != null) {
                        log.error("Failed to clean up Archive objects for deleted Stream", exception);
                    } else if (Boolean.TRUE.equals(progress)) {
                        // The callback may run on an object-storage thread. Enqueue the next page instead of
                        // inspecting Timeline state from that thread.
                        schedule();
                    }
                });
                return ControllerResult.of(Collections.emptyList(), null);
            }).exceptionally(exception -> {
                inFlight.set(false);
                log.error("Failed to schedule deleted Stream Archive cleanup", exception);
                return null;
            });
        }

        private CompletableFuture<Void> deleteObject(ObjectStorage.ObjectInfo object) {
            try {
                ArchiveObjectKey.ManifestKey key = ArchiveObjectKey.parseManifestKey(object.key());
                if (key.type() == ObjectAttributes.Type.Normal) {
                    return objectStorage.delete(List.of(new ObjectStorage.ObjectPath(object.bucketId(), object.key())));
                }
                return CompositeObject.delete(ArchiveObjectKey.objectMetadata(object, key), objectStorage);
            } catch (IllegalArgumentException exception) {
                return objectStorage.delete(List.of(new ObjectStorage.ObjectPath(object.bucketId(), object.key())));
            }
        }

        private CompletableFuture<Void> cleanup(long streamId) {
            // Delete at most one LIST page per asynchronous step. A non-empty page proves that more
            // objects may remain, so continue with the same stream only after all deletes complete.
            return objectStorage.primary().list(new ObjectStorage.ListOptions(ArchiveObjectKey.manifestPrefix(streamId))
                .maxKeys(MAX_DELETED_STREAM_OBJECTS)).thenCompose(objects -> {
                    if (!objects.isEmpty()) {
                        synchronized (this) {
                            emptySinceMs.remove(streamId);
                        }
                        List<CompletableFuture<Void>> deletes = objects.stream().map(this::deleteObject).toList();
                        return CompletableFuture.allOf(deletes.toArray(new CompletableFuture[0]))
                            .thenCompose(ignored -> cleanup(streamId));
                    }
                    return onEmpty(streamId);
                });
        }

        private CompletableFuture<Void> onEmpty(long streamId) {
            long now = time.milliseconds();
            synchronized (this) {
                if (!emptySinceMs.containsKey(streamId)) {
                    // An empty page is only a provisional observation: a broker may still be
                    // finishing an archive copy. Require the same observation after five minutes.
                    emptySinceMs.put(streamId, now);
                    return CompletableFuture.completedFuture(null);
                }
                if (now - emptySinceMs.get(streamId) < DELETED_STREAM_QUIESCENCE_MS) {
                    return CompletableFuture.completedFuture(null);
                }
            }
            // Once the prefix has stayed empty for the quiescence window, remove the durable
            // record. Replay of that tombstone removes the stream from the pending set.
            return quorumController.appendWriteEvent("completeDeletedStreamArchiveCleanup", OptionalLong.empty(),
                () -> {
                    if (streams.contains(streamId) || !archives.containsKey(streamId)) {
                        return ControllerResult.of(Collections.emptyList(), null);
                    }
                    return ControllerResult.atomicOf(List.of(new ApiMessageAndVersion(
                        new RemoveS3StreamArchiveRecord().setStreamId(streamId), (short) 0)), null);
                })
                .thenApply(ignored -> null);
        }
    }

    @FunctionalInterface
    interface NodeEpochValidator {
        Errors validate(int nodeId, long nodeEpoch);
    }

    /** Read-only view of Stream metadata owned by StreamControlManager. */
    interface StreamMetadataProvider {
        StreamRuntimeMetadata get(long streamId);

        default boolean contains(long streamId) {
            return get(streamId) != null;
        }
    }
}
