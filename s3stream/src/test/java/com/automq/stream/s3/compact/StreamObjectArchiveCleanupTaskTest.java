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
import com.automq.stream.s3.CompositeObjectWriter;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamArchiveOperation;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class StreamObjectArchiveCleanupTaskTest {
    private static final long STREAM_ID = 7L;
    private static final long STREAM_EPOCH = 3L;

    /**
     * Given ARCHIVE has a durable prepared range, when retention cleanup runs, then it yields without preparing
     * cleanup work so ARCHIVE recovery owns the only active intent.
     */
    @Test
    void testCleanupYieldsToPreparedArchive() throws Exception {
        StreamArchiveState archivePrepared = state(0L, 10L, 10L, 0L, 0L).toBuilder()
            .archivePreparedEndOffset(20L)
            .build();
        List<StreamArchiveState> updates = new ArrayList<>();

        boolean healthy = StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(archivePrepared, updates))
            .objectStorage(new MemoryObjectStorage((short) 4))
            .stream(stream(10L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertTrue(healthy);
        assertEquals(List.of(), updates);
    }

    /**
     * Given consecutive fully expired Archived Composites, when one cleanup round runs, then the Broker persists the
     * exact batch, deep-deletes linked objects and manifests, and commits the exact size subtraction.
     */
    @Test
    void testCleanupPreparesDeepDeletesAndCommitsExpiredComposites() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite first = archivedComposite(objectStorage, 11L, 1011L, 0L, 10L, 7);
        ArchivedComposite second = archivedComposite(objectStorage, 12L, 1012L, 10L, 20L, 9);
        StreamArchiveState initial = state(0L, 20L, 16L, 0L, 0L);
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamManager streamManager = streamManager(initial, updates);

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager)
            .objectStorage(objectStorage)
            .stream(stream(20L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(2, updates.size());
        assertEquals(20L, updates.get(0).archiveCleanupEndOffset());
        assertEquals(16L, updates.get(0).archiveCleanupSize());
        assertEquals(0L, updates.get(0).archiveStartOffset());
        assertEquals(16L, updates.get(0).archiveSize());
        assertEquals(20L, updates.get(1).archiveStartOffset());
        assertEquals(0L, updates.get(1).archiveSize());
        assertEquals(20L, updates.get(1).archiveCleanupEndOffset());
        assertEquals(0L, updates.get(1).archiveCleanupSize());
        assertFalse(objectStorage.contains(first.manifestKey()));
        assertFalse(objectStorage.contains(second.manifestKey()));
        assertFalse(objectStorage.contains(ObjectUtils.genKey(0, first.linkedObjectId())));
        assertFalse(objectStorage.contains(ObjectUtils.genKey(0, second.linkedObjectId())));
    }

    /**
     * Given a fully expired archived Normal object, when cleanup runs, then it deletes the copied object directly and
     * commits the same Archive size transition without attempting Composite linked-object cleanup.
     */
    @Test
    void testCleanupDeletesExpiredNormalObjectDirectly() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        String key = ArchiveObjectKey.manifestKey(STREAM_ID, 0L, 10L, ObjectAttributes.Type.Normal, 13L, 7L);
        objectStorage.write(new WriteOptions(), key, Unpooled.wrappedBuffer(new byte[7])).get();
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 10L, 7L, 0L, 0L), updates))
            .objectStorage(objectStorage)
            .stream(stream(10L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(2, updates.size());
        assertEquals(10L, updates.get(1).archiveStartOffset());
        assertEquals(0L, updates.get(1).archiveSize());
        assertFalse(objectStorage.contains(key));
    }

    /**
     * Given a durable cleanup intent whose first Composite manifest is already missing, when cleanup recovers, then it
     * scans the complete prepared range, deletes every survivor, and commits without preparing a second batch.
     */
    @Test
    void testCleanupRecoversWholePreparedRangeDespiteDeletionHoles() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite first = archivedComposite(objectStorage, 21L, 1021L, 0L, 10L, 7);
        ArchivedComposite second = archivedComposite(objectStorage, 22L, 1022L, 10L, 20L, 9);
        ArchivedComposite retained = archivedComposite(objectStorage, 23L, 1023L, 20L, 30L, 11);
        CompositeObject.delete(archiveMetadata(objectStorage, first, 21L, 0L, 10L), objectStorage).get();
        StreamArchiveState prepared = state(0L, 30L, 27L, 20L, 16L);
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(prepared, updates))
            .objectStorage(objectStorage)
            .stream(stream(30L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(1, updates.size());
        assertEquals(20L, updates.get(0).archiveStartOffset());
        assertEquals(11L, updates.get(0).archiveSize());
        assertEquals(0L, updates.get(0).archiveCleanupSize());
        assertFalse(objectStorage.contains(second.manifestKey()));
        assertFalse(objectStorage.contains(ObjectUtils.genKey(0, second.linkedObjectId())));
        assertEquals(true, objectStorage.contains(retained.manifestKey()));
        assertEquals(true, objectStorage.contains(ObjectUtils.genKey(0, retained.linkedObjectId())));
    }

    /**
     * Given a prepared cleanup range followed by newer Archive keys, when recovery lists the ordered prefix, then it
     * stops at the first end offset beyond the prepared boundary and does not inspect later keys.
     */
    @Test
    void testCleanupRecoveryStopsListingAtPreparedBoundary() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite preparedObject = archivedComposite(objectStorage, 24L, 1024L, 0L, 10L, 7);
        ArchivedComposite retained = archivedComposite(objectStorage, 25L, 1025L, 10L, 20L, 9);
        objectStorage.write(new WriteOptions(), ArchiveObjectKey.manifestPrefix(STREAM_ID) + "not-a-manifest",
            Unpooled.wrappedBuffer(new byte[1])).get();
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 20L, 16L, 10L, 7L), updates))
            .objectStorage(objectStorage)
            .stream(stream(20L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(1, updates.size());
        assertEquals(10L, updates.get(0).archiveStartOffset());
        assertFalse(objectStorage.contains(preparedObject.manifestKey()));
        assertTrue(objectStorage.contains(retained.manifestKey()));
    }

    /**
     * Given an Archived Composite that is not fully expired, when cleanup repeats without a boundary or ownership
     * change, then the cached left boundary proves there is no work without issuing another LIST.
     */
    @Test
    void testCleanupCachesLeftBoundaryAcrossNoWorkRounds() throws Exception {
        MemoryObjectStorage memory = new MemoryObjectStorage((short) 4);
        archivedComposite(memory, 31L, 1031L, 0L, 10L, 7);
        ObjectStorage objectStorage = spy(memory);
        StreamObjectArchiveCleanupTask.LeftBoundaryCache cache =
            new StreamObjectArchiveCleanupTask.LeftBoundaryCache();
        StreamObjectArchiveCleanupTask task = StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 10L, 7L, 0L, 0L), new ArrayList<>()))
            .objectStorage(objectStorage)
            .stream(stream(5L))
            .cache(cache)
            .build();

        task.cleanup();
        task.cleanup();

        verify(objectStorage, times(1)).list(org.mockito.ArgumentMatchers.any(ObjectStorage.ListOptions.class));

        cache.invalidate();
        task.cleanup();

        verify(objectStorage, times(2)).list(org.mockito.ArgumentMatchers.any(ObjectStorage.ListOptions.class));
    }

    /**
     * Given a living left-boundary object followed by unrelated later keys, when cleanup scans in end-offset order,
     * then it stops at that object and caches the no-work result without inspecting later keys.
     */
    @Test
    void testCleanupStopsListingAtLivingBoundary() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite living = archivedComposite(objectStorage, 32L, 1032L, 0L, 20L, 7);
        objectStorage.write(new WriteOptions(), ArchiveObjectKey.manifestPrefix(STREAM_ID) + "not-a-manifest",
            Unpooled.wrappedBuffer(new byte[1])).get();

        boolean healthy = StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 20L, 7L, 0L, 0L), new ArrayList<>()))
            .objectStorage(objectStorage)
            .stream(stream(10L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertTrue(healthy);
        assertTrue(objectStorage.contains(living.manifestKey()));
    }

    /**
     * Given two selected Composites, when their linked-object deletes are delayed independently, then both deletes are
     * concurrently in flight and neither Archive manifest is deleted before its own linked delete completes.
     */
    @Test
    void testCleanupDeletesCompositesConcurrentlyAndManifestsLast() throws Exception {
        MemoryObjectStorage memory = new MemoryObjectStorage((short) 4);
        ArchivedComposite first = archivedComposite(memory, 51L, 1051L, 0L, 10L, 7);
        ArchivedComposite second = archivedComposite(memory, 52L, 1052L, 10L, 20L, 9);
        ObjectStorage objectStorage = mock(ObjectStorage.class);
        when(objectStorage.primary()).thenReturn(objectStorage);
        when(objectStorage.list(any(ObjectStorage.ListOptions.class))).thenAnswer(invocation ->
            memory.list(invocation.<ObjectStorage.ListOptions>getArgument(0)));
        when(objectStorage.rangeRead(any(), any(), any(Long.class), any(Long.class))).thenAnswer(invocation ->
            memory.rangeRead(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2),
                invocation.getArgument(3)));
        CompletableFuture<Void> firstLinkedDelete = new CompletableFuture<>();
        CompletableFuture<Void> secondLinkedDelete = new CompletableFuture<>();
        AtomicInteger linkedDeletes = new AtomicInteger();
        when(objectStorage.delete(any())).thenAnswer(invocation -> {
            List<ObjectStorage.ObjectPath> paths = invocation.getArgument(0);
            String key = paths.get(0).key();
            if (key.equals(ObjectUtils.genKey(0, first.linkedObjectId()))) {
                linkedDeletes.incrementAndGet();
                return firstLinkedDelete.thenCompose(ignored -> memory.delete(paths));
            }
            if (key.equals(ObjectUtils.genKey(0, second.linkedObjectId()))) {
                linkedDeletes.incrementAndGet();
                return secondLinkedDelete.thenCompose(ignored -> memory.delete(paths));
            }
            if (key.equals(first.manifestKey())) {
                assertTrue(firstLinkedDelete.isDone());
            } else if (key.equals(second.manifestKey())) {
                assertTrue(secondLinkedDelete.isDone());
            }
            return memory.delete(paths);
        });
        StreamObjectArchiveCleanupTask task = StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 20L, 16L, 0L, 0L), new ArrayList<>()))
            .objectStorage(objectStorage).stream(stream(20L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache()).build();
        CompletableFuture<Void> cleanup = CompletableFuture.runAsync(() -> {
            try {
                task.cleanup();
            } catch (Exception exception) {
                throw new java.util.concurrent.CompletionException(exception);
            }
        });

        while (linkedDeletes.get() < 2) {
            Thread.onSpinWait();
        }
        assertFalse(cleanup.isDone());
        assertTrue(memory.contains(first.manifestKey()));
        assertTrue(memory.contains(second.manifestKey()));
        firstLinkedDelete.complete(null);
        secondLinkedDelete.complete(null);
        cleanup.get();

        assertFalse(memory.contains(first.manifestKey()));
        assertFalse(memory.contains(second.manifestKey()));
    }

    /**
     * Given physical deletion succeeds but cleanup commit fails, when a later round recovers the durable intent, then
     * missing objects are idempotent success and the persisted cleanup size is subtracted exactly once.
     */
    @Test
    void testCleanupRetriesPreparedIntentAfterCommitFailure() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        archivedComposite(objectStorage, 61L, 1061L, 0L, 10L, 7);
        StreamArchiveState initial = state(0L, 10L, 7L, 0L, 0L);
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(initial);
        AtomicInteger updates = new AtomicInteger();
        StreamManager manager = mock(StreamManager.class);
        when(manager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(manager.updateStreamArchive(any())).thenAnswer(invocation -> {
            StreamArchiveOperation operation = invocation.getArgument(0);
            StreamArchiveState update = applyOperation(current.get(), operation);
            if (updates.incrementAndGet() == 2) {
                return CompletableFuture.failedFuture(new IllegalStateException("commit unavailable"));
            }
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });
        StreamObjectArchiveCleanupTask task = StreamObjectArchiveCleanupTask.builder().streamManager(manager)
            .objectStorage(objectStorage).stream(stream(10L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache()).build();

        assertThrows(ExecutionException.class, task::cleanup);
        assertEquals(7L, current.get().archiveCleanupSize());
        assertEquals(7L, current.get().archiveSize());

        task.cleanup();

        assertEquals(10L, current.get().archiveStartOffset());
        assertEquals(0L, current.get().archiveSize());
        assertEquals(0L, current.get().archiveCleanupSize());
    }

    /**
     * Given a malformed key after a valid expired candidate, when cleanup lists the Stream prefix, then it stops this
     * Stream before persisting state or deleting any object.
     */
    @Test
    void testCleanupStopsBeforeStateChangeOrDeletionForMalformedKey() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite valid = archivedComposite(objectStorage, 41L, 1041L, 0L, 10L, 7);
        objectStorage.write(new WriteOptions(), ArchiveObjectKey.manifestPrefix(STREAM_ID) + "not-a-manifest",
            Unpooled.wrappedBuffer(new byte[1])).get();
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamObjectArchiveCleanupTask task = StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 20L, 7L, 0L, 0L), updates))
            .objectStorage(objectStorage)
            .stream(stream(20L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build();

        assertDoesNotThrow(task::cleanup);

        assertEquals(List.of(), updates);
        assertTrue(objectStorage.contains(valid.manifestKey()));
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, valid.linkedObjectId())));
    }

    /**
     * Given an unpublished Archive key after the published boundary, when cleanup lists the ordered prefix, then it
     * stops at that boundary and does not let later keys affect cleanup of the published range.
     */
    @Test
    void testCleanupStopsListingAtPublishedBoundary() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite published = archivedComposite(objectStorage, 42L, 1042L, 0L, 10L, 7);
        ArchivedComposite unpublished = archivedComposite(objectStorage, 43L, 1043L, 10L, 20L, 9);
        objectStorage.write(new WriteOptions(), ArchiveObjectKey.manifestPrefix(STREAM_ID) + "not-a-manifest",
            Unpooled.wrappedBuffer(new byte[1])).get();
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(0L, 10L, 7L, 0L, 0L), updates))
            .objectStorage(objectStorage)
            .stream(stream(10L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(2, updates.size());
        assertEquals(10L, updates.get(1).archiveStartOffset());
        assertFalse(objectStorage.contains(published.manifestKey()));
        assertTrue(objectStorage.contains(unpublished.manifestKey()));
    }

    /**
     * Given 101 expired Composites followed by a partially live Composite, when one cleanup invocation runs, then it
     * reclaims only the first 100 complete objects and leaves all later manifests for another round.
     */
    @Test
    void testCleanupSelectsAtMostOneHundredFullyExpiredComposites() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        List<ArchivedComposite> composites = new ArrayList<>();
        for (int i = 0; i < 102; i++) {
            composites.add(archivedComposite(objectStorage, 100L + i, 1100L + i, i * 10L,
                (i + 1) * 10L, 1));
        }
        List<StreamArchiveState> updates = new ArrayList<>();
        StreamArchiveState initial = state(0L, 1_020L, 102L, 0L, 0L);

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(initial, updates))
            .objectStorage(objectStorage)
            .stream(stream(1_015L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(1_000L, updates.get(0).archiveCleanupEndOffset());
        assertEquals(100L, updates.get(0).archiveCleanupSize());
        assertEquals(1_000L, updates.get(1).archiveStartOffset());
        assertEquals(2L, updates.get(1).archiveSize());
        assertFalse(objectStorage.contains(composites.get(99).manifestKey()));
        assertTrue(objectStorage.contains(composites.get(100).manifestKey()));
        assertTrue(objectStorage.contains(composites.get(101).manifestKey()));
    }

    /**
     * Given a manifest fully before the durable cleanup cursor, when cleanup runs, then it removes the stale
     * manifest without charging its size a second time and still cleans the next continuous expired object.
     */
    @Test
    void testCleanupRemovesStaleManifestsBeforeCursor() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        ArchivedComposite stale = archivedComposite(objectStorage, 200L, 1200L, 0L, 10L, 7);
        ArchivedComposite expired = archivedComposite(objectStorage, 201L, 1201L, 10L, 20L, 9);
        ArchivedComposite living = archivedComposite(objectStorage, 202L, 1202L, 20L, 30L, 11);
        List<StreamArchiveState> updates = new ArrayList<>();

        StreamObjectArchiveCleanupTask.builder()
            .streamManager(streamManager(state(10L, 30L, 20L, 10L, 0L), updates))
            .objectStorage(objectStorage)
            .stream(stream(20L))
            .cache(new StreamObjectArchiveCleanupTask.LeftBoundaryCache())
            .build()
            .cleanup();

        assertEquals(9L, updates.get(0).archiveCleanupSize());
        assertEquals(20L, updates.get(1).archiveStartOffset());
        assertEquals(11L, updates.get(1).archiveSize());
        assertFalse(objectStorage.contains(stale.manifestKey()));
        assertFalse(objectStorage.contains(expired.manifestKey()));
        assertTrue(objectStorage.contains(living.manifestKey()));
        assertFalse(objectStorage.contains(ObjectUtils.genKey(0, stale.linkedObjectId())));
    }

    private static ArchivedComposite archivedComposite(MemoryObjectStorage storage, long objectId,
        long linkedObjectId, long startOffset, long endOffset, int logicalSize) throws Exception {
        String linkedKey = ObjectUtils.genKey(0, linkedObjectId);
        storage.write(new WriteOptions(), linkedKey, Unpooled.wrappedBuffer(new byte[logicalSize])).get();
        S3ObjectMetadata linked = new S3ObjectMetadata(linkedObjectId,
            ObjectAttributes.builder().bucket(storage.bucketId()).build().attributes());
        DataBlockIndex index = new DataBlockIndex(STREAM_ID, startOffset, Math.toIntExact(endOffset - startOffset),
            1, 0L, logicalSize);
        String manifestKey = ArchiveObjectKey.manifestKey(STREAM_ID, startOffset, endOffset, objectId, logicalSize);
        CompositeObjectWriter writer = CompositeObject.writer(storage.writer(new WriteOptions(), manifestKey));
        writer.addComponent(linked, List.of(index));
        writer.close().get();
        return new ArchivedComposite(manifestKey, linkedObjectId);
    }

    private static S3ObjectMetadata archiveMetadata(MemoryObjectStorage storage, ArchivedComposite composite,
        long objectId, long startOffset, long endOffset) {
        return new S3ObjectMetadata(objectId, com.automq.stream.s3.metadata.S3ObjectType.COMPOSITE,
            List.of(new com.automq.stream.s3.metadata.StreamOffsetRange(STREAM_ID, startOffset, endOffset)),
            0L, 0L, 0L, 0L,
            ObjectAttributes.builder().bucket(storage.bucketId()).type(ObjectAttributes.Type.Composite).build()
                .attributes(), composite.manifestKey());
    }

    private static Stream stream(long startOffset) {
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(STREAM_ID);
        when(stream.streamEpoch()).thenReturn(STREAM_EPOCH);
        when(stream.startOffset()).thenReturn(startOffset);
        return stream;
    }

    private static StreamArchiveState state(long archiveStartOffset, long archiveEndOffset, long archiveSize,
        long cleanupEndOffset, long cleanupSize) {
        return new StreamArchiveState(STREAM_ID, STREAM_EPOCH, archiveStartOffset, archiveEndOffset,
            archiveEndOffset, archiveEndOffset, archiveSize, cleanupEndOffset, cleanupSize);
    }

    private static StreamArchiveState applyOperation(StreamArchiveState current, StreamArchiveOperation operation) {
        if (operation instanceof StreamArchiveOperation.CleanupPrepare prepare) {
            return current.toBuilder().archiveCleanupEndOffset(prepare.archiveCleanupEndOffset())
                .archiveCleanupSize(prepare.archiveCleanupSize()).build();
        }
        if (operation instanceof StreamArchiveOperation.CleanupCommit commit) {
            return current.toBuilder().archiveStartOffset(commit.archiveCleanupEndOffset())
                .archiveCleanupEndOffset(commit.archiveCleanupEndOffset())
                .archiveSize(current.archiveSize() - current.archiveCleanupSize()).archiveCleanupSize(0L).build();
        }
        throw new IllegalArgumentException("Unexpected cleanup operation: " + operation);
    }

    private static StreamManager streamManager(StreamArchiveState initial, List<StreamArchiveState> updates) {
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(initial);
        StreamManager manager = mock(StreamManager.class);
        when(manager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(manager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveOperation operation = invocation.getArgument(0);
            StreamArchiveState update = applyOperation(current.get(), operation);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });
        return manager;
    }

    private record ArchivedComposite(String manifestKey, long linkedObjectId) {
    }
}
