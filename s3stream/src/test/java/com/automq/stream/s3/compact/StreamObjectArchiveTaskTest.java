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
import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.s3.operator.Writer;
import com.automq.stream.s3.streams.StreamArchiveOperation;
import com.automq.stream.s3.streams.StreamArchiveState;
import com.automq.stream.s3.streams.StreamManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.compact.StreamObjectArchiveTask.MAX_OBJECTS_PER_BATCH;
import static com.automq.stream.s3.compact.StreamObjectArchiveTask.MAX_OBJECTS_WITH_LOOKAHEAD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class StreamObjectArchiveTaskTest {
    private static final long STREAM_ID = 7L;
    private static final long STREAM_EPOCH = 3L;
    private static final long COMPOSITE_TARGET_SIZE = 512L * 1024 * 1024;

    /**
     * Given consecutive terminal Composites at the Archive cursor, when ARCHIVE runs, then it durably prepares,
     * copies each manifest to its deterministic key, and publishes the complete logical range without deleting sources.
     */
    @Test
    void testArchivePreparesCopiesAndPublishesTerminalComposites() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata first = composite(objectStorage, 11L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        S3ObjectMetadata second = composite(objectStorage, 12L, 10L, 20L, COMPOSITE_TARGET_SIZE);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 100L, MAX_OBJECTS_WITH_LOOKAHEAD))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, second)));
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(STREAM_ID);
        when(stream.streamEpoch()).thenReturn(STREAM_EPOCH);
        when(stream.startOffset()).thenReturn(0L);
        when(stream.confirmOffset()).thenReturn(100L);

        StreamArchiveState initial = state(0L, 0L, 0L, 0L);
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(initial);
        ArchiveUpdates updates = new ArchiveUpdates();
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(streamManager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveOperation operation = invocation.getArgument(0);
            updates.operations.add(operation);
            StreamArchiveState update = applyOperation(current.get(), operation);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });

        StreamObjectArchiveTask.builder()
            .objectManager(objectManager)
            .streamManager(streamManager)
            .objectStorage(objectStorage)
            .stream(stream)
            .build()
            .archive();

        assertEquals(2, updates.size());
        assertEquals(List.of(11L, 12L), prepare(updates, 0).archiveObjectIds());
        assertEquals(20L, updates.get(0).archivePreparedEndOffset());
        assertEquals(20L, updates.get(1).archiveEndOffset());
        assertEquals(2 * COMPOSITE_TARGET_SIZE, updates.get(1).archiveSize());
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, 11L)));
        assertTrue(objectStorage.contains(ObjectUtils.genKey(0, 12L)));
        assertTrue(objectStorage.contains(ArchiveObjectKey.manifestKey(
            STREAM_ID, 0L, 10L, 11L, COMPOSITE_TARGET_SIZE)));
        assertTrue(objectStorage.contains(ArchiveObjectKey.manifestKey(
            STREAM_ID, 10L, 20L, 12L, COMPOSITE_TARGET_SIZE)));
    }

    /**
     * Given a durable prepared range after failover, when ARCHIVE recovers, then it rewrites the complete range and
     * publishes without issuing another prepare.
     */
    @Test
    void testArchiveRecoversPreparedRangeBeforeSelectingNewWork() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata preparedObject = composite(objectStorage, 21L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        String archiveKey = ArchiveObjectKey.manifestKey(STREAM_ID, 0L, 10L, 21L, COMPOSITE_TARGET_SIZE);
        Writer staleWriter = objectStorage.writer(new WriteOptions(), archiveKey);
        staleWriter.write(Unpooled.wrappedBuffer(new byte[] {1}));
        staleWriter.close().get();
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, MAX_OBJECTS_PER_BATCH))
            .thenReturn(CompletableFuture.completedFuture(List.of(preparedObject)));
        Stream stream = stream(0L, 100L);
        StreamArchiveState prepared = state(0L, 10L, 0L, 0L);
        ArchiveUpdates updates = new ArchiveUpdates();
        StreamManager streamManager = streamManager(prepared, updates);

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(objectStorage).stream(stream).build().archive();

        assertEquals(1, updates.size());
        assertEquals(10L, updates.get(0).archiveEndOffset());
        assertEquals(COMPOSITE_TARGET_SIZE, updates.get(0).archiveSize());
        assertTrue(objectStorage.contains(archiveKey));
        assertTrue(objectStorage.list(new com.automq.stream.s3.operator.ObjectStorage.ListOptions(archiveKey))
            .get().get(0).size() > 1L);
    }

    /**
     * Given retention has overtaken a durable prepared range, when ARCHIVE runs, then recovery still publishes the
     * prepared range before catch-up cleanup can drain it.
     */
    @Test
    void testArchiveRecoversPreparedRangeAfterRetentionOvertakesPublishedEnd() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata preparedObject = composite(objectStorage, 22L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, MAX_OBJECTS_PER_BATCH))
            .thenReturn(CompletableFuture.completedFuture(List.of(preparedObject)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 10L, 0L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(20L, 100L)).build().archive();

        assertEquals(1, updates.size());
        assertEquals(10L, updates.get(0).archiveEndOffset());
        assertEquals(COMPOSITE_TARGET_SIZE, updates.get(0).archiveSize());
    }

    /**
     * Given retention has overtaken published Archive data, when no prepared range exists, then ARCHIVE leaves new
     * online candidates untouched while retention and metadata cleanup drain the old range.
     */
    @Test
    void testArchiveDoesNotSelectNewWorkWhileOvertakenArchiveIsDraining() throws Exception {
        ObjectManager objectManager = mock(ObjectManager.class);
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenReturn(CompletableFuture.completedFuture(
            archiveState(0L, 0L, 10L, 10L, COMPOSITE_TARGET_SIZE)));

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(20L, 100L)).build().archive();

        verify(objectManager, never()).getStreamObjects(org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt());
        verify(streamManager, never()).updateStreamArchive(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Given retention cleanup has a durable intent, when ARCHIVE runs, then it yields without selecting or recovering
     * work so only one prepared state machine is active.
     */
    @Test
    void testArchiveYieldsToPreparedCleanup() throws Exception {
        ObjectManager objectManager = mock(ObjectManager.class);
        StreamManager streamManager = mock(StreamManager.class);
        StreamArchiveState cleanupPrepared = state(10L, 10L, 10L, 5L).toBuilder()
            .archiveCleanupEndOffset(5L)
            .build();
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH))
            .thenReturn(CompletableFuture.completedFuture(cleanupPrepared));

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(0L, 100L)).build().archive();

        verify(objectManager, never()).getStreamObjects(org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt());
        verify(streamManager, never()).updateStreamArchive(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Given retention has drained Archive data and metadata, when the first surviving online object overlaps Stream
     * start, then ARCHIVE aligns every empty cursor to that object's start without publishing the object.
     */
    @Test
    void testArchiveAdvancesEmptyCursorToOverlappingOnlineObject() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata surviving = composite(objectStorage, 23L, 50L, 150L, 1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, 1))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(0L, 0L, 0L, 0L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(100L, 200L)).build().archive();

        assertEquals(1, updates.size());
        assertEquals(50L, updates.get(0).archiveStartOffset());
        assertEquals(0L, updates.get(0).archiveMetadataEndOffset());
        assertEquals(50L, updates.get(0).archiveEndOffset());
        assertEquals(0L, updates.get(0).archiveSize());
        assertFalse(objectStorage.contains(ArchiveObjectKey.manifestKey(STREAM_ID, 50L, 150L, 23L, 1L)));
    }

    /**
     * Given an empty Archive and no living Stream Object, when retention is ahead, then ARCHIVE waits for new data.
     */
    @Test
    void testArchiveWaitsWithoutLivingStreamObject() throws Exception {
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, 1))
            .thenReturn(CompletableFuture.completedFuture(List.of()));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(0L, 0L, 0L, 0L, 0L), updates))
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(100L, 200L)).build().archive();

        assertTrue(updates.isEmpty());
    }

    /**
     * Given retention falls in a Stream Set Object, when ARCHIVE examines the living range, then it waits for split.
     */
    @Test
    void testArchiveWaitsForLivingStreamSetObjectSplit() throws Exception {
        S3ObjectMetadata streamSetObject = new S3ObjectMetadata(23L, S3ObjectType.STREAM_SET,
            List.of(new StreamOffsetRange(STREAM_ID, 50L, 150L)), 0L, 0L, 1L, -1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, 1))
            .thenReturn(CompletableFuture.completedFuture(List.of(streamSetObject)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(0L, 0L, 0L, 0L, 0L), updates))
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(100L, 200L)).build().archive();

        assertTrue(updates.isEmpty());
    }

    /**
     * Given a stale Image produces an empty-cursor proposal that the Controller rejects, when ARCHIVE runs, then the
     * task exits after that single update and leaves a later scheduler cycle to retry from a fresh Image.
     */
    @Test
    void testArchiveEmptyCursorConflictDoesNotRetryInTask() {
        S3ObjectMetadata surviving = new S3ObjectMetadata(23L, S3ObjectType.STREAM,
            List.of(new StreamOffsetRange(STREAM_ID, 50L, 150L)), 0L, 0L, 1L, -1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, 1))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenReturn(CompletableFuture.completedFuture(
            archiveState(0L, 0L, 0L, 0L, 0L)));
        when(streamManager.updateStreamArchive(org.mockito.ArgumentMatchers.any()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("stale Archive state")));

        assertThrows(ExecutionException.class, () -> StreamObjectArchiveTask.builder()
            .objectManager(objectManager).streamManager(streamManager)
            .objectStorage(new MemoryObjectStorage((short) 4)).stream(stream(100L, 200L)).build().archive());

        verify(streamManager).updateStreamArchive(org.mockito.ArgumentMatchers.any());
    }

    /**
     * Given an empty cursor aligned below Stream start, when the overlapping Composite is smaller than the normal
     * terminal target, then ARCHIVE copies and publishes it through the normal prepare/publish transitions.
     */
    @Test
    void testArchivePublishesOverlappingCompositeAfterEmptyCursorAdvance() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata surviving = composite(objectStorage, 24L, 50L, 150L, 1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, 1))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(archiveState(50L, 50L, 50L, 50L, 0L), updates))
            .objectStorage(objectStorage).stream(stream(100L, 200L)).build().archive();

        assertEquals(2, updates.size());
        assertEquals(List.of(24L), prepare(updates, 0).archiveObjectIds());
        assertEquals(150L, updates.get(1).archiveEndOffset());
        assertEquals(1L, updates.get(1).archiveSize());
        assertTrue(objectStorage.contains(ArchiveObjectKey.manifestKey(STREAM_ID, 50L, 150L, 24L, 1L)));
    }

    /**
     * Given retention overtakes a prepared Archive, when successive lifecycle rounds recover, drain, align, and
     * resume, then the prepared range publishes first and the overlapping survivor publishes only after empty-cursor
     * alignment.
     */
    @Test
    void testArchiveConvergesAfterRetentionOvertakesPreparedWork() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata prepared = composite(objectStorage, 25L, 10L, 20L, 1L);
        S3ObjectMetadata surviving = composite(objectStorage, 26L, 50L, 150L, 1L);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 10L, 20L, MAX_OBJECTS_PER_BATCH))
            .thenReturn(CompletableFuture.completedFuture(List.of(prepared)));
        when(objectManager.getStreamObjects(STREAM_ID, 100L, 200L, 1))
            .thenReturn(CompletableFuture.completedFuture(List.of(surviving)));
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(
            archiveState(0L, 0L, 10L, 20L, 1L));
        ArchiveUpdates updates = new ArchiveUpdates();
        StreamManager streamManager = mock(StreamManager.class);
        when(streamManager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(streamManager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveOperation operation = invocation.getArgument(0);
            updates.operations.add(operation);
            StreamArchiveState update = applyOperation(current.get(), operation);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });
        StreamObjectArchiveTask task = StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager).objectStorage(objectStorage).stream(stream(100L, 200L)).build();

        task.archive();
        assertEquals(20L, current.get().archiveEndOffset());
        current.set(archiveState(20L, 20L, 20L, 20L, 0L));
        task.archive();
        assertEquals(50L, current.get().archiveEndOffset());
        assertEquals(20L, current.get().archiveMetadataEndOffset());
        assertEquals(0L, current.get().archiveSize());
        // Simulate the Controller-owned metadata cleanup catching up after the Broker advances the empty cursor.
        current.set(current.get().toBuilder().archiveMetadataEndOffset(50L).build());
        task.archive();

        assertEquals(150L, current.get().archiveEndOffset());
        assertEquals(1L, current.get().archiveSize());
        assertEquals(List.of(26L), prepare(updates, updates.operations.size() - 2).archiveObjectIds());
        assertTrue(objectStorage.contains(ArchiveObjectKey.manifestKey(STREAM_ID, 50L, 150L, 26L, 1L)));
    }

    /**
     * Given a prepared Archive batch whose manifest copy fails, when ARCHIVE runs, then the prepared state remains
     * unpublished so a later task can recover the whole batch.
     */
    @Test
    void testArchiveCopyFailurePreventsPublication() throws Exception {
        MemoryObjectStorage sourceStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata object = composite(sourceStorage, 31L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        com.automq.stream.s3.operator.ObjectStorage objectStorage = mock(
            com.automq.stream.s3.operator.ObjectStorage.class);
        com.automq.stream.s3.operator.ObjectStorage primary = mock(
            com.automq.stream.s3.operator.ObjectStorage.class);
        when(objectStorage.primary()).thenReturn(primary);
        when(objectStorage.bucketURI((short) 4)).thenReturn(BucketURI.parse("4@s3://source-bucket"));
        when(objectStorage.rangeRead(org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(object.key()), org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong())).thenAnswer(invocation -> sourceStorage.rangeRead(
                invocation.getArgument(0), object.key(), invocation.getArgument(2), invocation.getArgument(3)));
        when(primary.copy(org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(object.key()), org.mockito.ArgumentMatchers.anyString()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("copy failed")));
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, MAX_OBJECTS_PER_BATCH))
            .thenReturn(CompletableFuture.completedFuture(List.of(object)));
        ArchiveUpdates updates = new ArchiveUpdates();
        StreamManager streamManager = streamManager(state(0L, 10L, 0L, 0L), updates);

        assertThrows(ExecutionException.class, () -> StreamObjectArchiveTask.builder()
            .objectManager(objectManager).streamManager(streamManager).objectStorage(objectStorage)
            .stream(stream(0L, 100L)).build().archive());

        verify(primary).copy(org.mockito.ArgumentMatchers.eq("source-bucket"),
            org.mockito.ArgumentMatchers.eq(object.key()),
            org.mockito.ArgumentMatchers.anyString());
        assertEquals(List.of(), updates);
    }

    /**
     * Given more than 100 consecutive terminal Composites, when ARCHIVE selects a fresh batch, then prepare contains
     * exactly the first 100 objects and leaves the remainder for a later round.
     */
    @Test
    void testArchivePrepareIsBoundedToOneHundredComposites() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        List<S3ObjectMetadata> objects = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            objects.add(composite(objectStorage, 100L + i, i * 10L, (i + 1) * 10L, COMPOSITE_TARGET_SIZE));
        }
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 2_000L, MAX_OBJECTS_WITH_LOOKAHEAD))
            .thenReturn(CompletableFuture.completedFuture(objects));
        ArchiveUpdates updates = new ArchiveUpdates();
        StreamManager streamManager = streamManager(state(0L, 0L, 0L, 0L), updates);

        StreamObjectArchiveTask.builder().objectManager(objectManager).streamManager(streamManager)
            .objectStorage(objectStorage).stream(stream(0L, 2_000L)).build().archive();

        assertEquals(100, prepare(updates, 0).archiveObjectIds().size());
        assertEquals(1_000L, updates.get(0).archivePreparedEndOffset());
        assertEquals(1_000L, updates.get(1).archiveEndOffset());
    }

    /**
     * Given a small trailing Composite with no stable merge constraint, when ARCHIVE selects candidates, then the
     * temporarily unscheduled object remains online and no Archive transition is attempted.
     */
    @Test
    void testArchiveDoesNotSelectTemporarilyUnscheduledComposite() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata small = composite(objectStorage, 41L, 0L, 10L, COMPOSITE_TARGET_SIZE / 2);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 100L, MAX_OBJECTS_WITH_LOOKAHEAD))
            .thenReturn(CompletableFuture.completedFuture(List.of(small)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 0L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, 100L)).build().archive();

        assertEquals(List.of(), updates);
    }

    /**
     * Given two small Composites whose combined offset delta cannot be represented by MAJOR_V1, when ARCHIVE selects
     * candidates, then the first Composite is terminal even though the retained-size target has not been reached.
     */
    @Test
    void testArchiveSelectsCompositeAtStableOffsetDeltaBoundary() throws Exception {
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        long boundary = Integer.MAX_VALUE;
        S3ObjectMetadata first = composite(objectStorage, 42L, 0L, boundary, COMPOSITE_TARGET_SIZE / 4);
        S3ObjectMetadata second = composite(objectStorage, 43L, boundary, boundary + 1,
            COMPOSITE_TARGET_SIZE / 4);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, boundary + 1, MAX_OBJECTS_WITH_LOOKAHEAD))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, second)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 0L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, boundary + 1)).build().archive();

        assertEquals(List.of(42L), prepare(updates, 0).archiveObjectIds());
        assertEquals(boundary, updates.get(1).archiveEndOffset());
    }

    /**
     * Given an old Normal Stream Object under high metadata pressure, when ARCHIVE runs, then server-side copy keeps
     * the complete object and publishes a Normal Archive key while retaining the online source.
     */
    @Test
    void testArchiveCopiesNormalObject() throws Exception {
        long now = TimeUnit.DAYS.toMillis(10);
        MemoryObjectStorage objectStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata object = normal(objectStorage, 71L, 0L, 10L, now - TimeUnit.HOURS.toMillis(2));
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 10L, MAX_OBJECTS_WITH_LOOKAHEAD))
            .thenReturn(CompletableFuture.completedFuture(List.of(object)));
        ArchiveUpdates updates = new ArchiveUpdates();

        StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 0L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, 10L)).pressure(StreamObjectArchiveTask.Pressure.HIGH)
            .currentTimeMillis(() -> now).build().archive();

        assertEquals(2, updates.size());
        assertEquals(List.of(71L), prepare(updates, 0).archiveObjectIds());
        List<com.automq.stream.s3.operator.ObjectStorage.ObjectInfo> archived = objectStorage.list(
            new com.automq.stream.s3.operator.ObjectStorage.ListOptions(
                com.automq.stream.s3.metadata.ArchiveObjectKey.manifestPrefix(STREAM_ID))).get();
        assertEquals(1, archived.size());
        com.automq.stream.s3.metadata.ArchiveObjectKey.ManifestKey archiveKey =
            com.automq.stream.s3.metadata.ArchiveObjectKey.parseManifestKey(archived.get(0).key());
        assertEquals(ObjectAttributes.Type.Normal, archiveKey.type());
        assertEquals(object.objectSize(), archiveKey.objectSize());
        assertEquals(object.objectSize(), updates.get(1).archiveSize());
        assertTrue(objectStorage.contains(object.key()));
    }

    /**
     * Given a small object with no stable merge boundary, when pressure age policies are applied, then MEDIUM uses
     * 24 hours and HIGH uses one hour as inclusive Archive thresholds.
     */
    @Test
    void testArchivePressureAgeThresholds() {
        long now = TimeUnit.DAYS.toMillis(10);

        assertFalse(StreamObjectArchiveTask.Pressure.MEDIUM.isOldEnough(
            now - TimeUnit.HOURS.toMillis(24) + 1, now));
        assertTrue(StreamObjectArchiveTask.Pressure.MEDIUM.isOldEnough(
            now - TimeUnit.HOURS.toMillis(24), now));
        assertFalse(StreamObjectArchiveTask.Pressure.HIGH.isOldEnough(
            now - TimeUnit.HOURS.toMillis(1) + 1, now));
        assertTrue(StreamObjectArchiveTask.Pressure.HIGH.isOldEnough(
            now - TimeUnit.HOURS.toMillis(1), now));
        assertFalse(StreamObjectArchiveTask.Pressure.LOW.isOldEnough(0L, now));
    }

    /**
     * Given a prepared batch with independent delayed copies, when Archive recovers it, then all object copies may
     * remain concurrently in flight and publication waits for every copy.
     */
    @Test
    void testArchiveCopiesObjectsConcurrently() throws Exception {
        MemoryObjectStorage sourceStorage = new MemoryObjectStorage((short) 4);
        S3ObjectMetadata first = composite(sourceStorage, 61L, 0L, 10L, COMPOSITE_TARGET_SIZE);
        S3ObjectMetadata second = composite(sourceStorage, 62L, 10L, 20L, COMPOSITE_TARGET_SIZE);
        CompletableFuture<Void> firstCopy = new CompletableFuture<>();
        CompletableFuture<Void> secondCopy = new CompletableFuture<>();
        com.automq.stream.s3.operator.ObjectStorage objectStorage = mock(
            com.automq.stream.s3.operator.ObjectStorage.class);
        when(objectStorage.bucketId()).thenReturn((short) 4);
        when(objectStorage.primary()).thenReturn(objectStorage);
        when(objectStorage.bucketURI((short) 4)).thenReturn(BucketURI.parse("4@s3://source-bucket"));
        when(objectStorage.rangeRead(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenAnswer(invocation -> sourceStorage.rangeRead(invocation.getArgument(0), invocation.getArgument(1),
                invocation.getArgument(2), invocation.getArgument(3)));
        when(objectStorage.copy(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.anyString())).thenAnswer(invocation ->
                invocation.<String>getArgument(1).equals(first.key()) ? firstCopy : secondCopy);
        ObjectManager objectManager = mock(ObjectManager.class);
        when(objectManager.getStreamObjects(STREAM_ID, 0L, 20L, MAX_OBJECTS_PER_BATCH))
            .thenReturn(CompletableFuture.completedFuture(List.of(first, second)));
        ArchiveUpdates updates = new ArchiveUpdates();
        StreamObjectArchiveTask task = StreamObjectArchiveTask.builder().objectManager(objectManager)
            .streamManager(streamManager(state(0L, 20L, 0L, 0L), updates)).objectStorage(objectStorage)
            .stream(stream(0L, 100L)).build();
        CompletableFuture<Void> archive = CompletableFuture.runAsync(() -> {
            try {
                task.archive();
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        });

        String firstKey = ArchiveObjectKey.manifestKey(STREAM_ID, 0L, 10L, 61L, COMPOSITE_TARGET_SIZE);
        String secondKey = ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 20L, 62L, COMPOSITE_TARGET_SIZE);
        verify(objectStorage, timeout(1_000)).copy(org.mockito.ArgumentMatchers.eq("source-bucket"),
            org.mockito.ArgumentMatchers.eq(first.key()),
            org.mockito.ArgumentMatchers.eq(firstKey));
        verify(objectStorage, timeout(1_000)).copy(org.mockito.ArgumentMatchers.eq("source-bucket"),
            org.mockito.ArgumentMatchers.eq(second.key()),
            org.mockito.ArgumentMatchers.eq(secondKey));
        assertFalse(archive.isDone());

        firstCopy.complete(null);
        secondCopy.complete(null);
        archive.get();
        assertEquals(1, updates.size());
        assertEquals(20L, updates.get(0).archiveEndOffset());
    }

    private static S3ObjectMetadata composite(MemoryObjectStorage storage, long objectId, long startOffset,
        long endOffset, long logicalSize) throws Exception {
        S3ObjectMetadata linked = new S3ObjectMetadata(objectId + 1000,
            ObjectAttributes.builder().bucket(storage.bucketId()).build().attributes());
        DataBlockIndex index = new DataBlockIndex(STREAM_ID, startOffset, Math.toIntExact(endOffset - startOffset),
            1, 0L, Math.toIntExact(logicalSize));
        CompositeObjectWriter writer = CompositeObject.writer(storage.writer(new WriteOptions(),
            ObjectUtils.genKey(0, objectId)));
        writer.addComponent(linked, List.of(index));
        writer.close().get();
        return new S3ObjectMetadata(objectId, S3ObjectType.COMPOSITE,
            List.of(new StreamOffsetRange(STREAM_ID, startOffset, endOffset)), 0L, 0L, writer.size(), objectId,
            ObjectAttributes.builder().bucket(storage.bucketId()).type(ObjectAttributes.Type.Composite).build()
                .attributes());
    }

    private static S3ObjectMetadata normal(MemoryObjectStorage storage, long objectId, long startOffset,
        long endOffset, long committedTimestamp) throws Exception {
        ObjectWriter writer = ObjectWriter.writer(objectId, storage, Integer.MAX_VALUE, Integer.MAX_VALUE);
        StreamRecordBatch batch = StreamRecordBatch.of(STREAM_ID, 0L, startOffset,
            Math.toIntExact(endOffset - startOffset), TestUtils.random(16), DefaultByteBufSupplier.INSTANCE);
        writer.write(STREAM_ID, List.of(batch));
        writer.close().get();
        batch.release();
        return new S3ObjectMetadata(objectId, S3ObjectType.STREAM,
            List.of(new StreamOffsetRange(STREAM_ID, startOffset, endOffset)), committedTimestamp,
            committedTimestamp, writer.size(), objectId,
            ObjectAttributes.builder().bucket(storage.bucketId()).type(ObjectAttributes.Type.Normal).build()
                .attributes());
    }

    private static StreamArchiveState state(long archiveEndOffset, long preparedEndOffset, long archiveSize,
        long cleanupSize) {
        return new StreamArchiveState(STREAM_ID, STREAM_EPOCH, 0L, 0L, archiveEndOffset, preparedEndOffset,
            archiveSize, 0L, cleanupSize);
    }

    private static StreamArchiveState archiveState(long archiveStartOffset, long archiveMetadataEndOffset,
        long archiveEndOffset, long archivePreparedEndOffset, long archiveSize) {
        return new StreamArchiveState(STREAM_ID, STREAM_EPOCH, archiveStartOffset, archiveMetadataEndOffset,
            archiveEndOffset, archivePreparedEndOffset, archiveSize, archiveStartOffset, 0L);
    }

    private static Stream stream(long startOffset, long confirmOffset) {
        Stream stream = mock(Stream.class);
        when(stream.streamId()).thenReturn(STREAM_ID);
        when(stream.streamEpoch()).thenReturn(STREAM_EPOCH);
        when(stream.startOffset()).thenReturn(startOffset);
        when(stream.confirmOffset()).thenReturn(confirmOffset);
        return stream;
    }

    private static StreamArchiveState applyOperation(StreamArchiveState current, StreamArchiveOperation operation) {
        if (operation instanceof StreamArchiveOperation.ArchivePrepare prepare) {
            return current.toBuilder().archivePreparedEndOffset(prepare.archivePreparedEndOffset())
                .build();
        }
        if (operation instanceof StreamArchiveOperation.ArchivePublish publish) {
            return current.toBuilder().archiveEndOffset(publish.archiveEndOffset())
                .archivePreparedEndOffset(publish.archiveEndOffset()).archiveSize(publish.archiveSize())
                .build();
        }
        if (operation instanceof StreamArchiveOperation.AdvanceEmptyCursor advance) {
            return current.toBuilder().archiveStartOffset(advance.newArchiveOffset())
                .archiveEndOffset(advance.newArchiveOffset()).archivePreparedEndOffset(advance.newArchiveOffset())
                .archiveCleanupEndOffset(advance.newArchiveOffset()).archiveCleanupSize(0L).archiveSize(0L).build();
        }
        throw new IllegalArgumentException("Unexpected Archive operation: " + operation);
    }

    private static StreamManager streamManager(StreamArchiveState initial, ArchiveUpdates updates) {
        AtomicReference<StreamArchiveState> current = new AtomicReference<>(initial);
        StreamManager manager = mock(StreamManager.class);
        when(manager.getStreamArchive(STREAM_ID, STREAM_EPOCH)).thenAnswer(ignored ->
            CompletableFuture.completedFuture(current.get()));
        when(manager.updateStreamArchive(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            StreamArchiveOperation operation = invocation.getArgument(0);
            updates.operations.add(operation);
            StreamArchiveState update = applyOperation(current.get(), operation);
            updates.add(update);
            current.set(update);
            return CompletableFuture.completedFuture(null);
        });
        return manager;
    }

    private static StreamArchiveOperation.ArchivePrepare prepare(ArchiveUpdates updates, int index) {
        return (StreamArchiveOperation.ArchivePrepare) updates.operations.get(index);
    }

    private static final class ArchiveUpdates extends ArrayList<StreamArchiveState> {
        private final List<StreamArchiveOperation> operations = new ArrayList<>();
    }
}
