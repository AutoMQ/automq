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

package kafka.log.stream.s3.metadata;

import kafka.server.BrokerServer;
import kafka.server.metadata.KRaftMetadataCache;

import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.S3ObjectRecord;
import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.MetadataLoader;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.S3ObjectState;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.api.ReadOptions;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.failover.StorageFailureHandler;
import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.metadata.ArchiveObjectKey;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.impl.MemoryWriteAheadLog;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class StreamMetadataManagerTest {
    private static final long STREAM_ID = 7L;
    private static final int SOURCE_NODE_ID = 1;
    private static final int TARGET_NODE_ID = 2;

    /**
     * Given a published Archive range, when Fetch is wholly before the published boundary, then ordinary metadata
     * carries the archived manifest's physical location and Composite retained logical size.
     */
    @Test
    public void testFetchArchiveOnlyReturnsOrdinaryMetadataWithPhysicalKey() throws Exception {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 30L);
        replayArchive(delta, 10L, 30L, 30L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        ObjectStorage primary = mock(ObjectStorage.class);
        when(storage.primary()).thenReturn(primary);
        String firstKey = ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 20L, 101L, 999L);
        String secondKey = ArchiveObjectKey.manifestKey(STREAM_ID, 20L, 30L, 102L, 888L);
        when(primary.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of(
            new ObjectStorage.ObjectInfo((short) 0, firstKey, 11L, 123L),
            new ObjectStorage.ObjectInfo((short) 0, secondKey, 12L, 124L))));
        StreamMetadataManager manager = newManager(image, storage);

        InRangeObjects result = manager.fetch(STREAM_ID, 12L, 30L, 2).get(1, TimeUnit.SECONDS);

        assertEquals(List.of(101L, 102L), result.objects().stream()
            .map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        S3ObjectMetadata first = result.objects().get(0);
        assertEquals(S3ObjectType.COMPOSITE, first.getType());
        assertEquals(firstKey, first.key());
        assertEquals(999L, first.objectSize());
        assertEquals(11L, first.committedTimestamp());
        assertEquals(S3StreamConstant.INVALID_TS, first.dataTimeInMs());
        assertEquals((short) 0, first.bucket());
    }

    /**
     * Given a Normal object in the published Archive range, when Fetch lists its key, then metadata preserves the
     * ordinary Stream Object type so the normal reader consumes the complete copied object.
     */
    @Test
    public void testFetchArchiveNormalObjectRestoresStreamType() throws Exception {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 20L);
        replayArchive(delta, 10L, 20L, 20L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        String key = ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 20L, ObjectAttributes.Type.Normal,
            101L, 123L);
        when(storage.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of(
            new ObjectStorage.ObjectInfo((short) 0, key, 11L, 123L))));

        InRangeObjects result = newManager(image, storage).fetch(STREAM_ID, 10L, 20L, 1)
            .get(1, TimeUnit.SECONDS);

        assertEquals(1, result.objects().size());
        assertEquals(S3ObjectType.STREAM, result.objects().get(0).getType());
        assertEquals(ObjectAttributes.Type.Normal,
            ObjectAttributes.from(result.objects().get(0).attributes()).type());
        assertEquals(key, result.objects().get(0).key());
        assertEquals(123L, result.objects().get(0).objectSize());
    }

    /**
     * Given Fetch starts at the published Archive boundary, when online metadata covers the request, then the
     * existing online path returns it without listing or exposing a source distinction.
     */
    @Test
    public void testFetchOnlineOnlyDoesNotListArchive() throws Exception {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 30L);
        replayObject(delta, 201L, TARGET_NODE_ID, 20L, 30L);
        replayArchive(delta, 10L, 20L, 20L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        StreamMetadataManager manager = newManager(image, storage);

        InRangeObjects result = manager.fetch(STREAM_ID, 20L, 30L, 2).get(1, TimeUnit.SECONDS);

        assertEquals(List.of(201L), result.objects().stream()
            .map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        verify(storage, never()).list(any(ObjectStorage.ListOptions.class));
    }

    /**
     * Given publication is followed by a prepared copy and online data, when Fetch crosses the captured boundary,
     * then it ignores the prepared copy and appends online metadata from that same Image under one shared limit.
     */
    @Test
    public void testFetchCrossBoundaryUsesCapturedImageAndSharedLimit() throws Exception {
        MetadataDelta initialDelta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(initialDelta, 10L, 30L);
        replayObject(initialDelta, 201L, TARGET_NODE_ID, 20L, 30L);
        replayArchive(initialDelta, 10L, 20L, 30L);
        MetadataImage initialImage = initialDelta.apply(new MetadataProvenance(1L, 0, 1L));
        CompletableFuture<List<ObjectStorage.ObjectInfo>> listFuture = new CompletableFuture<>();
        ObjectStorage storage = mock(ObjectStorage.class);
        when(storage.list(any(ObjectStorage.ListOptions.class))).thenReturn(listFuture);
        StreamMetadataManager manager = newManager(initialImage, storage);

        CompletableFuture<InRangeObjects> fetch = manager.fetch(STREAM_ID, 10L, 30L, 2);
        MetadataDelta replacementDelta = new MetadataDelta(initialImage);
        replayObject(replacementDelta, 202L, TARGET_NODE_ID, 20L, 30L);
        MetadataImage replacementImage = replacementDelta.apply(new MetadataProvenance(2L, 0, 2L));
        manager.onMetadataUpdate(replacementDelta, replacementImage, mock(LoaderManifest.class));
        String publishedKey = ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 20L, 101L, 100L);
        String preparedKey = ArchiveObjectKey.manifestKey(STREAM_ID, 20L, 30L, 102L, 100L);
        listFuture.complete(List.of(
            new ObjectStorage.ObjectInfo((short) 0, publishedKey, 11L, 123L),
            new ObjectStorage.ObjectInfo((short) 0, preparedKey, 12L, 124L)));

        InRangeObjects result = fetch.get(1, TimeUnit.SECONDS);
        assertEquals(List.of(101L, 201L), result.objects().stream()
            .map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
        org.mockito.ArgumentCaptor<ObjectStorage.ListOptions> options =
            org.mockito.ArgumentCaptor.forClass(ObjectStorage.ListOptions.class);
        verify(storage).list(options.capture());
        assertEquals(ArchiveObjectKey.manifestPrefix(STREAM_ID), options.getValue().prefix());
        assertEquals(ArchiveObjectKey.startAfter(STREAM_ID, 10L), options.getValue().startAfter());
        assertEquals(2, options.getValue().maxKeys());
    }

    /**
     * Given published Archive metadata is not physically covered, when LIST succeeds, then Fetch reports corruption
     * immediately instead of falling back to online metadata or waiting for another Image.
     */
    @Test
    public void testFetchMissingPublishedArchiveCoverageFails() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 30L);
        replayObject(delta, 201L, TARGET_NODE_ID, 20L, 30L);
        replayArchive(delta, 10L, 20L, 20L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        when(storage.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of()));
        StreamMetadataManager manager = newManager(image, storage);

        CompletionException exception = assertThrows(CompletionException.class,
            () -> manager.fetch(STREAM_ID, 10L, 30L, 4).join());

        assertInstanceOf(ArchiveMissingCoverageException.class, exception.getCause());
        assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forException(exception.getCause()));
    }

    /**
     * Given malformed or discontinuous Archive keys, when Fetch parses the published range, then each request fails
     * with its dedicated corruption category and does not alter other Stream state.
     */
    @Test
    public void testFetchMalformedAndDiscontinuousArchiveRangesFail() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 30L);
        replayArchive(delta, 10L, 30L, 30L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage malformedStorage = mock(ObjectStorage.class);
        when(malformedStorage.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of(
            new ObjectStorage.ObjectInfo((short) 0,
                ArchiveObjectKey.manifestPrefix(STREAM_ID) + "not-a-manifest", 11L, 123L))));
        CompletionException malformed = assertThrows(CompletionException.class,
            () -> newManager(image, malformedStorage).fetch(STREAM_ID, 10L, 30L, 4).join());
        assertInstanceOf(ArchiveMalformedKeyException.class, malformed.getCause());

        ObjectStorage discontinuousStorage = mock(ObjectStorage.class);
        when(discontinuousStorage.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of(
            new ObjectStorage.ObjectInfo((short) 0,
                ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 20L, 101L, 100L), 11L, 123L),
            new ObjectStorage.ObjectInfo((short) 0,
                ArchiveObjectKey.manifestKey(STREAM_ID, 21L, 30L, 102L, 100L), 12L, 124L))));
        CompletionException discontinuous = assertThrows(CompletionException.class,
            () -> newManager(image, discontinuousStorage).fetch(STREAM_ID, 10L, 30L, 4).join());
        assertInstanceOf(ArchiveDiscontinuousRangeException.class, discontinuous.getCause());
    }

    /**
     * Given a listed Archive Object crosses the captured publication boundary, when Fetch validates it, then the
     * request fails as corruption rather than exposing any prepared suffix.
     */
    @Test
    public void testFetchRejectsArchiveObjectCrossingPublishedBoundary() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 30L);
        replayArchive(delta, 10L, 20L, 30L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        when(storage.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of(
            new ObjectStorage.ObjectInfo((short) 0,
                ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 25L, 101L, 100L), 11L, 123L))));

        CompletionException exception = assertThrows(CompletionException.class,
            () -> newManager(image, storage).fetch(STREAM_ID, 10L, 30L, 4).join());

        assertInstanceOf(ArchiveDiscontinuousRangeException.class, exception.getCause());
    }

    /**
     * Given Archive LIST fails at the object-storage boundary, when Fetch observes the failure, then it propagates the
     * error without applying a second retry policy or entering metadata pending-fetch retry.
     */
    @Test
    public void testFetchArchiveListFailureDoesNotWaitForMetadataUpdate() {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 30L);
        replayArchive(delta, 10L, 20L, 20L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        IllegalStateException listFailure = new IllegalStateException("LIST unavailable");
        when(storage.list(any(ObjectStorage.ListOptions.class)))
            .thenReturn(CompletableFuture.failedFuture(listFailure));
        StreamMetadataManager manager = newManager(image, storage);

        CompletionException exception = assertThrows(CompletionException.class,
            () -> manager.fetch(STREAM_ID, 10L, 30L, 4).join());

        assertEquals(listFailure, exception.getCause());
        verify(storage).list(any(ObjectStorage.ListOptions.class));
    }

    /**
     * Given an empty requested range begins inside Archive, when Fetch runs, then it returns an empty result without
     * listing or consuming an Archive manifest.
     */
    @Test
    public void testFetchArchiveEqualOffsetsReturnsEmptyResult() throws Exception {
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(delta, 10L, 20L);
        replayArchive(delta, 10L, 20L, 20L);
        MetadataImage image = delta.apply(new MetadataProvenance(1L, 0, 1L));
        ObjectStorage storage = mock(ObjectStorage.class);
        String key = ArchiveObjectKey.manifestKey(STREAM_ID, 10L, 20L, 101L, 100L);
        when(storage.list(any(ObjectStorage.ListOptions.class))).thenReturn(CompletableFuture.completedFuture(List.of(
            new ObjectStorage.ObjectInfo((short) 0, key, 11L, 123L))));

        InRangeObjects result = newManager(image, storage).fetch(STREAM_ID, 12L, 12L, 4)
            .get(1, TimeUnit.SECONDS);

        assertEquals(STREAM_ID, result.streamId());
        assertEquals(List.of(), result.objects());
        verify(storage, never()).list(any(ObjectStorage.ListOptions.class));
    }

    /**
     * Given logical end and current-owner object metadata ahead of a historical hole, when the historical upload
     * commits, then the pending fetch retries and returns continuous coverage across the ownership boundary.
     */
    @Test
    public void testFetchWaitsForHistoricalCoverageAndRetriesAfterMetadataUpdate() throws Exception {
        MetadataDelta initialDelta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(initialDelta, 10L, 30L);
        replayObject(initialDelta, 2L, TARGET_NODE_ID, 20L, 30L);
        MetadataImage initialImage = initialDelta.apply(new MetadataProvenance(1L, 0, 1L));
        StreamMetadataManager manager = newManager(initialImage);

        CompletableFuture<InRangeObjects> fetch = manager.fetch(STREAM_ID, 10L, 30L, 4);

        assertFalse(fetch.isDone());

        MetadataDelta historicalCommit = new MetadataDelta(initialImage);
        replayObject(historicalCommit, 1L, SOURCE_NODE_ID, 10L, 20L);
        MetadataImage coveredImage = historicalCommit.apply(new MetadataProvenance(2L, 0, 2L));
        manager.onMetadataUpdate(historicalCommit, coveredImage, mock(LoaderManifest.class));

        InRangeObjects objects = fetch.get(1, TimeUnit.SECONDS);
        assertEquals(10L, objects.startOffset());
        assertEquals(30L, objects.endOffset());
        assertEquals(List.of(1L, 2L), objects.objects().stream()
            .map(metadata -> metadata.objectId()).collect(Collectors.toList()));
    }

    /**
     * Given ordinary and snapshot reads observe logical end ahead of historical coverage, when the historical object
     * metadata commits, then both S3Storage paths retry automatically and return the same continuous records.
     */
    @Test
    public void testOrdinaryAndSnapshotReadsConvergeAfterHistoricalCommit() throws Exception {
        MetadataDelta initialDelta = new MetadataDelta(MetadataImage.EMPTY);
        replayStream(initialDelta, 10L, 30L);
        replayObject(initialDelta, 2L, TARGET_NODE_ID, 20L, 30L);
        MetadataImage initialImage = initialDelta.apply(new MetadataProvenance(1L, 0, 1L));
        StreamMetadataManager manager = newManager(initialImage);
        S3BlockCache blockCache = (context, streamId, startOffset, endOffset, maxBytes) ->
            manager.fetch(streamId, startOffset, endOffset, 4)
                .thenApply(objects -> new ReadDataBlock(List.of(StreamRecordBatch.of(streamId, 0L, startOffset,
                    Math.toIntExact(endOffset - startOffset), java.nio.ByteBuffer.wrap(new byte[1]),
                    DefaultByteBufSupplier.INSTANCE)), CacheAccessType.BLOCK_CACHE_MISS));
        S3Storage storage = new S3Storage(new Config(), new MemoryWriteAheadLog(), mock(StreamManager.class),
            mock(ObjectManager.class), blockCache, new MemoryObjectStorage(), mock(StorageFailureHandler.class));
        FetchContext snapshotContext = new FetchContext();
        snapshotContext.setReadOptions(ReadOptions.builder().snapshotRead(true).build());

        CompletableFuture<ReadDataBlock> ordinaryRead = storage.read(STREAM_ID, 10L, 30L, 1024);
        CompletableFuture<ReadDataBlock> snapshotRead = storage.read(snapshotContext, STREAM_ID, 10L, 30L, 1024);

        assertFalse(ordinaryRead.isDone());
        assertFalse(snapshotRead.isDone());
        MetadataDelta historicalCommit = new MetadataDelta(initialImage);
        replayObject(historicalCommit, 1L, SOURCE_NODE_ID, 10L, 20L);
        MetadataImage coveredImage = historicalCommit.apply(new MetadataProvenance(2L, 0, 2L));
        manager.onMetadataUpdate(historicalCommit, coveredImage, mock(LoaderManifest.class));
        ReadDataBlock ordinaryResult = ordinaryRead.get(1, TimeUnit.SECONDS);
        ReadDataBlock snapshotResult = snapshotRead.get(1, TimeUnit.SECONDS);
        assertEquals(1, ordinaryResult.getRecords().size());
        assertEquals(1, snapshotResult.getRecords().size());
        assertEquals(10L, ordinaryResult.getRecords().get(0).getBaseOffset());
        assertEquals(20, ordinaryResult.getRecords().get(0).getCount());
        assertEquals(ordinaryResult.getRecords().get(0).getBaseOffset(),
            snapshotResult.getRecords().get(0).getBaseOffset());
        assertEquals(ordinaryResult.getRecords().get(0).getCount(), snapshotResult.getRecords().get(0).getCount());
    }

    private static StreamMetadataManager newManager(MetadataImage image) {
        return newManager(image, mock(ObjectStorage.class));
    }

    private static StreamMetadataManager newManager(MetadataImage image, ObjectStorage storage) {
        if (storage.primary() == null) {
            when(storage.primary()).thenReturn(storage);
        }
        BrokerServer broker = mock(BrokerServer.class);
        KRaftMetadataCache metadataCache = mock(KRaftMetadataCache.class);
        MetadataLoader metadataLoader = mock(MetadataLoader.class);
        when(metadataCache.retainedImage()).thenReturn(image);
        when(broker.metadataCache()).thenReturn(metadataCache);
        when(broker.metadataLoader()).thenReturn(metadataLoader);
        when(metadataLoader.installPublishers(anyList())).thenReturn(CompletableFuture.completedFuture(null));
        ObjectReaderFactory objectReaderFactory = mock(ObjectReaderFactory.class);
        when(objectReaderFactory.getObjectStorage()).thenReturn(storage);
        return new StreamMetadataManager(broker, TARGET_NODE_ID, objectReaderFactory, mock(LocalStreamRangeIndexCache.class));
    }

    private static void replayStream(MetadataDelta delta, long startOffset, long logicalEndOffset) {
        delta.replay(new S3StreamRecord().setStreamId(STREAM_ID).setRangeIndex(1).setStartOffset(startOffset));
        delta.replay(new RangeRecord().setStreamId(STREAM_ID).setRangeIndex(0).setNodeId(SOURCE_NODE_ID)
            .setStartOffset(startOffset).setEndOffset(20L));
        delta.replay(new RangeRecord().setStreamId(STREAM_ID).setRangeIndex(1).setNodeId(TARGET_NODE_ID)
            .setStartOffset(20L).setEndOffset(logicalEndOffset));
    }

    private static void replayObject(MetadataDelta delta, long objectId, int nodeId, long startOffset,
        long endOffset) {
        delta.replay(new S3ObjectRecord().setObjectId(objectId)
            .setObjectState((byte) S3ObjectState.COMMITTED.ordinal()).setObjectSize(1L).setTimestamp(1L)
            .setAttributes(ObjectAttributes.DEFAULT.attributes()));
        S3StreamSetObject object = new S3StreamSetObject(objectId, nodeId,
            List.of(new StreamOffsetRange(STREAM_ID, startOffset, endOffset)), objectId);
        delta.replay((S3StreamSetObjectRecord) object.toRecord(AutoMQVersion.V0).message());
    }

    private static void replayArchive(MetadataDelta delta, long startOffset, long endOffset, long preparedEndOffset) {
        delta.replay(new S3StreamArchiveRecord().setStreamId(STREAM_ID)
            .setArchiveStartOffset(startOffset).setArchiveMetadataEndOffset(endOffset)
            .setArchiveEndOffset(endOffset).setArchivePreparedEndOffset(preparedEndOffset)
            .setArchiveSize(endOffset - startOffset).setArchiveCleanupEndOffset(startOffset)
            .setArchiveCleanupSize(0L));
    }

}
