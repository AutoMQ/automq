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
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
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
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.impl.MemoryWriteAheadLog;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class StreamMetadataManagerTest {
    private static final long STREAM_ID = 7L;
    private static final int SOURCE_NODE_ID = 1;
    private static final int TARGET_NODE_ID = 2;

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
        BrokerServer broker = mock(BrokerServer.class);
        KRaftMetadataCache metadataCache = mock(KRaftMetadataCache.class);
        MetadataLoader metadataLoader = mock(MetadataLoader.class);
        when(metadataCache.retainedImage()).thenReturn(image);
        when(broker.metadataCache()).thenReturn(metadataCache);
        when(broker.metadataLoader()).thenReturn(metadataLoader);
        when(metadataLoader.installPublishers(anyList())).thenReturn(CompletableFuture.completedFuture(null));
        return new StreamMetadataManager(broker, TARGET_NODE_ID, mock(ObjectReaderFactory.class),
            mock(LocalStreamRangeIndexCache.class));
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

}
