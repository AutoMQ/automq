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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AssignedStreamIdRecord;
import org.apache.kafka.common.metadata.NodeWALMetadataRecord;
import org.apache.kafka.common.metadata.NodeWALUncommittedOffsetsRecord;
import org.apache.kafka.common.metadata.RangeRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamArchiveRecord;
import org.apache.kafka.common.metadata.RemoveS3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.common.metadata.S3StreamEndOffsetsRecord;
import org.apache.kafka.common.metadata.S3StreamRecord;
import org.apache.kafka.common.metadata.S3StreamObjectRecord;
import org.apache.kafka.common.metadata.S3StreamSetObjectRecord;
import org.apache.kafka.image.S3StreamsMetadataImage.RangeGetter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.stream.InRangeObjects;
import org.apache.kafka.metadata.stream.NodeWALUncommittedOffset;
import org.apache.kafka.metadata.stream.RangeMetadata;
import org.apache.kafka.metadata.stream.S3StreamEndOffsetsCodec;
import org.apache.kafka.metadata.stream.S3StreamArchiveMetadata;
import org.apache.kafka.metadata.stream.S3StreamObject;
import org.apache.kafka.metadata.stream.S3StreamSetObject;
import org.apache.kafka.metadata.stream.StreamEndOffset;
import org.apache.kafka.metadata.stream.StreamTags;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.timeline.TimelineHashMap;

import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.index.RangeIndex;
import com.automq.stream.s3.index.SparseRangeIndex;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.S3StreamConstant;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.utils.FutureUtil;
import com.google.common.collect.Range;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
@Tag("S3Unit")
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class S3StreamsMetadataImageTest {

    private static final int BROKER0 = 0;
    private static final int BROKER1 = 1;
    private static final long STREAM0 = 0;
    private static final long STREAM1 = 1;
    private static final long STREAM2 = 2;
    private static final long KB = 1024;

    private static final long MB = 1024 * KB;

    private static final long GB = 1024 * MB;
    private final RangeGetter defaultRangeGetter = new RangeGetter() {
        @Override
        public CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId) {
            return FutureUtil.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId) {
            return FutureUtil.failedFuture(new UnsupportedOperationException());
        }
    };

    static final S3StreamsMetadataImage IMAGE1;

    static final List<ApiMessageAndVersion> DELTA1_RECORDS;

    static final S3StreamsMetadataImage IMAGE2;

    // TODO: complete the test for StreamsMetadataImage

    static {
        IMAGE1 = S3StreamsMetadataImage.EMPTY;
        DELTA1_RECORDS = List.of();
        IMAGE2 = S3StreamsMetadataImage.EMPTY;
    }

    @AfterEach
    public void cleanup() {
        S3StreamSetObject.cleanCache();
    }

    @Test
    public void testAssignedChange() {
        S3StreamsMetadataImage image0 = S3StreamsMetadataImage.EMPTY;
        ApiMessageAndVersion record0 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
            .setAssignedStreamId(0), (short) 0);
        S3StreamsMetadataDelta delta0 = new S3StreamsMetadataDelta(image0);
        RecordTestUtils.replayAll(delta0, List.of(record0));
        S3StreamsMetadataImage image1 = new S3StreamsMetadataImage(0, RegistryRef.NOOP, new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0));
        assertEquals(image1, delta0.apply());
        testToImageAndBack(image1);

        ApiMessageAndVersion record1 = new ApiMessageAndVersion(new AssignedStreamIdRecord()
            .setAssignedStreamId(10), (short) 0);
        S3StreamsMetadataDelta delta1 = new S3StreamsMetadataDelta(image1);
        RecordTestUtils.replayAll(delta1, List.of(record1));
        S3StreamsMetadataImage image2 = new S3StreamsMetadataImage(10, RegistryRef.NOOP, new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0));
        assertEquals(image2, delta1.apply());
    }

    @Test
    public void testImage_compatible() {
        S3StreamsMetadataImage image = new S3StreamsMetadataImage(0, RegistryRef.NOOP, new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0));
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(image);
        delta.replay(new S3StreamRecord().setStreamId(233L));
        delta.replay((S3StreamSetObjectRecord) new S3StreamSetObject(0, 1, List.of(new StreamOffsetRange(233L, 100, 200L)), 0).toRecord(AutoMQVersion.V0).message());
        delta.replay(new S3StreamEndOffsetsRecord().setEndOffsets(S3StreamEndOffsetsCodec.encode(List.of(new StreamEndOffset(233L, 300L)))));

        image = delta.apply();
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);

        delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        image = delta.apply();
        delta = new S3StreamsMetadataDelta(image);
        delta.replay((S3StreamSetObjectRecord) new S3StreamSetObject(0, 1, List.of(new StreamOffsetRange(233L, 100, 200L)), 0).toRecord(AutoMQVersion.V0).message());
        image = delta.apply();

        Assertions.assertEquals(300L, image.streamEndOffsets().get(233L));
    }

    /**
     * Given stream and range records, verify image replay and snapshot preserve the logical end.
     */
    @Test
    public void testLogicalEndReplayAndSnapshot() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(new S3StreamRecord().setStreamId(STREAM0).setRangeIndex(0).setStartOffset(100L));
        delta.replay(new RangeRecord().setStreamId(STREAM0).setRangeIndex(0)
            .setStartOffset(100L).setEndOffset(150L).setNodeId(BROKER0));
        S3StreamsMetadataImage image = delta.apply();
        assertEquals(150L, image.streamEndOffsets().get(STREAM0));

        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        assertEquals(150L, delta.apply().streamEndOffsets().get(STREAM0));
    }

    /**
     * Given a live Stream without a materialized archive record, verify lookup returns the
     * Stream-start defaults while snapshots remain free of archive records.
     */
    @Test
    public void testArchiveDefaultStateIsNotMaterialized() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(100L));
        S3StreamsMetadataImage image = delta.apply();

        assertEquals(new S3StreamArchiveMetadata(STREAM0, 100L, 100L, 100L, 100L, 0L, 100L, 0L),
            image.getStreamArchiveMetadata(STREAM0));

        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        assertFalse(writer.records().stream().anyMatch(record ->
            record.message() instanceof S3StreamArchiveRecord
                || record.message() instanceof RemoveS3StreamArchiveRecord));
    }

    /**
     * Given successive complete archive records, verify replay replaces every field and a
     * snapshot round trip preserves the surviving state, including cleanup intent.
     */
    @Test
    public void testArchiveReplayReplacementAndSnapshot() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(archiveRecord(STREAM0, 10L, 20L, 30L, 40L, 500L, 25L, 100L));
        assertEquals(Set.of(STREAM0), delta.changedStreams());
        S3StreamsMetadataImage firstImage = delta.apply();
        assertFalse(firstImage.isEmpty());
        assertEquals(new S3StreamArchiveMetadata(STREAM0, 10L, 20L, 30L, 40L, 500L, 25L, 100L),
            firstImage.getStreamArchiveMetadata(STREAM0));

        delta = new S3StreamsMetadataDelta(firstImage);
        delta.replay(archiveRecord(STREAM0, 25L, 45L, 50L, 50L, 400L, 25L, 0L));
        S3StreamsMetadataImage replacedImage = delta.apply();
        assertEquals(new S3StreamArchiveMetadata(STREAM0, 25L, 45L, 50L, 50L, 400L, 25L, 0L),
            replacedImage.getStreamArchiveMetadata(STREAM0));
        assertEquals(500L, firstImage.getStreamArchiveMetadata(STREAM0).archiveSize());

        RecordListWriter writer = new RecordListWriter();
        replacedImage.write(writer, new ImageWriterOptions.Builder().build());
        List<S3StreamArchiveRecord> archiveRecords = writer.records().stream()
            .map(ApiMessageAndVersion::message)
            .filter(S3StreamArchiveRecord.class::isInstance)
            .map(S3StreamArchiveRecord.class::cast)
            .collect(Collectors.toList());
        assertEquals(1, archiveRecords.size());
        assertFalse(writer.records().stream().anyMatch(record ->
            record.message() instanceof RemoveS3StreamArchiveRecord));

        S3StreamsMetadataDelta snapshotDelta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(snapshotDelta, writer.records());
        assertEquals(replacedImage.getStreamArchiveMetadata(STREAM0),
            snapshotDelta.apply().getStreamArchiveMetadata(STREAM0));
    }

    /**
     * Given a removal record, verify replay deletes materialized archive state and snapshots
     * emit neither the removed full record nor a removal record.
     */
    @Test
    public void testArchiveRemoval() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(archiveRecord(STREAM0, 10L, 20L, 30L, 40L, 500L, 25L, 100L));
        S3StreamsMetadataImage image = delta.apply();

        delta = new S3StreamsMetadataDelta(image);
        delta.replay(new RemoveS3StreamArchiveRecord().setStreamId(STREAM0));
        assertEquals(Set.of(STREAM0), delta.changedStreams());
        image = delta.apply();
        assertNull(image.getStreamArchiveMetadata(STREAM0));
        assertTrue(image.isEmpty());

        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        assertFalse(writer.records().stream().anyMatch(record ->
            record.message() instanceof S3StreamArchiveRecord
                || record.message() instanceof RemoveS3StreamArchiveRecord));
    }

    /**
     * Given an older image with Archive state, verify completing a snapshot removes state that
     * is absent from the snapshot's surviving full records.
     */
    @Test
    public void testArchiveSnapshotCompletionRemovesAbsentState() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(archiveRecord(STREAM0, 10L, 20L, 30L, 40L, 500L, 25L, 100L));
        S3StreamsMetadataImage image = delta.apply();

        delta = new S3StreamsMetadataDelta(image);
        delta.finishSnapshot();
        assertNull(delta.apply().getStreamArchiveMetadata(STREAM0));
    }

    /**
     * Given a prepared Archive boundary after owner recovery, verify the Image enumerates its ordered source range.
     */
    @Test
    public void testPreparedArchiveRangeCanBeRecoveredWithoutObjectIdState() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(new S3StreamRecord().setStreamId(STREAM0).setStartOffset(0L));
        delta.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(10L)
            .setStartOffset(0L).setEndOffset(50L));
        delta.replay(new S3StreamObjectRecord().setStreamId(STREAM0).setObjectId(11L)
            .setStartOffset(50L).setEndOffset(100L));
        delta.replay(archiveRecord(STREAM0, 0L, 0L, 0L, 100L, 0L, 0L, 0L));

        S3StreamsMetadataImage recoveredImage = delta.apply();
        S3StreamArchiveMetadata archive = recoveredImage.getStreamArchiveMetadata(STREAM0);
        assertEquals(List.of(10L, 11L), recoveredImage.getStreamObjects(STREAM0,
                archive.archiveEndOffset(), archive.archivePreparedEndOffset(), 100).stream()
            .map(S3StreamObject::objectId)
            .collect(Collectors.toList()));
    }

    private static S3StreamArchiveRecord archiveRecord(
        long streamId,
        long archiveStartOffset,
        long archiveMetadataEndOffset,
        long archiveEndOffset,
        long archivePreparedEndOffset,
        long archiveSize,
        long archiveCleanupEndOffset,
        long archiveCleanupSize
    ) {
        return new S3StreamArchiveRecord()
            .setStreamId(streamId)
            .setArchiveStartOffset(archiveStartOffset)
            .setArchiveMetadataEndOffset(archiveMetadataEndOffset)
            .setArchiveEndOffset(archiveEndOffset)
            .setArchivePreparedEndOffset(archivePreparedEndOffset)
            .setArchiveSize(archiveSize)
            .setArchiveCleanupEndOffset(archiveCleanupEndOffset)
            .setArchiveCleanupSize(archiveCleanupSize);
    }

    /**
     * Given node WAL responsibility deltas, verify replay upserts and tombstones entries.
     */
    @Test
    public void testNodeWALUncommittedOffsetReplay() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(new NodeWALMetadataRecord().setNodeId(BROKER0));
        delta.replay(uncommittedRecord(BROKER0, STREAM0, 10L, 20L));
        S3StreamsMetadataImage image = delta.apply();
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 10L, 20L),
            image.getNodeWALUncommittedOffset(BROKER0, STREAM0));

        S3StreamsMetadataImage previousImage = image;
        delta = new S3StreamsMetadataDelta(image);
        delta.replay(uncommittedRecord(BROKER0, STREAM0, 15L, 20L));
        image = delta.apply();
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 15L, 20L),
            image.getNodeWALUncommittedOffset(BROKER0, STREAM0));
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 10L, 20L),
            previousImage.getNodeWALUncommittedOffset(BROKER0, STREAM0));

        delta = new S3StreamsMetadataDelta(image);
        delta.replay(uncommittedRecord(BROKER0, STREAM0, 20L, 20L));
        assertTrue(delta.apply().nodeWALUncommittedOffsets(BROKER0).isEmpty());
    }

    /**
     * Given raw responsibility entries, verify snapshot preserves them and splits records at
     * 10,000 entries.
     */
    @Test
    public void testNodeWALUncommittedOffsetSnapshotRoundTripAndChunking() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(new NodeWALMetadataRecord().setNodeId(BROKER0));
        List<NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset> entries = new ArrayList<>();
        for (long streamId = 0; streamId < 10_002; streamId++) {
            long endOffset = streamId == STREAM1 ? 50L : 200L;
            entries.add(new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(streamId).setStartOffset(0L).setEndOffset(endOffset));
        }
        entries.add(new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
            .setStreamId(20_000L).setStartOffset(0L).setEndOffset(200L));
        delta.replay(new NodeWALUncommittedOffsetsRecord().setNodeId(BROKER0).setEntries(entries));
        S3StreamsMetadataImage image = delta.apply();

        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        List<NodeWALUncommittedOffsetsRecord> records = writer.records().stream()
            .map(ApiMessageAndVersion::message)
            .filter(NodeWALUncommittedOffsetsRecord.class::isInstance)
            .map(NodeWALUncommittedOffsetsRecord.class::cast)
            .collect(Collectors.toList());
        assertEquals(List.of(10_000, 3), records.stream().map(record -> record.entries().size())
            .collect(Collectors.toList()));
        NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset raw = records.stream()
            .flatMap(record -> record.entries().stream())
            .filter(entry -> entry.streamId() == STREAM0)
            .findFirst().orElseThrow();
        assertEquals(0L, raw.startOffset());
        assertTrue(records.stream().flatMap(record -> record.entries().stream())
            .anyMatch(entry -> entry.streamId() == STREAM1));
        assertTrue(records.stream().flatMap(record -> record.entries().stream())
            .anyMatch(entry -> entry.streamId() == 20_000L));

        S3StreamsMetadataDelta snapshotDelta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(snapshotDelta, writer.records());
        Map<Long, NodeWALUncommittedOffset> snapshotOffsets = snapshotDelta.apply()
            .nodeWALUncommittedOffsets(BROKER0);
        assertEquals(10_003, snapshotOffsets.size());
        assertEquals(new NodeWALUncommittedOffset(STREAM0, 0L, 200L), snapshotOffsets.get(STREAM0));
        assertEquals(new NodeWALUncommittedOffset(STREAM1, 0L, 50L), snapshotOffsets.get(STREAM1));
    }

    /**
     * Given an older image with multiple entries, verify completing a snapshot removes entries
     * absent from all of its chunks while retaining replayed entries.
     */
    @Test
    public void testNodeWALUncommittedOffsetSnapshotCompletionRemovesAbsentEntries() {
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        delta.replay(new NodeWALMetadataRecord().setNodeId(BROKER0));
        delta.replay(new NodeWALUncommittedOffsetsRecord().setNodeId(BROKER0).setEntries(List.of(
            new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(STREAM0).setStartOffset(0L).setEndOffset(10L),
            new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(STREAM1).setStartOffset(0L).setEndOffset(10L))));
        S3StreamsMetadataImage image = delta.apply();

        delta = new S3StreamsMetadataDelta(image);
        delta.replay(new NodeWALMetadataRecord().setNodeId(BROKER0));
        delta.replay(uncommittedRecord(BROKER0, STREAM1, 5L, 10L));
        delta.finishSnapshot();
        Map<Long, NodeWALUncommittedOffset> offsets = delta.apply()
            .nodeWALUncommittedOffsets(BROKER0);
        assertEquals(Map.of(STREAM1, new NodeWALUncommittedOffset(STREAM1, 5L, 10L)), offsets);
    }

    private static NodeWALUncommittedOffsetsRecord uncommittedRecord(
        int nodeId, long streamId, long startOffset, long endOffset
    ) {
        return new NodeWALUncommittedOffsetsRecord().setNodeId(nodeId).setEntries(List.of(
            new NodeWALUncommittedOffsetsRecord.NodeWALUncommittedOffset()
                .setStreamId(streamId).setStartOffset(startOffset).setEndOffset(endOffset)));
    }

    private void testToImageAndBack(S3StreamsMetadataImage image) {
        RecordListWriter writer = new RecordListWriter();
        ImageWriterOptions options = new ImageWriterOptions.Builder().build();
        image.write(writer, options);
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(S3StreamsMetadataImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        S3StreamsMetadataImage newImage = delta.apply();
        assertEquals(image, newImage);
    }

    private RangeGetter buildMemoryRangeGetter() {
        return new RangeGetter() {
            @Override
            public CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId) {
                if (objectId == 0) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 100L, 120L)));
                } else if (objectId == 1) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 120L, 140L)));
                } else if (objectId == 2) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 180L, 200L)));
                } else if (objectId == 3) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 400L, 420L)));
                } else if (objectId == 4) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 520L, 600L)));
                } else if (objectId == 5) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 140L, 160L)));
                } else if (objectId == 6) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 160L, 180L)));
                } else if (objectId == 7) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 420L, 520L)));
                } else if (objectId == 9) {
                    return CompletableFuture.completedFuture(Optional.of(new StreamOffsetRange(STREAM0, 600L, 888L)));
                } else {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
            }

            @Override
            public CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId) {
                Map<Long, SparseRangeIndex> streamRangeIndexMap;
                if (nodeId == BROKER0) {
                    streamRangeIndexMap = Map.of(
                        STREAM0, new SparseRangeIndex(2, List.of(
                            new RangeIndex(100, 120, 0),
                            new RangeIndex(180, 200, 2),
                            new RangeIndex(520, 600, 4))));
                } else {
                    streamRangeIndexMap = Map.of(
                        STREAM0, new SparseRangeIndex(2, List.of(
                            new RangeIndex(140, 160, 5),
                            new RangeIndex(420, 520, 7),
                            // objectId 8 is not exist (compacted)
                            new RangeIndex(600, 700, 8))));
                }
                return CompletableFuture.completedFuture(LocalStreamRangeIndexCache.toBuffer(streamRangeIndexMap));
            }
        };
    }

    @SuppressWarnings("NPathComplexity")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetObjects(boolean isHugeCluster) throws ExecutionException, InterruptedException {
        DeltaList<S3StreamSetObject> broker0Objects = DeltaList.of(
            new S3StreamSetObject(0, BROKER0, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 100L, 120L)), 0L),
            new S3StreamSetObject(1, BROKER0, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 120L, 140L)), 1L),
            new S3StreamSetObject(2, BROKER0, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 180L, 200L)), 2L),
            new S3StreamSetObject(3, BROKER0, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 400L, 420L)), 3L),
            new S3StreamSetObject(4, BROKER0, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 520L, 600L)), 4L));
        DeltaList<S3StreamSetObject> broker1Objects = DeltaList.of(
            new S3StreamSetObject(5, BROKER1, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 140L, 160L)), 0L),
            new S3StreamSetObject(6, BROKER1, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 160L, 180L)), 1L),
            new S3StreamSetObject(7, BROKER1, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 420L, 520L)), 2L),
            new S3StreamSetObject(9, BROKER1, isHugeCluster ? null : List.of(new StreamOffsetRange(STREAM0, 600L, 888L)), 3L));
        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
            broker0Objects);
        NodeS3StreamSetObjectMetadataImage broker1WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER1, S3StreamConstant.INVALID_BROKER_EPOCH,
            broker1Objects);
        List<RangeMetadata> ranges = List.of(
            new RangeMetadata(STREAM0, 0L, 0, 10L, 140L, BROKER0),
            new RangeMetadata(STREAM0, 1L, 1, 140L, 180L, BROKER1),
            new RangeMetadata(STREAM0, 2L, 2, 180L, 420L, BROKER0),
            new RangeMetadata(STREAM0, 3L, 3, 420L, 520L, BROKER1),
            new RangeMetadata(STREAM0, 4L, 4, 520L, 600L, BROKER0),
            new RangeMetadata(STREAM0, 5L, 5, 600L, 888L, BROKER1));
        DeltaList<S3StreamObject> streamObjects = DeltaList.of(
            new S3StreamObject(8, STREAM0, 10L, 100L),
            new S3StreamObject(9, STREAM0, 200L, 300L),
            new S3StreamObject(10, STREAM0, 300L, 400L));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 10, ranges, streamObjects);
        RegistryRef ref = new RegistryRef();
        TimelineHashMap<Long, S3StreamMetadataImage> streamMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        streamMetadataMap.put(STREAM0, streamImage);
        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        nodeMetadataMap.put(BROKER0, broker0WALMetadataImage);
        nodeMetadataMap.put(BROKER1, broker1WALMetadataImage);
        ref = ref.next();
        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(
            STREAM0,
            ref,
            streamMetadataMap,
            nodeMetadataMap, new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(ref.registry(), 0));

        RangeGetter rangeGetter = isHugeCluster ? buildMemoryRangeGetter() : defaultRangeGetter;
        // 1. search stream_1
        InRangeObjects objects = streamsImage.getObjects(STREAM1, 10, 100, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(InRangeObjects.INVALID, objects);

        // 2. search stream_0 in [0, 600)
        // failed for trimmed startOffset
        objects = streamsImage.getObjects(STREAM0, 0, 600, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(InRangeObjects.INVALID, objects);

        // 3. search stream_0 for full range [10, 600)
        objects = streamsImage.getObjects(STREAM0, 10, 600, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(10, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(11, objects.objects().size());
        List<Long> expectedObjectIds = List.of(
            8L, 0L, 1L, 5L, 6L, 2L, 9L, 10L, 3L, 7L, 4L);
        assertEquals(expectedObjectIds, objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 4. search stream_0 in [20, 550)
        objects = streamsImage.getObjects(STREAM0, 20, 550, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(10, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(11, objects.objects().size());
        assertEquals(expectedObjectIds, objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 5. search stream_0 in [20, 550) with limit 5
        objects = streamsImage.getObjects(STREAM0, 20, 550, 5, rangeGetter).get();
        assertEquals(10, objects.startOffset());
        assertEquals(180, objects.endOffset());
        assertEquals(5, objects.objects().size());
        assertEquals(expectedObjectIds.subList(0, 5), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 6. search stream_0 in [400, 520)
        objects = streamsImage.getObjects(STREAM0, 400, 520, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(400, objects.startOffset());
        assertEquals(520, objects.endOffset());
        assertEquals(2, objects.objects().size());
        assertEquals(expectedObjectIds.subList(8, 10), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 7. search stream_0 in [401, 519)
        objects = streamsImage.getObjects(STREAM0, 401, 519, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(400, objects.startOffset());
        assertEquals(520, objects.endOffset());
        assertEquals(2, objects.objects().size());
        assertEquals(expectedObjectIds.subList(8, 10), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 8. search stream_0 in [399, 521)
        objects = streamsImage.getObjects(STREAM0, 399, 521, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(300, objects.startOffset());
        assertEquals(600, objects.endOffset());
        assertEquals(4, objects.objects().size());
        assertEquals(expectedObjectIds.subList(7, 11), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // 9. search stream0 in [399, 1000)
        objects = streamsImage.getObjects(STREAM0, 399, 1000, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(300, objects.startOffset());
        assertEquals(888, objects.endOffset());
        assertEquals(5, objects.objects().size());
        assertEquals(List.of(10L, 3L, 7L, 4L, 9L), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        objects = streamsImage.getObjects(STREAM0, 101, 400L, Integer.MAX_VALUE, rangeGetter).get();
        assertEquals(100L, objects.startOffset());
        assertEquals(400L, objects.endOffset());
        assertEquals(7, objects.objects().size());
        assertEquals(expectedObjectIds.subList(1, 8), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        objects = streamsImage.getObjects(STREAM0, 10, ObjectUtils.NOOP_OFFSET, 9, rangeGetter).get();
        assertEquals(10, objects.startOffset());
        assertEquals(420, objects.endOffset());
        assertEquals(9, objects.objects().size());
        assertEquals(List.of(8L, 0L, 1L, 5L, 6L, 2L, 9L, 10L, 3L), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        objects = streamsImage.getObjects(STREAM0, 550, ObjectUtils.NOOP_OFFSET, 9, rangeGetter).get();
        assertEquals(520, objects.startOffset());
        assertEquals(888, objects.endOffset());
        assertEquals(2, objects.objects().size());
        assertEquals(List.of(4L, 9L), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));

        // test get from local cache
        broker0WALMetadataImage.clearOffsetIndexMap();
        LocalStreamRangeIndexCache cache = Mockito.mock(LocalStreamRangeIndexCache.class);
        Mockito.doAnswer(invocation -> CompletableFuture.completedFuture(4L)).when(cache).searchObjectId(STREAM0, 550);
        objects = streamsImage.getObjects(STREAM0, 550, ObjectUtils.NOOP_OFFSET, 9, rangeGetter, cache).get();
        assertEquals(520, objects.startOffset());
        assertEquals(888, objects.endOffset());
        assertEquals(2, objects.objects().size());
        assertEquals(List.of(4L, 9L), objects.objects().stream().map(S3ObjectMetadata::objectId).collect(Collectors.toList()));
    }

    @Test
    public void testGetObjectsSanityCheck() {
        S3StreamsMetadataImage streamsImage = createStreamImage();
        S3StreamsMetadataImage.GetObjectsContext ctx = new S3StreamsMetadataImage.GetObjectsContext(STREAM0,
            22L, 100L, 2, defaultRangeGetter, null);

        // test empty result
        Assertions.assertDoesNotThrow(() -> streamsImage.sanityCheck(ctx, Collections.emptyList()));

        // test missing range
        Assertions.assertThrows(IllegalArgumentException.class, () -> streamsImage.sanityCheck(ctx, List.of(
            new S3ObjectMetadata(0, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM1, 22L, 100L)), System.currentTimeMillis())
        )));

        // test mismatched first range
        Assertions.assertThrows(IllegalArgumentException.class, () -> streamsImage.sanityCheck(ctx, List.of(
            new S3ObjectMetadata(0, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM0, 40L, 50L)), System.currentTimeMillis())
        )));

        // test not continuous range
        Assertions.assertThrows(IllegalArgumentException.class, () -> streamsImage.sanityCheck(ctx, List.of(
            new S3ObjectMetadata(0, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM0, 10L, 50L)), System.currentTimeMillis()),
            new S3ObjectMetadata(1, S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(STREAM0, 600L, 100L)), System.currentTimeMillis())
        )));

        // test over-sized objects
        Assertions.assertThrows(IllegalArgumentException.class, () -> streamsImage.sanityCheck(ctx, List.of(
            new S3ObjectMetadata(0, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM0, 10L, 50L)), System.currentTimeMillis()),
            new S3ObjectMetadata(1, S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(STREAM0, 50L, 80L)), System.currentTimeMillis()),
            new S3ObjectMetadata(2, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM0, 80L, 100L)), System.currentTimeMillis())
        )));

        // test normal case
        Assertions.assertDoesNotThrow(() -> streamsImage.sanityCheck(ctx, List.of(
            new S3ObjectMetadata(0, S3ObjectType.STREAM_SET,
                List.of(new StreamOffsetRange(STREAM0, 10L, 50L)), System.currentTimeMillis()),
            new S3ObjectMetadata(1, S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(STREAM0, 50L, 120L)), System.currentTimeMillis())
        )));
    }

    /**
     * Test get objects with the first hit object is a stream object.
     */
    @Test
    public void testGetObjectsWithFirstStreamObject() throws ExecutionException, InterruptedException {
        DeltaList<S3StreamSetObject> broker0Objects = DeltaList.of(
            new S3StreamSetObject(0, BROKER0, List.of(new StreamOffsetRange(STREAM0, 20L, 40L)), 0L));
        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
            broker0Objects);
        List<RangeMetadata> ranges = List.of(
            new RangeMetadata(STREAM0, 0L, 0, 10L, 40L, BROKER0),
            new RangeMetadata(STREAM0, 2L, 2, 40L, 60L, BROKER0));
        DeltaList<S3StreamObject> streamObjects = DeltaList.of(
            new S3StreamObject(8, STREAM0, 10L, 20L),
            new S3StreamObject(8, STREAM0, 40L, 60L));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 10, ranges, streamObjects);

        RegistryRef ref = new RegistryRef();
        TimelineHashMap<Long, S3StreamMetadataImage> streamMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        streamMetadataMap.put(STREAM0, streamImage);
        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        nodeMetadataMap.put(BROKER0, broker0WALMetadataImage);
        ref = ref.next();
        S3StreamsMetadataImage streamsImage = new S3StreamsMetadataImage(STREAM0, ref, streamMetadataMap,
            nodeMetadataMap, new TimelineHashMap<>(ref.registry(), 0), new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0));

        InRangeObjects objects = streamsImage.getObjects(STREAM0, 22L, 55, 4, defaultRangeGetter).get();
        assertEquals(2, objects.objects().size());
        assertEquals(20L, objects.startOffset());
        assertEquals(60L, objects.endOffset());

        objects = streamsImage.getObjects(STREAM0, 22L, 55, 1, defaultRangeGetter).get();
        assertEquals(1, objects.objects().size());
        assertEquals(20L, objects.startOffset());
        assertEquals(40L, objects.endOffset());
    }

    private S3StreamsMetadataImage createStreamImage() {
        DeltaList<S3StreamSetObject> broker0Objects = DeltaList.of(
            new S3StreamSetObject(0, BROKER0, List.of(new StreamOffsetRange(STREAM0, 10L, 20L)), 0L),
            new S3StreamSetObject(1, BROKER0, List.of(new StreamOffsetRange(STREAM0, 40L, 60L)), 1L));
        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage = new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
            broker0Objects);
        List<RangeMetadata> ranges = List.of(
            new RangeMetadata(STREAM0, 0L, 0, 10L, 40L, BROKER0),
            new RangeMetadata(STREAM0, 2L, 2, 40L, 60L, BROKER0));
        DeltaList<S3StreamObject> streamObjects = DeltaList.of(
            new S3StreamObject(8, STREAM0, 20L, 40L));
        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(STREAM0, 4L, StreamState.OPENED, new S3StreamRecord.TagCollection(), 10, ranges, streamObjects);
        RegistryRef ref = new RegistryRef();
        TimelineHashMap<Long, S3StreamMetadataImage> streamMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        streamMetadataMap.put(STREAM0, streamImage);
        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        nodeMetadataMap.put(BROKER0, broker0WALMetadataImage);
        ref = ref.next();
        return new S3StreamsMetadataImage(STREAM0, ref, streamMetadataMap,
            nodeMetadataMap, new TimelineHashMap<>(ref.registry(), 0), new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(ref.registry(), 0), new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0));
    }

    private S3StreamsMetadataImage generateStreamImage(long streamId, Range<Long> streamObjectRange,
        Range<Long> streamSetObjectRange, int step) {
        long objectId = 0;

        List<RangeMetadata> ranges = new ArrayList<>();
        List<S3StreamObject> streamObjects = new ArrayList<>();

        // streamObject first
        int rangeIndex = 0;
        for (long i = streamObjectRange.lowerEndpoint(); i < streamObjectRange.upperEndpoint(); i += step) {
            ranges.add(new RangeMetadata(streamId, 0L, rangeIndex,
                i, i + step, BROKER0));

            streamObjects.add(new S3StreamObject(objectId, streamId, i, i + step));
            rangeIndex++;
            objectId++;
        }

        // streamSetObject second
        DeltaList<S3StreamSetObject> broker0Objects = new DeltaList<>();
        for (long i = streamSetObjectRange.lowerEndpoint(); i < streamSetObjectRange.upperEndpoint(); i += step) {
            ranges.add(new RangeMetadata(streamId, 0L, rangeIndex,
                i, i + step, BROKER0));
            rangeIndex++;
            broker0Objects.add(new S3StreamSetObject(objectId, BROKER0,
                List.of(new StreamOffsetRange(streamId, i, i + step)), i));
            objectId++;
        }

        NodeS3StreamSetObjectMetadataImage broker0WALMetadataImage =
            new NodeS3StreamSetObjectMetadataImage(BROKER0, S3StreamConstant.INVALID_BROKER_EPOCH,
                broker0Objects);

        S3StreamMetadataImage streamImage = new S3StreamMetadataImage(
            streamId, 4L, StreamState.OPENED, new S3StreamRecord.TagCollection(),
            streamObjectRange.lowerEndpoint(),
            ranges, new DeltaList<>(streamObjects));

        RegistryRef ref = new RegistryRef();
        TimelineHashMap<Long, S3StreamMetadataImage> streamMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        streamMetadataMap.put(streamId, streamImage);
        TimelineHashMap<Integer, NodeS3StreamSetObjectMetadataImage> nodeMetadataMap = new TimelineHashMap<>(ref.registry(), 0);
        nodeMetadataMap.put(BROKER0, broker0WALMetadataImage);
        ref = ref.next();
        return new S3StreamsMetadataImage(streamId, ref, streamMetadataMap,
            nodeMetadataMap, new TimelineHashMap<>(ref.registry(), 0), new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(ref.registry(), 0),
            new TimelineHashMap<>(RegistryRef.NOOP.registry(), 0));
    }

    /**
     * Test get objects with the first hit object is a stream set object.
     */
    @Test
    public void testGetObjectsWithFirstStreamSetObject() throws ExecutionException, InterruptedException {
        S3StreamsMetadataImage streamsImage = createStreamImage();

        InRangeObjects objects = streamsImage.getObjects(STREAM0, 12L, 30, 4, defaultRangeGetter).get();
        assertEquals(2, objects.objects().size());
        assertEquals(10L, objects.startOffset());
        assertEquals(40L, objects.endOffset());

        objects = streamsImage.getObjects(STREAM0, 12L, 30, 1, defaultRangeGetter).get();
        assertEquals(1, objects.objects().size());
        assertEquals(10L, objects.startOffset());
        assertEquals(20L, objects.endOffset());
    }

    @Test
    public void testConcurrentFetchMetadataWithoutConcurrentModificationException() {
        S3StreamsMetadataImage streamsImage = generateStreamImage(STREAM0,
            Range.closedOpen(0L, 100000L),
            Range.closedOpen(100000L, 110000L), 20);

        long startOffset = streamsImage.getStreamMetadata(STREAM0).getStartOffset();
        long endOffset = streamsImage.getStreamMetadata(STREAM0).lastRange().endOffset();

        AtomicBoolean hasException = new AtomicBoolean(false);

        CountDownLatch doneLatch = new CountDownLatch(4);
        CountDownLatch startLatch = new CountDownLatch(1);

        class Item {
            long start;
            long end;
            int limit;

            public Item(long start, long end, int limit) {
                this.start = start;
                this.end = end;
                this.limit = limit;
            }
        }

        ConcurrentHashMap<Item, InRangeObjects> result = new ConcurrentHashMap<>();
        ExecutorService es = Executors.newFixedThreadPool(4);
        for (int j = 0; j < 4; j++) {
            es.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    // ignore
                    return;
                }
                ThreadLocalRandom r = ThreadLocalRandom.current();
                for (int i = 0; i < 10000 && !hasException.get(); i++) {
                    try {
                        long start = r.nextLong(startOffset, endOffset);
                        long end = r.nextLong(start, endOffset);
                        int limit = r.nextInt(3, 17);
                        Item item = new Item(start, end, limit);
                        result.put(item, streamsImage.getObjects(STREAM0, start, end, limit, defaultRangeGetter).get());
                    } catch (Exception e) {
                        hasException.set(true);
                    }
                }

                doneLatch.countDown();
            });
        }

        startLatch.countDown();
        try {
            doneLatch.await();
        } catch (InterruptedException e) {
            //
        } finally {
            es.shutdown();
        }

        assertFalse(hasException.get());

        result.entrySet().forEach(entry -> {
            Item item = entry.getKey();
            InRangeObjects objects = entry.getValue();
            try {
                assertEquals(streamsImage.getObjects(STREAM0, item.start, item.end, item.limit, defaultRangeGetter).get(), objects);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testGetTopicPartitionStreamRelation() {
        Uuid topicId = Uuid.randomUuid();
        S3StreamsMetadataImage image = S3StreamsMetadataImage.EMPTY;
        S3StreamsMetadataDelta delta = new S3StreamsMetadataDelta(image);

        S3StreamRecord.TagCollection tags = new S3StreamRecord.TagCollection();
        tags.add(new S3StreamRecord.Tag().setKey(StreamTags.Topic.KEY).setValue(StreamTags.Topic.encode(topicId)));
        tags.add(new S3StreamRecord.Tag().setKey(StreamTags.Partition.KEY).setValue(StreamTags.Partition.encode(10)));
        delta.replay(new S3StreamRecord().setStreamId(STREAM0).setTags(tags));

        tags = new S3StreamRecord.TagCollection();
        tags.add(new S3StreamRecord.Tag().setKey(StreamTags.Topic.KEY).setValue(StreamTags.Topic.encode(topicId)));
        tags.add(new S3StreamRecord.Tag().setKey(StreamTags.Partition.KEY).setValue(StreamTags.Partition.encode(11)));
        delta.replay(new S3StreamRecord().setStreamId(STREAM1).setTags(tags));
        delta.replay(new RemoveS3StreamRecord().setStreamId(STREAM1));

        image = delta.apply();

        {
            Set<Long> streams = image.getTopicPartitionStreams(topicId, 10);
            assertEquals(Set.of(STREAM0), streams);

            streams = image.getTopicPartitionStreams(topicId, 11);
            assertTrue(streams.isEmpty());
        }

        {
            TopicIdPartition tp = image.getStreamTopicPartition(STREAM0);
            assertEquals(topicId, tp.topicId());
            assertEquals(10, tp.partition());

            tp = image.getStreamTopicPartition(STREAM1);
            assertNull(tp);
        }

        delta = new S3StreamsMetadataDelta(image);

        tags = new S3StreamRecord.TagCollection();
        tags.add(new S3StreamRecord.Tag().setKey(StreamTags.Topic.KEY).setValue(StreamTags.Topic.encode(topicId)));
        tags.add(new S3StreamRecord.Tag().setKey(StreamTags.Partition.KEY).setValue(StreamTags.Partition.encode(10)));
        delta.replay(new S3StreamRecord().setStreamId(STREAM2).setTags(tags));

        image = delta.apply();
        {
            Set<Long> streams = image.getTopicPartitionStreams(topicId, 10);
            assertEquals(Set.of(STREAM0, STREAM2), streams);
            TopicIdPartition tp = image.getStreamTopicPartition(STREAM2);
            assertEquals(topicId, tp.topicId());
            assertEquals(10, tp.partition());
        }

        delta = new S3StreamsMetadataDelta(image);
        delta.replay(new RemoveS3StreamRecord().setStreamId(STREAM0));

        image = delta.apply();
        {
            Set<Long> streams = image.getTopicPartitionStreams(topicId, 10);
            assertEquals(Set.of(STREAM2), streams);
            TopicIdPartition tp = image.getStreamTopicPartition(STREAM0);
            assertNull(tp);
        }
    }

}
