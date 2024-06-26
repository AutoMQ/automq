/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.compact;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.compact.objects.CompactedObject;
import com.automq.stream.s3.compact.objects.CompactedObjectBuilder;
import com.automq.stream.s3.memory.MemoryMetadataManager;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;

import static com.automq.stream.s3.ByteBufAllocPolicy.POOLED_DIRECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class CompactionTestBase {
    protected static final int BROKER_0 = 0;
    protected static final long STREAM_0 = 0;
    protected static final long STREAM_1 = 1;
    protected static final long STREAM_2 = 2;
    protected static final long STREAM_3 = 3;
    protected static final long OBJECT_0 = 0;
    protected static final long OBJECT_1 = 1;
    protected static final long OBJECT_2 = 2;
    protected static final long OBJECT_3 = 3;
    protected static final long CACHE_SIZE = 1024;
    protected static final double EXECUTION_SCORE_THRESHOLD = 0.5;
    protected static final long STREAM_SPLIT_SIZE = 30;
    protected static final int MAX_STREAM_NUM_IN_WAL = 100;
    protected static final int MAX_STREAM_OBJECT_NUM = 100;
    protected static final List<S3ObjectMetadata> S3_WAL_OBJECT_METADATA_LIST = new ArrayList<>();
    protected MemoryMetadataManager streamManager;
    protected MemoryMetadataManager objectManager;
    protected ObjectStorage objectStorage;

    public void setUp() throws Exception {
        ByteBufAlloc.setPolicy(POOLED_DIRECT);
        streamManager = Mockito.mock(MemoryMetadataManager.class);
        when(streamManager.getStreams(Mockito.anyList())).thenReturn(CompletableFuture.completedFuture(
            List.of(new StreamMetadata(STREAM_0, 0, 0, 20, StreamState.OPENED),
                new StreamMetadata(STREAM_1, 0, 25, 500, StreamState.OPENED),
                new StreamMetadata(STREAM_2, 0, 30, 270, StreamState.OPENED))));

        objectManager = Mockito.spy(MemoryMetadataManager.class);
        objectStorage = new MemoryObjectStorage();
        // stream data for object 0
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_0, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(objectId, objectStorage, 1024, 1024);
            StreamRecordBatch r1 = new StreamRecordBatch(STREAM_0, 0, 0, 15, TestUtils.random(2));
            StreamRecordBatch r2 = new StreamRecordBatch(STREAM_1, 0, 25, 5, TestUtils.random(2));
            StreamRecordBatch r3 = new StreamRecordBatch(STREAM_1, 0, 30, 30, TestUtils.random(22));
            StreamRecordBatch r4 = new StreamRecordBatch(STREAM_2, 0, 30, 30, TestUtils.random(22));
            objectWriter.write(STREAM_0, List.of(r1));
            objectWriter.write(STREAM_1, List.of(r2));
            objectWriter.write(STREAM_1, List.of(r3));
            objectWriter.write(STREAM_2, List.of(r4));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_0, 0, 15),
                new StreamOffsetRange(STREAM_1, 25, 30),
                new StreamOffsetRange(STREAM_1, 30, 60),
                new StreamOffsetRange(STREAM_2, 30, 60)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_0, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_0);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
            List.of(r1, r2, r3, r4).forEach(StreamRecordBatch::release);
        }).join();

        // stream data for object 1
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_1, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_1, objectStorage, 1024, 1024);
            StreamRecordBatch r5 = new StreamRecordBatch(STREAM_0, 0, 15, 5, TestUtils.random(1));
            StreamRecordBatch r6 = new StreamRecordBatch(STREAM_1, 0, 60, 60, TestUtils.random(52));
            objectWriter.write(STREAM_0, List.of(r5));
            objectWriter.write(STREAM_1, List.of(r6));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_0, 15, 20),
                new StreamOffsetRange(STREAM_1, 60, 120)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_1, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_1);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
            List.of(r5, r6).forEach(StreamRecordBatch::release);
        }).join();

        // stream data for object 2
        objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(30)).thenAccept(objectId -> {
            assertEquals(OBJECT_2, objectId);
            ObjectWriter objectWriter = ObjectWriter.writer(OBJECT_2, objectStorage, 1024, 1024);
            StreamRecordBatch r8 = new StreamRecordBatch(STREAM_1, 0, 400, 100, TestUtils.random(92));
            StreamRecordBatch r9 = new StreamRecordBatch(STREAM_2, 0, 230, 40, TestUtils.random(32));
            objectWriter.write(STREAM_1, List.of(r8));
            objectWriter.write(STREAM_2, List.of(r9));
            objectWriter.close().join();
            List<StreamOffsetRange> streamsIndices = List.of(
                new StreamOffsetRange(STREAM_1, 400, 500),
                new StreamOffsetRange(STREAM_2, 230, 270)
            );
            S3ObjectMetadata objectMetadata = new S3ObjectMetadata(OBJECT_2, S3ObjectType.STREAM_SET, streamsIndices, System.currentTimeMillis(),
                System.currentTimeMillis(), objectWriter.size(), OBJECT_2);
            S3_WAL_OBJECT_METADATA_LIST.add(objectMetadata);
            List.of(r8, r9).forEach(StreamRecordBatch::release);
        }).join();
        doReturn(CompletableFuture.completedFuture(S3_WAL_OBJECT_METADATA_LIST)).when(objectManager).getServerObjects();
    }

    public void tearDown() {
        S3_WAL_OBJECT_METADATA_LIST.clear();
    }

    protected boolean compare(StreamDataBlock block1, StreamDataBlock block2) {
        boolean attr = block1.getStreamId() == block2.getStreamId() &&
            block1.getStartOffset() == block2.getStartOffset() &&
            block1.getEndOffset() == block2.getEndOffset() &&
            block1.dataBlockIndex().recordCount() == block2.dataBlockIndex().recordCount();
        if (!attr) {
            return false;
        }
        try {
            block1.getDataCf().get(100, TimeUnit.MILLISECONDS);
            block2.getDataCf().get(100, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            // ignore
        }
        if (!block1.getDataCf().isDone()) {
            return !block2.getDataCf().isDone();
        } else {
            if (!block2.getDataCf().isDone()) {
                return false;
            } else {
                return block1.getDataCf().join().compareTo(block2.getDataCf().join()) == 0;
            }
        }
    }

    protected boolean compare(List<StreamDataBlock> streamDataBlocks1, List<StreamDataBlock> streamDataBlocks2) {
        if (streamDataBlocks1.size() != streamDataBlocks2.size()) {
            return false;
        }
        for (int i = 0; i < streamDataBlocks1.size(); i++) {
            if (!compare(streamDataBlocks1.get(i), streamDataBlocks2.get(i))) {
                return false;
            }
        }
        return true;
    }

    protected boolean compare(Map<Long, List<StreamDataBlock>> streamDataBlockMap1,
        Map<Long, List<StreamDataBlock>> streamDataBlockMap2) {
        if (streamDataBlockMap1.size() != streamDataBlockMap2.size()) {
            return false;
        }
        for (Map.Entry<Long, List<StreamDataBlock>> entry : streamDataBlockMap1.entrySet()) {
            long objectId = entry.getKey();
            List<StreamDataBlock> streamDataBlocks = entry.getValue();
            assertTrue(streamDataBlockMap2.containsKey(objectId));
            if (!compare(streamDataBlocks, streamDataBlockMap2.get(objectId))) {
                return false;
            }
        }
        return true;
    }

    protected boolean compare(CompactedObjectBuilder builder1, CompactedObjectBuilder builder2) {
        if (builder1.type() != builder2.type()) {
            return false;
        }
        return compare(builder1.streamDataBlocks(), builder2.streamDataBlocks());
    }

    protected boolean compare(CompactedObject compactedObject1, CompactedObject compactedObject2) {
        if (compactedObject1.type() != compactedObject2.type()) {
            return false;
        }
        return compare(compactedObject1.streamDataBlocks(), compactedObject2.streamDataBlocks());
    }

    protected long calculateObjectSize(List<StreamDataBlock> streamDataBlocksGroups) {
        long bodySize = streamDataBlocksGroups.stream().mapToLong(StreamDataBlock::getBlockSize).sum();
        int indexBlockSize = DataBlockIndex.BLOCK_INDEX_SIZE * streamDataBlocksGroups.size();
        long tailSize = ObjectWriter.Footer.FOOTER_SIZE;
        return bodySize + indexBlockSize + tailSize;
    }

    protected List<StreamDataBlock> mergeStreamDataBlocksForGroup(List<List<StreamDataBlock>> streamDataBlockGroups) {
        List<StreamDataBlock> mergedStreamDataBlocks = new ArrayList<>();
        for (List<StreamDataBlock> streamDataBlocks : streamDataBlockGroups) {
            StreamDataBlock mergedBlock = new StreamDataBlock(
                streamDataBlocks.get(0).getStreamId(),
                streamDataBlocks.get(0).getStartOffset(),
                streamDataBlocks.get(streamDataBlocks.size() - 1).getEndOffset(),
                streamDataBlocks.get(0).getObjectId(),
                streamDataBlocks.get(0).getBlockStartPosition(),
                streamDataBlocks.stream().mapToInt(StreamDataBlock::getBlockSize).sum(),
                streamDataBlocks.stream().map(StreamDataBlock::dataBlockIndex).mapToInt(DataBlockIndex::recordCount).sum());
            mergedBlock.getDataCf().complete(mergeStreamDataBlocksData(streamDataBlocks));
            mergedStreamDataBlocks.add(mergedBlock);
        }
        return mergedStreamDataBlocks;
    }

    private ByteBuf mergeStreamDataBlocksData(List<StreamDataBlock> streamDataBlocks) {
        CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
        for (StreamDataBlock block : streamDataBlocks) {
            buf.addComponent(true, block.getDataCf().join());
        }
        return buf;
    }
}
