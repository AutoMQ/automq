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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import io.netty.channel.DefaultEventLoop;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class StreamReaderTest {
    private static final long STREAM_ID = 233;
    private static final int BLOCK_SIZE_THRESHOLD = 1024;
    private Map<Long, MockObject> objects;
    private DefaultEventLoop[] eventLoops;
    private ObjectManager objectManager;
    private Function<S3ObjectMetadata, ObjectReader> objectReaderFactory;
    private DataBlockCache dataBlockCache;

    private StreamReader streamReader;

    @BeforeEach
    void setup() {
        objects = new HashMap<>();
        objects.put(
            0L,
            MockObject
                .builder(0, BLOCK_SIZE_THRESHOLD)
                .write(STREAM_ID, List.of(
                    new StreamRecordBatch(STREAM_ID, 0, 0, 1, TestUtils.random(1))
                )).build()
        );
        objects.put(
            1L,
            MockObject
                .builder(1L, 1)
                .write(STREAM_ID, List.of(
                    new StreamRecordBatch(STREAM_ID, 0, 1, 1, TestUtils.random(19)),
                    new StreamRecordBatch(STREAM_ID, 0, 2, 1, TestUtils.random(10)),
                    new StreamRecordBatch(STREAM_ID, 0, 3, 1, TestUtils.random(10))
                )).build()
        );
        for (int i = 0; i < 6; i++) {
            long offset = 4 + i * 5;
            objects.put(
                i + 2L,
                MockObject
                    .builder(i + 2L, BLOCK_SIZE_THRESHOLD)
                    .write(STREAM_ID, List.of(
                        new StreamRecordBatch(STREAM_ID, 0, offset, 1, TestUtils.random(1024 * 1024 / 4)),
                        new StreamRecordBatch(STREAM_ID, 0, offset + 1, 1, TestUtils.random(1024 * 1024 / 4)),
                        new StreamRecordBatch(STREAM_ID, 0, offset + 2, 1, TestUtils.random(1024 * 1024 / 4)),
                        new StreamRecordBatch(STREAM_ID, 0, offset + 3, 1, TestUtils.random(1024 * 1024 / 4)),
                        new StreamRecordBatch(STREAM_ID, 0, offset + 4, 1, TestUtils.random(1024 * 1024 / 4))
                    )).build()
            );
        }

        eventLoops = new DefaultEventLoop[1];
        eventLoops[0] = new DefaultEventLoop();

        objectManager = mock(ObjectManager.class);
        objectReaderFactory = m -> objects.get(m.objectId()).objectReader();
        dataBlockCache = spy(new DataBlockCache(Long.MAX_VALUE, eventLoops));
        streamReader = new StreamReader(STREAM_ID, 0, eventLoops[0], objectManager, objectReaderFactory, dataBlockCache);
    }

    @Test
    public void testRead_withReadahead() throws ExecutionException, InterruptedException {
        // user read get objects
        when(objectManager.getObjects(eq(STREAM_ID), eq(0L), eq(-1L), eq(StreamReader.GET_OBJECT_STEP)))
            .thenReturn(CompletableFuture.completedFuture(List.of(objects.get(0L).metadata, objects.get(1L).metadata, objects.get(2L).metadata, objects.get(3L).metadata)));
        when(objectManager.getObjects(eq(STREAM_ID), eq(14L), eq(-1L), eq(StreamReader.GET_OBJECT_STEP)))
            .thenReturn(CompletableFuture.completedFuture(List.of(objects.get(4L).metadata, objects.get(5L).metadata, objects.get(6L).metadata, objects.get(7L).metadata)));

        AtomicReference<CompletableFuture<ReadDataBlock>> readCf = new AtomicReference<>();

        eventLoops[0].submit(() -> readCf.set(streamReader.read(0, 29, 21))).get();
        ReadDataBlock rst = readCf.get().get();
        assertEquals(3, rst.getRecords().size());
        assertEquals(0, rst.getRecords().get(0).getBaseOffset());
        assertEquals(1, rst.getRecords().get(1).getBaseOffset());
        assertEquals(2, rst.getRecords().get(2).getBaseOffset());
        rst.getRecords().forEach(StreamRecordBatch::release);
        // the block should be removed after read

        eventLoops[0].submit(() -> {
            assertFalse(streamReader.blocksMap.containsKey(0L));
            assertFalse(streamReader.blocksMap.containsKey(1L));
            assertFalse(streamReader.blocksMap.containsKey(2L));
        }).get();

        // the user read touch {objId=0, blk=0} {objId=1, blk=0..1}
        // after the user read, readahead is triggered expect readahead {objId=1, blk=2} {objId=2, blk=0..3}
        verify(dataBlockCache, timeout(1000).times(8)).getBlock(any(), any());
        verify(objectManager, times(1)).getObjects(anyLong(), anyLong(), anyLong(), anyInt());
        assertEquals(3L, streamReader.nextReadOffset);
        assertEquals(14, streamReader.loadedBlockIndexEndOffset);
        assertEquals(8L, streamReader.readahead.nextReadaheadOffset);
        assertEquals(3L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(1024L * 1024, streamReader.readahead.nextReadaheadSize);
        assertEquals(3, dataBlockCache.caches[0].inactive.size());

        eventLoops[0].submit(() -> readCf.set(streamReader.read(3L, 29L, 1))).get();
        rst = readCf.get().get();
        assertEquals(1, rst.getRecords().size());
        assertEquals(3, rst.getRecords().get(0).getBaseOffset());
        rst.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(4L, streamReader.nextReadOffset);
        // the user read touch {objId=1, blk=2}
        // after the user read, readahead is triggered expect readahead {objId=2, blk=4} {objId=3, blk=0..2}
        verify(dataBlockCache, timeout(1000).times(13)).getBlock(any(), any());
        assertEquals(14L, streamReader.loadedBlockIndexEndOffset);
        assertEquals(12L, streamReader.readahead.nextReadaheadOffset);
        assertEquals(8L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(1024L * 1024, streamReader.readahead.nextReadaheadSize);
        assertEquals(4, dataBlockCache.caches[0].inactive.size());

        eventLoops[0].submit(() -> readCf.set(streamReader.read(4L, 29L, 1))).get();
        rst = readCf.get().get();
        assertEquals(1, rst.getRecords().size());
        assertEquals(4, rst.getRecords().get(0).getBaseOffset());
        rst.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(5L, streamReader.nextReadOffset);
        // the user read touch {objId=2, blk=0}
        // won't trigger readahead
        verify(dataBlockCache, timeout(1000).times(14)).getBlock(any(), any());
        assertEquals(14L, streamReader.loadedBlockIndexEndOffset);
        assertEquals(12L, streamReader.readahead.nextReadaheadOffset);
        assertEquals(8L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(1024L * 1024, streamReader.readahead.nextReadaheadSize);
        assertEquals(5, dataBlockCache.caches[0].inactive.size());

        eventLoops[0].submit(() -> readCf.set(streamReader.read(5L, 14L, Integer.MAX_VALUE))).get();
        rst = readCf.get().get();
        assertEquals(9, rst.getRecords().size());
        rst.getRecords().forEach(StreamRecordBatch::release);
        // expect readahead expands to twice
        // - load more index
        // - readahead from offset = 14...22
        verify(dataBlockCache, timeout(1000).times(14 + 9 + 8)).getBlock(any(), any());
        assertEquals(34L, streamReader.loadedBlockIndexEndOffset);
        assertEquals(14L + 8, streamReader.readahead.nextReadaheadOffset);
        assertEquals(14L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(1024L * 1024 * 2, streamReader.readahead.nextReadaheadSize);
        assertEquals(14, dataBlockCache.caches[0].inactive.size());
    }

}
