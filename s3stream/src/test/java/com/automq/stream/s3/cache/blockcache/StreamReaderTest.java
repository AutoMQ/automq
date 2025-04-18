/*
 * Copyright 2025, AutoMQ HK Limited.
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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.threads.EventLoop;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.automq.stream.s3.cache.blockcache.StreamReader.GET_OBJECT_STEP;
import static com.automq.stream.s3.cache.blockcache.StreamReader.READAHEAD_SIZE_UNIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
    private EventLoop[] eventLoops;
    private ObjectManager objectManager;
    private ObjectReaderFactory objectReaderFactory;
    private DataBlockCache dataBlockCache;

    private StreamReader streamReader;

    @BeforeEach
    void setup() {
        objects = new HashMap<>();
        // object=0 [0, 1)

        // object=1 [1, 4)

        // object=2 [4, 9)
        // object=3 [9, 14)
        // object=4 [14, 19)
        // object=5 [19, 24)
        // object=6 [24, 29)
        // object=7 [29, 34)
        objects.put(0L, MockObject.builder(0, BLOCK_SIZE_THRESHOLD).mockDelay(100).write(STREAM_ID, List.of(
            new StreamRecordBatch(STREAM_ID, 0, 0, 1, TestUtils.random(1))
        )).build());
        objects.put(1L, MockObject.builder(1L, 1).mockDelay(100).write(STREAM_ID, List.of(
            new StreamRecordBatch(STREAM_ID, 0, 1, 1, TestUtils.random(19)),
            new StreamRecordBatch(STREAM_ID, 0, 2, 1, TestUtils.random(10)),
            new StreamRecordBatch(STREAM_ID, 0, 3, 1, TestUtils.random(10))
        )).build());
        for (int i = 0; i < 6; i++) {
            long offset = 4 + i * 5;
            objects.put(i + 2L, MockObject.builder(i + 2L, BLOCK_SIZE_THRESHOLD).mockDelay(100).write(STREAM_ID, List.of(
                new StreamRecordBatch(STREAM_ID, 0, offset, 1, TestUtils.random(1024 * 1024 / 4)),
                new StreamRecordBatch(STREAM_ID, 0, offset + 1, 1, TestUtils.random(1024 * 1024 / 4)),
                new StreamRecordBatch(STREAM_ID, 0, offset + 2, 1, TestUtils.random(1024 * 1024 / 4)),
                new StreamRecordBatch(STREAM_ID, 0, offset + 3, 1, TestUtils.random(1024 * 1024 / 4)),
                new StreamRecordBatch(STREAM_ID, 0, offset + 4, 1, TestUtils.random(1024 * 1024 / 4))
            )).build());
        }

        eventLoops = new EventLoop[1];
        eventLoops[0] = new EventLoop("");

        objectManager = mock(ObjectManager.class);
        when(objectManager.isObjectExist(anyLong())).thenReturn(true);
        objectReaderFactory = new ObjectReaderFactory() {
            @Override
            public ObjectReader get(S3ObjectMetadata metadata) {
                return objects.get(metadata.objectId()).objectReader();
            }

            @Override
            public ObjectStorage getObjectStorage() {
                return mock(MemoryObjectStorage.class);
            }
        };
        dataBlockCache = spy(new DataBlockCache(Long.MAX_VALUE, eventLoops));
        streamReader = new StreamReader(STREAM_ID, 0, eventLoops[0], objectManager, objectReaderFactory, dataBlockCache);
    }

    @Test
    public void testRead_withReadahead() throws ExecutionException, InterruptedException, TimeoutException {
        // user read get objects
        when(objectManager.getObjects(eq(STREAM_ID), eq(0L), eq(29L), eq(GET_OBJECT_STEP))).thenReturn(CompletableFuture.completedFuture(List.of(objects.get(0L).metadata, objects.get(1L).metadata, objects.get(2L).metadata, objects.get(3L).metadata)));
        when(objectManager.getObjects(eq(STREAM_ID), eq(14L), eq(-1L), eq(GET_OBJECT_STEP))).thenReturn(CompletableFuture.completedFuture(List.of(objects.get(4L).metadata, objects.get(5L).metadata, objects.get(6L).metadata, objects.get(7L).metadata)));
        when(objectManager.getObjects(eq(STREAM_ID), eq(14L), eq(15L), eq(GET_OBJECT_STEP))).thenReturn(CompletableFuture.completedFuture(List.of(objects.get(4L).metadata)));

        AtomicReference<CompletableFuture<ReadDataBlock>> readCf = new AtomicReference<>();

        eventLoops[0].submit(() -> readCf.set(streamReader.read(0, 29, 21))).get();
        ReadDataBlock rst = readCf.get().get();
        waitForStreamReaderUpdate();
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
        // after the user read, readahead is triggered expect readahead 1MB {objId=1, blk=2} {objId=2, blk=0..3} cause of 1 cache miss
        verify(dataBlockCache, timeout(1000).times(8)).getBlock(any(), any(), any());
        verify(objectManager, times(1)).getObjects(anyLong(), anyLong(), anyLong(), anyInt());
        assertEquals(3L, streamReader.nextReadOffset);
        assertEquals(14, streamReader.loadedBlockIndexEndOffset);
        assertEquals(8L, streamReader.readahead.nextReadaheadOffset);
        assertEquals(3L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(READAHEAD_SIZE_UNIT * 2, streamReader.readahead.nextReadaheadSize);

        // await block load complete
        dataBlockCache.caches[0].blocks.values().forEach(d -> d.dataFuture().join());

        eventLoops[0].submit(() -> readCf.set(streamReader.read(3L, 29L, 1))).get();
        rst = readCf.get().get();
        waitForStreamReaderUpdate();
        assertEquals(1, rst.getRecords().size());
        assertEquals(3, rst.getRecords().get(0).getBaseOffset());
        rst.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(4L, streamReader.nextReadOffset);
        // the user read touch {objId=1, blk=2}
        // after the user read, readahead is triggered expect readahead 1MB {objId=2, blk=4} {objId=3, blk=0..1} cause of 1 cache miss
        eventLoops[0].submit(() -> {
            assertEquals(14L, streamReader.loadedBlockIndexEndOffset);
            assertEquals(12L, streamReader.readahead.nextReadaheadOffset);
            assertEquals(8L, streamReader.readahead.readaheadMarkOffset);
            assertEquals(READAHEAD_SIZE_UNIT * 2, streamReader.readahead.nextReadaheadSize);
        }).get();

        eventLoops[0].submit(() -> readCf.set(streamReader.read(4L, 29L, 1))).get();
        rst = readCf.get().get();
        waitForStreamReaderUpdate();
        assertEquals(1, rst.getRecords().size());
        assertEquals(4, rst.getRecords().get(0).getBaseOffset());
        rst.getRecords().forEach(StreamRecordBatch::release);

        assertEquals(5L, streamReader.nextReadOffset);
        // the user read touch {objId=2, blk=0}
        // won't trigger readahead
        verify(dataBlockCache, timeout(1000).times(14)).getBlock(any(), any(), any());
        assertEquals(14L, streamReader.loadedBlockIndexEndOffset);
        assertEquals(12L, streamReader.readahead.nextReadaheadOffset);
        assertEquals(8L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(READAHEAD_SIZE_UNIT * 2, streamReader.readahead.nextReadaheadSize);

        eventLoops[0].submit(() -> readCf.set(streamReader.read(5L, 14L, Integer.MAX_VALUE))).get();
        rst = readCf.get().get();
        waitForStreamReaderUpdate();
        assertEquals(9, rst.getRecords().size());
        rst.getRecords().forEach(StreamRecordBatch::release);
        // - load more index
        verify(dataBlockCache, timeout(1000).times(14 + 9 + 6)).getBlock(any(), any(), any());
        assertEquals(34L, streamReader.loadedBlockIndexEndOffset);
        assertEquals(14L + 6, streamReader.readahead.nextReadaheadOffset);
        assertEquals(14L, streamReader.readahead.readaheadMarkOffset);
        assertEquals(READAHEAD_SIZE_UNIT * 3, streamReader.readahead.nextReadaheadSize);

        when(objectManager.isObjectExist(anyLong())).thenReturn(false);
        eventLoops[0].submit(() -> readCf.set(streamReader.read(14L, 15L, Integer.MAX_VALUE))).get();
        Throwable ex = null;
        try {
            readCf.get().get(1, TimeUnit.SECONDS);
        } catch (Throwable e) {
            ex = FutureUtil.cause(e);
        }
        assertInstanceOf(ObjectNotExistException.class, ex);

        AtomicBoolean failed = new AtomicBoolean(false);
        doAnswer(args -> {
            if (failed.get()) {
                return true;
            } else {
                failed.set(true);
                return false;
            }
        }).when(objectManager).isObjectExist(anyLong());
        eventLoops[0].submit(() -> readCf.set(streamReader.read(14L, 15L, Integer.MAX_VALUE))).get();

        // verify blocks free listener
        List<StreamReader.Block> blocks = new ArrayList<>(streamReader.blocksMap.values());
        blocks.forEach(b -> Assertions.assertFalse(b.data.freeListeners.isEmpty()));
        eventLoops[0].submit(() -> streamReader.close());
        blocks.forEach(b -> Assertions.assertTrue(b.data.freeListeners.isEmpty()));
    }

    public void waitForStreamReaderUpdate() throws ExecutionException, InterruptedException {
        // The order of waiting cannot be changed
        CompletableFuture<Void> afterReadTryReadaheadCf = streamReader.getAfterReadTryReadaheadCf();
        if (afterReadTryReadaheadCf != null) {
            afterReadTryReadaheadCf.get();
        }

        CompletableFuture<Void> inflightReadaheadCf = streamReader.getReadaheadInflightReadaheadCf();
        if (inflightReadaheadCf != null) {
            inflightReadaheadCf.get();
        }
    }

}
