/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.StreamRecordBatchCodec;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.utils.MockTime;
import com.automq.stream.utils.threads.EventLoop;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit") public class DataBlockCacheTest {
    static final long STREAM_ID = 233;

    EventLoop[] eventLoops;
    MockTime time = new MockTime();
    DataBlockCache cache;

    @BeforeEach
    void setup() {
        eventLoops = new EventLoop[] {new EventLoop("")};
        time = new MockTime();
        cache = new DataBlockCache(1024, eventLoops, time);
    }

    @Test
    public void testGetBlock() throws ExecutionException, InterruptedException, TimeoutException {
        ObjectReader objectReader = mock(ObjectReader.class);
        when(objectReader.metadata()).thenReturn(new S3ObjectMetadata(233L, 100000, S3ObjectType.STREAM));
        DataBlockIndex idx1 = new DataBlockIndex(STREAM_ID, 0, 10, 1, 0, 100);
        DataBlockIndex idx2 = new DataBlockIndex(STREAM_ID, 20, 10, 1, 400, 200);
        DataBlockIndex idx3 = new DataBlockIndex(STREAM_ID, 60, 10, 1, 1000, 1500);
        DataBlockIndex idx4 = new DataBlockIndex(STREAM_ID, 90, 10, 1, 3000, 500);

        CompletableFuture<ObjectReader.DataBlockGroup> readCf1 = new CompletableFuture<>();
        when(objectReader.read(eq(idx1))).thenReturn(readCf1);
        CompletableFuture<ObjectReader.DataBlockGroup> readCf2 = new CompletableFuture<>();
        when(objectReader.read(eq(idx2))).thenReturn(readCf2);
        CompletableFuture<ObjectReader.DataBlockGroup> readCf3 = new CompletableFuture<>();
        when(objectReader.read(eq(idx3))).thenReturn(readCf3);
        CompletableFuture<ObjectReader.DataBlockGroup> readCf4 = new CompletableFuture<>();
        when(objectReader.read(eq(idx4))).thenReturn(readCf4);

        AtomicReference<CompletableFuture<DataBlock>> cf1 = new AtomicReference<>();
        AtomicReference<CompletableFuture<DataBlock>> cf2 = new AtomicReference<>();
        AtomicReference<CompletableFuture<DataBlock>> cf3 = new AtomicReference<>();
        AtomicReference<CompletableFuture<DataBlock>> cf4 = new AtomicReference<>();
        eventLoops[0].submit(() -> {
            cf1.set(cache.getBlock(objectReader, idx1));
            cf2.set(cache.getBlock(objectReader, idx2));
            cf3.set(cache.getBlock(objectReader, idx3));
            cf4.set(cache.getBlock(objectReader, idx4));

            // the #getBlock(..., idx4) will be blocked by the sizeLimiter
            verify(objectReader, times(3)).read(any());
            assertEquals(1024 - (100 + 200 + 1500), cache.sizeLimiter.permits());
            assertEquals(4, cache.caches[0].blocks.size());

            readCf1.complete(new ObjectReader.DataBlockGroup(newDataBlockGroupBuf(idx1)));
        }).get();
        cf1.get().get().freeFuture().get(1, TimeUnit.SECONDS);
        eventLoops[0].submit(() -> {
            assertTrue(cf1.get().isDone());
            try {
                assertEquals(1, cf1.get().get().dataBuf().refCnt());
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            assertFalse(cf2.get().isDone());
            // the idx1 is evicted cause of the cache is overflow
            assertEquals(1024 - (200 + 1500), cache.sizeLimiter.permits());
            assertEquals(3, cache.caches[0].blocks.size());
            // the #getBlock(..., idx4) is still blocked, cause of idx2 + idx3 = 1200 > 1024
            verify(objectReader, times(3)).read(any());

            readCf2.complete(new ObjectReader.DataBlockGroup(newDataBlockGroupBuf(idx2)));
            readCf3.complete(new ObjectReader.DataBlockGroup(newDataBlockGroupBuf(idx3)));

        }).get();
        verify(objectReader, timeout(1000).times(4)).read(any());
        eventLoops[0].submit(() -> {
            assertTrue(cf2.get().isDone());
            assertTrue(cf3.get().isDone());
            assertFalse(cf4.get().isDone());
            // the idx2, idx3 is evicted cause of the cache is full
            assertEquals(1024 - 500, cache.sizeLimiter.permits());
            assertEquals(1, cache.caches[0].blocks.size());

            readCf4.complete(new ObjectReader.DataBlockGroup(newDataBlockGroupBuf(idx4)));
        }).get();
        eventLoops[0].submit(() -> {
            assertTrue(cf4.get().isDone());
            assertEquals(1024 - 500, cache.sizeLimiter.permits());
            assertEquals(1, cache.caches[0].blocks.size());
            assertEquals(1, cache.caches[0].lru.size());
        }).get();

    }

    @Test
    public void testGetBlock_unread() throws ExecutionException, InterruptedException, TimeoutException {
        ObjectReader objectReader = mock(ObjectReader.class);
        doAnswer(args -> {
            DataBlockIndex idx = args.getArgument(0);
            return CompletableFuture.completedFuture(new ObjectReader.DataBlockGroup(newDataBlockGroupBuf(idx)));
        }).when(objectReader).read(any());
        when(objectReader.metadata()).thenReturn(new S3ObjectMetadata(233L, 100000, S3ObjectType.STREAM));

        AtomicReference<CompletableFuture<?>> cf1 = new AtomicReference<>();
        eventLoops[0].submit(() -> {
            cf1.set(cache.getBlock(objectReader, new DataBlockIndex(STREAM_ID, 0, 10, 1, 0, 100)).thenAccept(DataBlock::markUnread));
        }).get();
        AtomicReference<CompletableFuture<DataBlock>> cf2 = new AtomicReference<>();
        eventLoops[0].submit(() -> {
            cf2.set(cache.getBlock(objectReader, new DataBlockIndex(STREAM_ID, 100, 10, 1, 1000, 900)).whenComplete((b, ex) -> {
                b.markUnread();
                b.markRead();
            }));
        }).get();
        eventLoops[0].submit(() -> {
            assertEquals(1, cache.caches[0].blocks.size());
            cache.getBlock(objectReader, new DataBlockIndex(STREAM_ID, 0, 200, 1, 0, 100));
        }).get();
        cf2.get().get().freeFuture().get(1, TimeUnit.SECONDS);
        eventLoops[0].submit(() -> {
            // expect idx2 is firstly evicted cause of idx2 is markRead
            DataBlockCache.Cache cache = this.cache.caches[0];
            assertEquals(2, cache.blocks.size());
            assertEquals(2, cache.lru.size());
            assertTrue(cache.blocks.containsKey(new DataBlockCache.DataBlockGroupKey(233, new DataBlockIndex(STREAM_ID, 0, 10, 1, 0, 100))));
            assertTrue(cache.blocks.containsKey(new DataBlockCache.DataBlockGroupKey(233, new DataBlockIndex(STREAM_ID, 0, 200, 1, 0, 100))));
            assertEquals(1024 - 100 - 100, this.cache.sizeLimiter.permits());
        }).get();

        AtomicReference<CompletableFuture<?>> cf3 = new AtomicReference<>();
    }

    @Test
    public void testGetBlock_evictExpired() throws ExecutionException, InterruptedException, TimeoutException {
        ObjectReader objectReader = mock(ObjectReader.class);
        doAnswer(args -> {
            DataBlockIndex idx = args.getArgument(0);
            return CompletableFuture.completedFuture(new ObjectReader.DataBlockGroup(newDataBlockGroupBuf(idx)));
        }).when(objectReader).read(any());
        when(objectReader.metadata()).thenReturn(new S3ObjectMetadata(233L, 100000, S3ObjectType.STREAM));

        AtomicReference<CompletableFuture<?>> cf1 = new AtomicReference<>();
        eventLoops[0].submit(() -> {
            cf1.set(cache.getBlock(objectReader, new DataBlockIndex(STREAM_ID, 0, 10, 1, 0, 100)).thenAccept(DataBlock::markUnread));
        }).get();
        cf1.get().get(1, TimeUnit.SECONDS);

        AtomicReference<CompletableFuture<?>> cf2 = new AtomicReference<>();
        time.setCurrentTimeMs(time.milliseconds() + DataBlockCache.DATA_TTL - 1);
        eventLoops[0].submit(() -> {
            cf2.set(cache.getBlock(objectReader, new DataBlockIndex(STREAM_ID, 100, 10, 1, 100, 200)).thenAccept(DataBlock::markUnread));
        }).get();
        cf2.get().get(1, TimeUnit.SECONDS);
        assertEquals(2, cache.caches[0].blocks.size());

        // expect idx1 is evicted cause of the expired
        time.setCurrentTimeMs(time.milliseconds() + 2);
        AtomicReference<CompletableFuture<?>> cf3 = new AtomicReference<>();
        eventLoops[0].submit(() -> {
            cf3.set(cache.getBlock(objectReader, new DataBlockIndex(STREAM_ID, 100, 10, 1, 100, 200)).thenAccept(DataBlock::markUnread));
        }).get();
        cf3.get().get(1, TimeUnit.SECONDS);
        assertEquals(1, cache.caches[0].blocks.size());
        assertEquals(100, cache.caches[0].blocks.keySet().iterator().next().dataBlockIndex.startOffset());
    }

    private ByteBuf newDataBlockGroupBuf(DataBlockIndex index) {
        int remainingSize = index.size() - index.recordCount() * StreamRecordBatchCodec.HEADER_SIZE - ObjectWriter.DataBlock.BLOCK_HEADER_SIZE;
        if (remainingSize <= 0) {
            throw new IllegalArgumentException("Invalid index:" + index);
        }
        List<StreamRecordBatch> records = new ArrayList<>(index.recordCount());
        long offset = index.startOffset();
        // the first N - 1 record's count = 1, body size = 1
        for (int i = 0; i < index.recordCount() - 1; i++, offset++) {
            records.add(new StreamRecordBatch(STREAM_ID, 0, offset, 1, TestUtils.random(1)));
        }
        // the last record padding the remaining
        records.add(new StreamRecordBatch(STREAM_ID, 0, offset, index.endOffsetDelta() - (index.recordCount() - 1), TestUtils.random(remainingSize)));
        ByteBuf buf = new ObjectWriter.DataBlock(STREAM_ID, records).buffer();
        assertEquals(index.size(), buf.readableBytes());
        return buf;
    }

}
