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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class StreamRecordBatchDiskCacheTest {
    @TempDir
    Path directory;

    /**
     * Preserves the full batch encoding, including link counts, across mappings and source release.
     * Repeated reads own independent buffers with exactly one remaining reference.
     */
    @ParameterizedTest
    @ValueSource(ints = {4, -4})
    public void testRoundTrip(int count) throws IOException {
        StreamRecordBatchDiskCache cache = new StreamRecordBatchDiskCache(
            new FileCache(directory.resolve("cache").toString(), 128, 8, 16));
        StreamRecordBatch source = batch(17, 2, 100, count, new byte[] {1, 2, 3, 4, 5});
        byte[] expected = ByteBufUtil.getBytes(source.encoded());
        FileCache.ReadRange range;
        try {
            ByteBuf encoded = source.encoded();
            int readerIndex = encoded.readerIndex();
            int writerIndex = encoded.writerIndex();
            range = cache.put(source);
            assertEquals(1, encoded.refCnt());
            assertEquals(readerIndex, encoded.readerIndex());
            assertEquals(writerIndex, encoded.writerIndex());
            source.getPayload().setByte(0, 99);
        } finally {
            source.release();
        }

        StreamRecordBatch firstRead = cache.get(range).orElseThrow();
        try {
            assertEquals(17, firstRead.getStreamId());
            assertEquals(2, firstRead.getEpoch());
            assertEquals(100, firstRead.getBaseOffset());
            assertEquals(count, firstRead.getCount());
            assertEquals(104, firstRead.getLastOffset());
            assertEquals(5, firstRead.size());
            assertEquals(1, firstRead.encoded().refCnt());
            assertArrayEquals(expected, ByteBufUtil.getBytes(firstRead.encoded()));
            firstRead.getPayload().setByte(0, 88);
        } finally {
            firstRead.release();
        }
        StreamRecordBatch secondRead = cache.get(range).orElseThrow();
        try {
            assertArrayEquals(expected, ByteBufUtil.getBytes(secondRead.encoded()));
            assertEquals(1, secondRead.encoded().refCnt());
        } finally {
            secondRead.release();
        }
    }

    /**
     * Evicts a batch independently and reuses its file blocks without reviving its range.
     * A previously returned batch remains readable after that eviction.
     */
    @Test
    public void testEvictionAndBlockReuse() throws IOException {
        FileCache fileCache = new FileCache(directory.resolve("cache").toString(), 80, 8, 16);
        StreamRecordBatchDiskCache cache = new StreamRecordBatchDiskCache(fileCache);
        StreamRecordBatch first = batch(17, 2, 100, 1, new byte[] {1});
        StreamRecordBatch second = batch(17, 2, 101, 1, new byte[] {2});
        StreamRecordBatch third = batch(18, 3, 102, 1, new byte[] {3});
        try {
            FileCache.ReadRange firstRange = cache.put(first);
            FileCache.ReadRange secondRange = cache.put(second);
            StreamRecordBatch secondRead = cache.get(secondRange).orElseThrow();
            try {
                FileCache.ReadRange thirdRange = cache.put(third);
                assertTrue(cache.get(firstRange).isEmpty());
                assertCached(cache, secondRange, second);
                assertCached(cache, thirdRange, third);

                // Touch the third batch, then evict the second batch while its copy is still held.
                cache.put(first);
                assertTrue(cache.get(secondRange).isEmpty());
                assertArrayEquals(ByteBufUtil.getBytes(second.encoded()), ByteBufUtil.getBytes(secondRead.encoded()));
                assertEquals(1, fileCache.cacheMap.size());
                assertEquals(2, fileCache.cacheMap.values().iterator().next().size());
            } finally {
                secondRead.release();
            }
        } finally {
            first.release();
            second.release();
            third.release();
        }
    }

    /**
     * Disabled or undersized storage produces a miss.
     */
    @Test
    public void testMisses() throws IOException {
        StreamRecordBatch source = batch(17, 2, 100, 1, new byte[] {1});
        try {
            StreamRecordBatchDiskCache disabled = new StreamRecordBatchDiskCache(
                directory.resolve("disabled-cache").toString(), 0);
            FileCache.ReadRange disabledRange = disabled.put(source);
            StreamRecordBatchDiskCache.BulkReadResult disabledResult = disabled.get(List.of(disabledRange));
            assertEquals(List.of(disabledRange), disabledResult.missingRanges());
            assertTrue(disabledResult.batches().isEmpty());
            assertTrue(disabled.get(disabledRange).isEmpty());
            StreamRecordBatchDiskCache undersized = new StreamRecordBatchDiskCache(
                new FileCache(directory.resolve("small-cache").toString(), 8, 8, 16));
            FileCache.ReadRange undersizedRange = undersized.put(source);
            StreamRecordBatchDiskCache.BulkReadResult undersizedResult = undersized.get(List.of(undersizedRange));
            assertEquals(List.of(undersizedRange), undersizedResult.missingRanges());
            assertTrue(undersizedResult.batches().isEmpty());
            assertTrue(undersized.get(undersizedRange).isEmpty());
        } finally {
            source.release();
        }
    }

    /**
     * Reads complete lists in order; all stale ranges are reported without reading or touching cached batches.
     */
    @Test
    public void testReadBatchRanges() throws IOException {
        FileCache fileCache = new FileCache(directory.resolve("cache").toString(), 80, 8, 16);
        StreamRecordBatchDiskCache cache = new StreamRecordBatchDiskCache(fileCache);
        StreamRecordBatch first = batch(17, 2, 100, 1, new byte[] {1});
        StreamRecordBatch second = batch(17, 2, 101, 1, new byte[] {2});
        StreamRecordBatch third = batch(18, 3, 102, 1, new byte[] {3});
        try {
            FileCache.ReadRange firstRange = cache.put(first);
            FileCache.ReadRange secondRange = cache.put(second);
            StreamRecordBatchDiskCache.BulkReadResult empty = cache.get(List.of());
            assertTrue(empty.batches().isEmpty());
            assertTrue(empty.missingRanges().isEmpty());
            StreamRecordBatchDiskCache.BulkReadResult initialResult = cache.get(List.of(firstRange, secondRange));
            List<StreamRecordBatch> initial = initialResult.batches();
            try {
                assertTrue(initialResult.missingRanges().isEmpty());
                assertEquals(2, initial.size());
                assertArrayEquals(ByteBufUtil.getBytes(first.encoded()), ByteBufUtil.getBytes(initial.get(0).encoded()));
                assertArrayEquals(ByteBufUtil.getBytes(second.encoded()), ByteBufUtil.getBytes(initial.get(1).encoded()));
            } finally {
                initial.forEach(StreamRecordBatch::release);
            }
            FileCache.ReadRange thirdRange = cache.put(third);
            StreamRecordBatchDiskCache.BulkReadResult evicted = cache.get(List.of(secondRange, firstRange, thirdRange));
            assertEquals(List.of(firstRange), evicted.missingRanges());
            assertTrue(evicted.batches().isEmpty());
            FileCache.ReadRange replacementRange = cache.put(first);
            assertTrue(cache.get(secondRange).isEmpty());
            assertTrue(cache.get(firstRange).isEmpty());
            StreamRecordBatchDiskCache.BulkReadResult missing = cache.get(List.of(firstRange, thirdRange, secondRange));
            assertEquals(List.of(firstRange, secondRange), missing.missingRanges());
            assertTrue(missing.batches().isEmpty());
            StreamRecordBatchDiskCache.BulkReadResult result = cache.get(List.of(replacementRange, thirdRange));
            List<StreamRecordBatch> results = result.batches();
            try {
                assertTrue(result.missingRanges().isEmpty());
                assertEquals(2, results.size());
                assertArrayEquals(ByteBufUtil.getBytes(first.encoded()), ByteBufUtil.getBytes(results.get(0).encoded()));
                assertArrayEquals(ByteBufUtil.getBytes(third.encoded()), ByteBufUtil.getBytes(results.get(1).encoded()));
                assertEquals(1, results.get(0).encoded().refCnt());
                assertEquals(1, results.get(1).encoded().refCnt());
            } finally {
                results.forEach(StreamRecordBatch::release);
            }
        } finally {
            first.release();
            second.release();
            third.release();
        }
    }

    /**
     * A later decode failure releases both the earlier decoded batch and every encoded read buffer.
     */
    @Test
    public void testBulkDecodeFailureReleasesBuffers() {
        FileCache fileCache = mock(FileCache.class);
        StreamRecordBatchDiskCache cache = new StreamRecordBatchDiskCache(fileCache);
        StreamRecordBatch source = batch(17, 2, 100, 1, new byte[] {1});
        try {
            FileCache.ReadRange first = cache.put(source);
            FileCache.ReadRange second = cache.put(source);
            ByteBuf valid = source.encoded().copy();
            ByteBuf invalid = source.encoded().copy();
            try {
                invalid.setByte(0, 0);
                when(fileCache.get(anyList())).thenReturn(new FileCache.BulkReadResult(List.of(valid, invalid), List.of()));
                assertThrows(RuntimeException.class, () -> cache.get(List.of(first, second)));
                assertEquals(0, valid.refCnt());
                assertEquals(0, invalid.refCnt());
            } finally {
                if (valid.refCnt() > 0) {
                    valid.release(valid.refCnt());
                }
                if (invalid.refCnt() > 0) {
                    invalid.release(invalid.refCnt());
                }
            }
        } finally {
            source.release();
        }
    }

    private StreamRecordBatch batch(long streamId, long epoch, long baseOffset, int count, byte[] payload) {
        return StreamRecordBatch.of(streamId, epoch, baseOffset, count, Unpooled.wrappedBuffer(payload),
            DefaultByteBufSupplier.INSTANCE);
    }

    private void assertCached(StreamRecordBatchDiskCache cache, FileCache.ReadRange range,
        StreamRecordBatch expected) {
        StreamRecordBatch actual = cache.get(range).orElseThrow();
        try {
            assertArrayEquals(ByteBufUtil.getBytes(expected.encoded()), ByteBufUtil.getBytes(actual.encoded()));
        } finally {
            actual.release();
        }
    }
}
