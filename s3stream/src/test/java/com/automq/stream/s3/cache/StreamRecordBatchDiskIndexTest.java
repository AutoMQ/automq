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

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class StreamRecordBatchDiskIndexTest {
    @TempDir
    Path directory;

    /**
     * Finds complete ordinary and link batches spanning a range after source release.
     * Out-of-order admission preserves stream isolation and exclusive range boundaries.
     */
    @ParameterizedTest
    @ValueSource(ints = {4, -4})
    public void testRangeLookup(int count) throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        FileCache.ReadRange later = put(index, cache, 17, 112, count);
        FileCache.ReadRange first = put(index, cache, 17, 100, count);
        FileCache.ReadRange second = put(index, cache, 17, 104, count);
        FileCache.ReadRange otherStream = put(index, cache, 18, 100, count);

        assertRanges(List.of(first, second), index.get(17, 100, 108, 100));
        assertRanges(List.of(first, second), index.get(17, 103, 107, 100));
        assertRanges(List.of(first), index.get(17, 100, 104, 100));
        assertRanges(List.of(second), index.get(17, 104, 108, 100));
        assertRanges(List.of(later), index.get(17, 112, 116, 100));
        assertRanges(List.of(otherStream), index.get(18, 101, 103, 100));
        assertTrue(index.get(17, 99, 108, 100).isEmpty());
        assertTrue(index.get(17, 108, 112, 100).isEmpty());
        assertTrue(index.get(17, 100, 116, 100).isEmpty());
        assertTrue(index.get(16, 100, 104, 100).isEmpty());
        assertTrue(index.get(19, 100, 104, 100).isEmpty());
        assertEquals(4, index.size());

        StreamRecordBatch read = cache.get(index.get(17, 103, 104, 100).get(0)).orElseThrow();
        try {
            assertEquals(17, read.getStreamId());
            assertEquals(100, read.getBaseOffset());
            assertEquals(104, read.getLastOffset());
            assertEquals(count, read.getCount());
            assertEquals(1, read.encoded().refCnt());
        } finally {
            read.release();
        }
    }

    /**
     * A crossing admission replaces both overlapping ranges, but preserves an adjacent range
     * and another stream. Re-admission and a later smaller range replace the previous range.
     */
    @Test
    public void testReplacementAndOverlaps() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        put(index, cache, 17, 100, 10);
        put(index, cache, 17, 120, 10);
        FileCache.ReadRange neighbor = put(index, cache, 17, 130, 5);
        FileCache.ReadRange otherStream = put(index, cache, 18, 100, 10);
        FileCache.ReadRange crossing = put(index, cache, 17, 105, 20);

        assertTrue(index.get(17, 100, 105, 100).isEmpty());
        assertRanges(List.of(crossing), index.get(17, 105, 125, 100));
        assertTrue(index.get(17, 125, 130, 100).isEmpty());
        assertRanges(List.of(neighbor), index.get(17, 130, 135, 100));
        assertRanges(List.of(otherStream), index.get(18, 100, 110, 100));
        assertEquals(3, index.size());

        FileCache.ReadRange newest = put(index, cache, 17, 105, 20);
        assertRanges(List.of(newest), index.get(17, 105, 125, 100));
        assertEquals(3, index.size());

        FileCache.ReadRange smaller = put(index, cache, 17, 105, 4);
        assertRanges(List.of(smaller), index.get(17, 105, 109, 100));
        assertTrue(index.get(17, 109, 125, 100).isEmpty());
        assertEquals(3, index.size());

        index.clearStream(17);
        assertEquals(1, index.size());
        assertTrue(index.get(17, 105, 115, 100).isEmpty());
        assertRanges(List.of(otherStream), index.get(18, 100, 110, 100));
    }

    /**
     * A wider admission removes several contained ranges without removing adjacent batches.
     * Replaced index metadata does not delete the old disk copies.
     */
    @Test
    public void testContainingRangeReplacesEntries() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        FileCache.ReadRange old = put(index, cache, 17, 100, 5);
        put(index, cache, 17, 105, 5);
        put(index, cache, 17, 110, 10);
        FileCache.ReadRange next = put(index, cache, 17, 120, 10);
        FileCache.ReadRange outer = put(index, cache, 17, 100, 20);

        assertRanges(List.of(outer), index.get(17, 108, 119, 100));
        assertRanges(List.of(outer, next), index.get(17, 108, 125, 100));
        assertEquals(2, index.size());
        StreamRecordBatch oldBatch = cache.get(old).orElseThrow();
        oldBatch.release();
    }

    /**
     * A later smaller batch replaces a larger same-start batch and can join its adjacent batch.
     */
    @Test
    public void testSmallerSameStartReplacement() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        put(index, cache, 17, 100, 15);
        FileCache.ReadRange first = put(index, cache, 17, 100, 10);
        FileCache.ReadRange second = put(index, cache, 17, 110, 10);

        assertRanges(List.of(first, second), index.get(17, 105, 120, 100));
        assertEquals(2, index.size());
    }

    /**
     * Replacing several overlapping ranges frees their LRU capacity without evicting other streams.
     */
    @Test
    public void testOverlapReplacementReleasesLruCapacity() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(3);
        put(index, cache, 17, 100, 5);
        put(index, cache, 17, 105, 5);
        FileCache.ReadRange other = put(index, cache, 18, 100, 5);
        FileCache.ReadRange replacement = put(index, cache, 17, 100, 10);
        FileCache.ReadRange last = put(index, cache, 19, 100, 5);

        assertEquals(3, index.size());
        assertRanges(List.of(replacement), index.get(17, 100, 110, 100));
        assertRanges(List.of(other), index.get(18, 100, 105, 100));
        assertRanges(List.of(last), index.get(19, 100, 105, 100));
    }

    /**
     * Crossing batches replace each other; stale read cleanup cannot remove the replacement.
     */
    @Test
    public void testCrossingRanges() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        put(index, cache, 17, 100, 20);
        FileCache.ReadRange crossing = put(index, cache, 17, 110, 20);

        assertRanges(List.of(crossing), index.get(17, 115, 125, 100));
        assertTrue(index.get(17, 105, 125, 100).isEmpty());
        assertEquals(1, index.size());
        List<FileCache.ReadRange> stale = index.get(17, 110, 130, 100);

        FileCache.ReadRange next = put(index, cache, 17, 120, 10);
        index.remove(stale);
        assertTrue(index.get(17, 105, 125, 100).isEmpty());
        assertRanges(List.of(next), index.get(17, 120, 125, 100));
        assertEquals(1, index.size());
    }

    /**
     * The soft budget counts payload bytes, allowing the complete final batch to exceed the limit.
     * A budget-limited result is valid before a later gap; an unfulfilled range is a miss.
     */
    @Test
    public void testPayloadByteLimit() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        FileCache.ReadRange first = put(index, cache, 17, 100, 4, 6);
        FileCache.ReadRange second = put(index, cache, 17, 104, 4, 6);
        FileCache.ReadRange third = put(index, cache, 17, 108, 4, 6);

        assertRanges(List.of(first), index.get(17, 100, 112, 1));
        assertRanges(List.of(first), index.get(17, 100, 112, 6));
        assertRanges(List.of(first, second), index.get(17, 100, 112, 7));
        assertRanges(List.of(first, second), index.get(17, 103, 111, 7));
        assertRanges(List.of(first, second, third), index.get(17, 100, 112, 100));
        assertRanges(List.of(first, second, third), index.get(17, 100, 116, 18));
        assertTrue(index.get(17, 100, 116, 19).isEmpty());
        assertTrue(index.get(17, 100, 112, 0).isEmpty());
        assertTrue(index.get(17, 100, 100, 100).isEmpty());
    }

    /**
     * A gap produces a miss unless the continuous prefix satisfies the soft byte limit.
     */
    @Test
    public void testGapLookup() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        FileCache.ReadRange prefix = put(index, cache, 17, 100, 10, 6);
        put(index, cache, 17, 120, 15, 6);
        FileCache.ReadRange first = put(index, cache, 17, 120, 10, 6);
        FileCache.ReadRange second = put(index, cache, 17, 130, 10, 6);
        FileCache.ReadRange last = put(index, cache, 17, 140, 10, 6);

        assertTrue(index.get(17, 100, 150, 100).isEmpty());
        assertTrue(index.get(17, 100, 150, 7).isEmpty());
        assertRanges(List.of(prefix), index.get(17, 100, 150, 6));
        assertRanges(List.of(first, second, last), index.get(17, 120, 150, 100));
        assertTrue(index.get(17, 100, 160, 100).isEmpty());
    }

    /**
     * Empty entries inside a batch do not hide its coverage. Empty entries at a join retain
     * their payload-budget behavior, but cannot start coverage or fill a gap.
     */
    @Test
    public void testLookupWithEmptyEntries() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        FileCache.ReadRange first = put(index, cache, 17, 100, 10, 6);
        put(index, cache, 17, 105, 0, 6);
        FileCache.ReadRange join = put(index, cache, 17, 110, 0, 6);
        FileCache.ReadRange second = put(index, cache, 17, 110, 10, 6);
        put(index, cache, 17, 125, 0, 6);
        put(index, cache, 18, 120, 10, 6);

        assertRanges(List.of(first), index.get(17, 106, 109, 100));
        assertRanges(List.of(first, join, second), index.get(17, 106, 119, 100));
        assertRanges(List.of(first, join), index.get(17, 106, 119, 7));
        assertRanges(List.of(second), index.get(17, 110, 119, 100));
        assertTrue(index.get(17, 120, 130, 1).isEmpty());
        assertTrue(index.get(17, 125, 130, 1).isEmpty());
        assertTrue(index.get(17, 106, 130, 100).isEmpty());
    }

    /**
     * Every selected batch is refreshed in the metadata LRU after a successful range lookup.
     */
    @Test
    public void testRangeLookupRefreshesLru() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(3);
        FileCache.ReadRange first = put(index, cache, 17, 100, 4);
        FileCache.ReadRange second = put(index, cache, 17, 104, 4);
        put(index, cache, 18, 100, 4);

        assertRanges(List.of(first, second), index.get(17, 100, 108, 100));
        put(index, cache, 19, 100, 4);
        assertRanges(List.of(first, second), index.get(17, 100, 108, 100));
        assertTrue(index.get(18, 100, 104, 100).isEmpty());
        assertEquals(3, index.size());
    }

    /**
     * A range miss does not promote a cached prefix or return an incomplete result.
     */
    @Test
    public void testMissingRangeDoesNotRefreshLru() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(2);
        put(index, cache, 17, 100, 4);
        put(index, cache, 18, 104, 4);

        assertTrue(index.get(17, 100, 108, 100).isEmpty());
        put(index, cache, 19, 100, 4);
        assertTrue(index.get(17, 100, 104, 100).isEmpty());
        assertEquals(2, index.size());
    }

    /**
     * Metadata eviction and stream clearing release index capacity without deleting disk bytes.
     * Stream churn remains subject to the global entry limit.
     */
    @Test
    public void testMetadataLimitAndStreamClear() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(2);
        FileCache.ReadRange first = put(index, cache, 17, 100, 4);
        FileCache.ReadRange second = put(index, cache, 18, 100, 4);
        assertRanges(List.of(first), index.get(17, 100, 104, 100));
        FileCache.ReadRange third = put(index, cache, 19, 100, 4);

        assertEquals(2, index.size());
        assertTrue(index.get(18, 100, 104, 100).isEmpty());
        assertRanges(List.of(first), index.get(17, 100, 104, 100));
        assertRanges(List.of(third), index.get(19, 100, 104, 100));
        StreamRecordBatch unindexed = cache.get(second).orElseThrow();
        unindexed.release();

        index.clearStream(17);
        assertEquals(1, index.size());
        assertTrue(index.get(17, 100, 104, 100).isEmpty());
        assertRanges(List.of(third), index.get(19, 100, 104, 100));
        index.clearStream(20);
        assertEquals(1, index.size());
        put(index, cache, 19, 104, 4);
        index.clearStream(19);
        assertEquals(0, index.size());

        for (int i = 0; i < 20; i++) {
            put(index, cache, i, 0, 1);
            assertTrue(index.size() <= 2);
        }
        assertTrue(index.get(0, 0, 1, 100).isEmpty());
        assertEquals(1, index.get(19, 0, 1, 100).size());
    }

    /**
     * An indexed range can miss after disk eviction; disabled indexing creates no metadata.
     */
    @Test
    public void testDiskMissAndDisabledIndex() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(80);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(8);
        FileCache.ReadRange first = put(index, cache, 17, 100, 4);
        put(index, cache, 17, 104, 4);
        put(index, cache, 17, 108, 4);
        assertRanges(List.of(first), index.get(17, 100, 104, 100));
        assertTrue(cache.get(first).isEmpty());
        assertEquals(3, index.size());

        StreamRecordBatchDiskIndex disabled = new StreamRecordBatchDiskIndex(0);
        put(disabled, cache, 17, 100, 4);
        assertTrue(disabled.get(17, 100, 104, 100).isEmpty());
        assertEquals(0, disabled.size());
    }

    /**
     * A bulk miss removes only evicted ranges from the index and metadata LRU.
     * Cached ranges and another stream remain available, and cleanup releases metadata capacity.
     */
    @Test
    public void testRemoveMissingRanges() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(80);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(4);
        FileCache.ReadRange first = put(index, cache, 17, 100, 4);
        FileCache.ReadRange second = put(index, cache, 17, 104, 4);
        FileCache.ReadRange third = put(index, cache, 17, 108, 4);
        FileCache.ReadRange otherStream = put(index, cache, 18, 100, 4);

        List<FileCache.ReadRange> selected = index.get(17, 100, 112, 100);
        assertRanges(List.of(first, second, third), selected);
        StreamRecordBatchDiskCache.BulkReadResult read = cache.get(selected);
        assertRanges(List.of(first, second), read.missingRanges());
        assertSame(selected.get(0), read.missingRanges().get(0));
        assertSame(selected.get(1), read.missingRanges().get(1));
        assertTrue(read.batches().isEmpty());

        index.remove(read.missingRanges());
        assertEquals(2, index.size());
        assertTrue(index.get(17, 100, 112, 100).isEmpty());
        assertRanges(List.of(third), index.get(17, 108, 112, 100));

        put(index, cache, 19, 100, 4);
        put(index, cache, 20, 100, 4);
        assertEquals(4, index.size());
        assertRanges(List.of(otherStream), index.get(18, 100, 104, 100));
        assertRanges(List.of(third), index.get(17, 108, 112, 100));
    }

    /**
     * Admission can replace an evicted disk copy between a read miss and metadata cleanup.
     * Cleaning the old missing range preserves that replacement and all unreported ranges.
     */
    @Test
    public void testCleanupPreservesReplacement() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(80);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(4);
        FileCache.ReadRange first = put(index, cache, 17, 100, 4);
        FileCache.ReadRange second = put(index, cache, 17, 104, 4);
        FileCache.ReadRange otherStream = put(index, cache, 18, 100, 4);

        StreamRecordBatchDiskCache.BulkReadResult read = cache.get(index.get(17, 100, 108, 100));
        assertRanges(List.of(first), read.missingRanges());
        assertTrue(read.batches().isEmpty());
        FileCache.ReadRange replacement = put(index, cache, 17, 100, 4);

        index.remove(read.missingRanges());
        assertEquals(3, index.size());
        assertRanges(List.of(replacement, second), index.get(17, 100, 108, 100));
        assertRanges(List.of(otherStream), index.get(18, 100, 104, 100));
        StreamRecordBatch batch = cache.get(replacement).orElseThrow();
        batch.release();
    }

    /**
     * Cleanup accepts only the current index entry, even when another entry describes identical disk bytes.
     * Plain byte ranges, foreign index entries and replaced entries cannot remove current metadata.
     */
    @Test
    public void testCleanupRequiresCurrentEntry() throws IOException {
        StreamRecordBatchDiskCache cache = diskCache(1024);
        StreamRecordBatchDiskIndex index = new StreamRecordBatchDiskIndex(4);
        StreamRecordBatchDiskIndex other = new StreamRecordBatchDiskIndex(4);
        FileCache.ReadRange bytes = put(index, cache, 17, 100, 4);
        FileCache.ReadRange old = index.get(17, 100, 104, 100).get(0);
        StreamRecordBatch batch = cache.get(bytes).orElseThrow();
        FileCache.ReadRange current;
        try {
            other.put(batch, bytes);
            index.put(batch, bytes);
            current = index.get(17, 100, 104, 100).get(0);
        } finally {
            batch.release();
        }

        FileCache.ReadRange foreign = other.get(17, 100, 104, 100).get(0);
        index.remove(List.of(bytes, foreign, old));
        index.remove(List.of());
        assertEquals(1, index.size());
        assertSame(current, index.get(17, 100, 104, 100).get(0));

        index.remove(List.of(current, current));
        assertEquals(0, index.size());
        assertEquals(1, other.size());
    }

    private void assertRanges(List<FileCache.ReadRange> expected, List<FileCache.ReadRange> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).cacheId(), actual.get(i).cacheId());
            assertEquals(expected.get(i).position(), actual.get(i).position());
            assertEquals(expected.get(i).length(), actual.get(i).length());
        }
    }

    private StreamRecordBatchDiskCache diskCache(int capacityBytes) throws IOException {
        return new StreamRecordBatchDiskCache(
            new FileCache(directory.resolve("cache").toString(), capacityBytes, 8, 16));
    }

    private FileCache.ReadRange put(StreamRecordBatchDiskIndex index, StreamRecordBatchDiskCache cache,
        long streamId, long baseOffset, int count) {
        return put(index, cache, streamId, baseOffset, count, 1);
    }

    private FileCache.ReadRange put(StreamRecordBatchDiskIndex index, StreamRecordBatchDiskCache cache,
        long streamId, long baseOffset, int count, int payloadBytes) {
        StreamRecordBatch batch = StreamRecordBatch.of(streamId, 2, baseOffset, count,
            Unpooled.wrappedBuffer(new byte[payloadBytes]), DefaultByteBufSupplier.INSTANCE);
        try {
            FileCache.ReadRange range = cache.put(batch);
            index.put(batch, range);
            assertEquals(1, batch.encoded().refCnt());
            return range;
        } finally {
            batch.release();
        }
    }
}
