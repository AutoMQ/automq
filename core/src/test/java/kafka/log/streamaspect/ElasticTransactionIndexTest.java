/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package kafka.log.streamaspect;

import kafka.log.streamaspect.cache.FileCache;
import kafka.utils.TestUtils;

import org.apache.kafka.storage.internals.log.AbortedTxn;

import com.automq.stream.api.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class ElasticTransactionIndexTest {
    @Test
    public void testPositionSetCorrectlyWhenOpened() throws IOException {
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(1), SliceRange.of(0, Offsets.NOOP_OFFSET));
        ElasticTransactionIndex index = new ElasticTransactionIndex(0, TestUtils.tempFile(), new IStreamSliceSupplier(slice),
            new FileCache(TestUtils.tempFile().getPath(), 0));

        List<AbortedTxn> abortedTxns = new LinkedList<>();
        abortedTxns.add(new AbortedTxn(0L, 0, 10, 11));
        abortedTxns.add(new AbortedTxn(1L, 5, 15, 12));
        abortedTxns.add(new AbortedTxn(2L, 18, 35, 25));
        abortedTxns.add(new AbortedTxn(3L, 32, 50, 40));
        for (AbortedTxn abortedTxn : abortedTxns) {
            index.append(abortedTxn);
        }
        index.close();
        index = new ElasticTransactionIndex(0, TestUtils.tempFile(), new IStreamSliceSupplier(slice),
            new FileCache(TestUtils.tempFile().getPath(), 0));
        AbortedTxn anotherAbortTxn = new AbortedTxn(3L, 50, 60, 55);
        index.append(anotherAbortTxn);
        abortedTxns.add(anotherAbortTxn);
        List<AbortedTxn> rst = index.allAbortedTxns();
        assertEquals(abortedTxns, rst);
    }

    @Test
    public void test_withFileCache() throws IOException {
        String indexFile = TestUtils.tempFile().getPath();
        String cacheFile = TestUtils.tempFile().getPath();

        ElasticStreamSlice slice = new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(1), SliceRange.of(0, Offsets.NOOP_OFFSET));
        ElasticTransactionIndex index = new ElasticTransactionIndex(0, new File(indexFile), new IStreamSliceSupplier(slice),
            new FileCache(cacheFile, 10 * 1024));

        List<AbortedTxn> abortedTxns = new LinkedList<>();
        abortedTxns.add(new AbortedTxn(0L, 0, 10, 11));
        abortedTxns.add(new AbortedTxn(1L, 5, 15, 12));
        abortedTxns.add(new AbortedTxn(2L, 18, 35, 25));
        abortedTxns.add(new AbortedTxn(3L, 32, 50, 40));
        for (AbortedTxn abortedTxn : abortedTxns) {
            index.append(abortedTxn);
        }

        // get from write cache
        assertEquals(abortedTxns, index.allAbortedTxns());

        index = new ElasticTransactionIndex(0, new File(indexFile), new IStreamSliceSupplier(slice),
            new FileCache(cacheFile, 10 * 1024));
        // get from stream
        assertEquals(abortedTxns, index.allAbortedTxns());
        // get from read cache
        assertEquals(abortedTxns, index.allAbortedTxns());
    }

    @Test
    public void testUniqueFileCache() throws IOException {
        String indexFile = TestUtils.tempFile().getPath();
        String cacheFile = TestUtils.tempFile().getPath();

        FileCache fileCache = new FileCache(cacheFile, 10 * 1024);
        Stream stream = new MemoryClient.StreamImpl(1);
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(stream, SliceRange.of(0, Offsets.NOOP_OFFSET));
        ElasticTransactionIndex index = new ElasticTransactionIndex(0, new File(indexFile), new IStreamSliceSupplier(slice),
            fileCache);
        Assertions.assertEquals(0, slice.nextOffset());

        List<AbortedTxn> abortedTxns = new LinkedList<>();
        abortedTxns.add(new AbortedTxn(0L, 0, 10, 11));
        abortedTxns.add(new AbortedTxn(1L, 5, 15, 12));
        abortedTxns.add(new AbortedTxn(2L, 18, 35, 25));
        abortedTxns.add(new AbortedTxn(3L, 32, 50, 40));
        for (AbortedTxn abortedTxn : abortedTxns) {
            index.append(abortedTxn);
        }
        Assertions.assertEquals((long) abortedTxns.size() * AbortedTxn.TOTAL_SIZE, slice.nextOffset());
        Assertions.assertEquals(stream.nextOffset(), slice.nextOffset());

        // get from write cache
        assertEquals(abortedTxns, index.allAbortedTxns());

        slice = new DefaultElasticStreamSlice(stream, SliceRange.of(stream.nextOffset(), Offsets.NOOP_OFFSET));
        index = new ElasticTransactionIndex(0, new File(indexFile), new IStreamSliceSupplier(slice),
            fileCache);
        Assertions.assertEquals(0, slice.nextOffset());

        abortedTxns = new LinkedList<>();
        abortedTxns.add(new AbortedTxn(5L, 0, 10, 11));
        abortedTxns.add(new AbortedTxn(6L, 5, 15, 12));
        abortedTxns.add(new AbortedTxn(7L, 18, 35, 25));
        for (AbortedTxn abortedTxn : abortedTxns) {
            index.append(abortedTxn);
        }

        // get from read cache
        assertEquals(abortedTxns, index.allAbortedTxns());
    }
}
