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

package kafka.log.streamaspect;

import kafka.log.AbortedTxn;
import kafka.log.streamaspect.cache.FileCache;
import kafka.utils.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static scala.collection.JavaConverters.seqAsJavaList;

public class ElasticTransactionIndexTest {
    @Test
    public void testPositionSetCorrectlyWhenOpened() throws IOException {
        ElasticStreamSlice slice = new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(1), SliceRange.of(0, Offsets.NOOP_OFFSET));
        ElasticTransactionIndex index = new ElasticTransactionIndex(TestUtils.tempFile(), new IStreamSliceSupplier(slice),
                0, new FileCache(TestUtils.tempFile().getPath(), 0));

        List<AbortedTxn> abortedTxns = new LinkedList<>();
        abortedTxns.add(new AbortedTxn(0L, 0, 10, 11));
        abortedTxns.add(new AbortedTxn(1L, 5, 15, 12));
        abortedTxns.add(new AbortedTxn(2L, 18, 35, 25));
        abortedTxns.add(new AbortedTxn(3L, 32, 50, 40));
        abortedTxns.forEach(index::append);
        index.close();
        index = new ElasticTransactionIndex(TestUtils.tempFile(), new IStreamSliceSupplier(slice),
                0, new FileCache(TestUtils.tempFile().getPath(), 0));
        AbortedTxn anotherAbortTxn = new AbortedTxn(3L, 50, 60, 55);
        index.append(anotherAbortTxn);
        abortedTxns.add(anotherAbortTxn);
        List<AbortedTxn> rst = seqAsJavaList(index.allAbortedTxns());
        assertEquals(abortedTxns, rst);
    }

    @Test
    public void test_withFileCache() throws IOException {
        String indexFile = TestUtils.tempFile().getPath();
        String cacheFile = TestUtils.tempFile().getPath();

        ElasticStreamSlice slice = new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(1), SliceRange.of(0, Offsets.NOOP_OFFSET));
        ElasticTransactionIndex index = new ElasticTransactionIndex(new File(indexFile), new IStreamSliceSupplier(slice),
                0, new FileCache(cacheFile, 10 * 1024));

        List<AbortedTxn> abortedTxns = new LinkedList<>();
        abortedTxns.add(new AbortedTxn(0L, 0, 10, 11));
        abortedTxns.add(new AbortedTxn(1L, 5, 15, 12));
        abortedTxns.add(new AbortedTxn(2L, 18, 35, 25));
        abortedTxns.add(new AbortedTxn(3L, 32, 50, 40));
        abortedTxns.forEach(index::append);

        // get from write cache
        assertEquals(abortedTxns, seqAsJavaList(index.allAbortedTxns()));

        index = new ElasticTransactionIndex(new File(indexFile), new IStreamSliceSupplier(slice),
                0, new FileCache(cacheFile, 10 * 1024));
        // get from stream
        assertEquals(abortedTxns, seqAsJavaList(index.allAbortedTxns()));
        // get from read cache
        assertEquals(abortedTxns, seqAsJavaList(index.allAbortedTxns()));
    }


    static class IStreamSliceSupplier implements StreamSliceSupplier {
        final ElasticStreamSlice slice;

        public IStreamSliceSupplier(ElasticStreamSlice slice) {
            this.slice = slice;
        }

        @Override
        public ElasticStreamSlice get() throws IOException {
            return slice;
        }

        @Override
        public ElasticStreamSlice reset() throws IOException {
            return new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(2), SliceRange.of(0, Offsets.NOOP_OFFSET));
        }
    }

}
