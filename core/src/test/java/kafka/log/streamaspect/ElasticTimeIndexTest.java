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

import kafka.log.TimestampOffset;
import kafka.log.streamaspect.cache.FileCache;
import kafka.utils.TestUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
public class ElasticTimeIndexTest {

    int maxEntries = 30;
    long baseOffset = 45L;

    @Test
    public void testLookUp() throws IOException {
        for (boolean withCache : List.of(false, true)) {
            FileCache cache = withCache ? new FileCache(TestUtils.tempFile().getPath(), 10 * 1024) : new FileCache(TestUtils.tempFile().getPath(), 0);
            ElasticStreamSlice slice = new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(1), SliceRange.of(0, Offsets.NOOP_OFFSET));
            ElasticTimeIndex idx = new ElasticTimeIndex(TestUtils.tempFile(), new ElasticTransactionIndexTest.IStreamSliceSupplier(slice),
                    baseOffset, maxEntries * 12, TimestampOffset.Unknown(), cache);

            assertEquals(new TimestampOffset(-1, baseOffset), idx.lookup(100L));

            appendEntries(idx, maxEntries);

            // look for timestamp smaller than the earliest entry
            assertEquals(new TimestampOffset(-1L, baseOffset), idx.lookup(9));
            // look for timestamp in the middle of two entries.
            assertEquals(new TimestampOffset(20L, 65L), idx.lookup(25));
            // look for timestamp same as the one in the entry
            assertEquals(new TimestampOffset(30L, 75L), idx.lookup(30));
        }

    }

    void appendEntries(ElasticTimeIndex idx, int numEntries) {
        for (int i = 1; i < numEntries; i++) {
            idx.maybeAppend(i * 10L, i * 10L + baseOffset, false);
        }
    }

}
