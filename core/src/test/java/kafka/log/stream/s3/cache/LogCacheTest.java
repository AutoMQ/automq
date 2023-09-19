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

package kafka.log.stream.s3.cache;

import kafka.log.stream.s3.TestUtils;
import kafka.log.stream.s3.model.StreamRecordBatch;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LogCacheTest {


    @Test
    public void testPutGet() {
        LogCache logCache = new LogCache(1024 * 1024);

        logCache.put(new StreamRecordBatch(233L, 0L, 10L, 1, TestUtils.random(20)));
        logCache.put(new StreamRecordBatch(233L, 0L, 11L, 2, TestUtils.random(20)));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 13L, 2, TestUtils.random(20)));

        logCache.archiveCurrentBlock();
        logCache.put(new StreamRecordBatch(233L, 0L, 20L, 1, TestUtils.random(20)));
        logCache.put(new StreamRecordBatch(233L, 0L, 21L, 1, TestUtils.random(20)));

        List<StreamRecordBatch> records = logCache.get(233L, 10L, 21L, 1000);
        assertEquals(1, records.size());
        assertEquals(20L, records.get(0).getBaseOffset());

        records = logCache.get(233L, 10L, 15L, 1000);
        assertEquals(3, records.size());
        assertEquals(10L, records.get(0).getBaseOffset());

        records = logCache.get(233L, 10L, 16L, 1000);
        assertEquals(0, records.size());
    }

}
