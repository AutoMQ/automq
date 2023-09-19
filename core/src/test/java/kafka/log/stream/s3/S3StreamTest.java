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

package kafka.log.stream.s3;

import kafka.log.stream.api.ElasticStreamClientException;
import kafka.log.stream.api.FetchResult;
import kafka.log.stream.s3.cache.ReadDataBlock;
import kafka.log.stream.s3.model.StreamRecordBatch;
import kafka.log.stream.s3.streams.StreamManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
public class S3StreamTest {
    Storage storage;
    StreamManager streamManager;
    S3Stream stream;

    @BeforeEach
    public void setup() {
        storage = mock(Storage.class);
        streamManager = mock(StreamManager.class);
        StreamObjectsCompactionTask.Builder builder = new StreamObjectsCompactionTask.Builder(null, null);
        stream = new S3Stream(233, 1, 100, 233, storage, streamManager, builder, v -> null);
    }

    @Test
    public void testFetch() throws Throwable {
        stream.confirmOffset.set(120L);
        when(storage.read(eq(233L), eq(110L), eq(120L), eq(100)))
                .thenReturn(CompletableFuture.completedFuture(newReadDataBlock(110, 115, 110)));
        FetchResult rst = stream.fetch(110, 120, 100).get(1, TimeUnit.SECONDS);
        assertEquals(1, rst.recordBatchList().size());
        assertEquals(110, rst.recordBatchList().get(0).baseOffset());
        assertEquals(115, rst.recordBatchList().get(0).lastOffset());

        // TODO: add fetch from WAL cache

        boolean isException = false;
        try {
            stream.fetch(120, 140, 100).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ElasticStreamClientException) {
                isException = true;
            }
        }
        assertTrue(isException);
    }

    ReadDataBlock newReadDataBlock(long start, long end, int size) {
        StreamRecordBatch record = new StreamRecordBatch(0, 0, start, (int) (end - start), TestUtils.random(size));
        return new ReadDataBlock(List.of(record));
    }
}
