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

package kafka.log.es;

import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;
import com.automq.elasticstream.client.api.StreamClient;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.errors.es.SlowFetchHintException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("esUnit")
class AlwaysSuccessClientTest {
    private static final long SLOW_FETCH_TIMEOUT_MILLIS = AlwaysSuccessClient.SLOW_FETCH_TIMEOUT_MILLIS;

    @BeforeEach
    public void setup() {
        SeparateSlowAndQuickFetchHint.mark();
    }

    @AfterEach
    public void teardown() {
        SeparateSlowAndQuickFetchHint.reset();
    }

    @Test
    public void basicAppendAndFetch() throws ExecutionException, InterruptedException {
        AlwaysSuccessClient client = new AlwaysSuccessClient(new MemoryClient());
        Stream stream = client
            .streamClient()
            .createAndOpenStream(CreateStreamOptions.newBuilder().epoch(0).replicaCount(1).build())
            .get();
        List<byte[]> payloads = List.of("hello".getBytes(), "world".getBytes());
        payloads.forEach(payload -> stream.append(RawPayloadRecordBatch.of(ByteBuffer.wrap(payload))));

        FetchResult fetched = stream.fetch(0, 100, 1000).get();
        checkAppendAndFetch(payloads, fetched);

        stream.destroy();
    }

    @Test
    public void testQuickFetch() throws ExecutionException, InterruptedException {
        MemoryClientWithDelay memoryClientWithDelay = new MemoryClientWithDelay();
        AlwaysSuccessClient client = new AlwaysSuccessClient(memoryClientWithDelay);
        List<Long> quickFetchDelayMillisList = List.of(1L, SLOW_FETCH_TIMEOUT_MILLIS / 2);
        List<byte[]> payloads = List.of("hello".getBytes(), "world".getBytes());

        // test quick fetch
        for (Long delay : quickFetchDelayMillisList) {
            memoryClientWithDelay.setDelayMillis(delay);
            Stream stream = client
                .streamClient()
                .createAndOpenStream(CreateStreamOptions.newBuilder().epoch(0).replicaCount(1).build())
                .get();
            payloads.forEach(payload -> stream.append(RawPayloadRecordBatch.of(ByteBuffer.wrap(payload))));
            FetchResult fetched = stream.fetch(0, 100, 1000)
                .orTimeout(delay + 3, TimeUnit.MILLISECONDS)
                .get();
            checkAppendAndFetch(payloads, fetched);
            stream.destroy();
        }
    }

    @Test
    public void testSlowFetch() throws ExecutionException, InterruptedException {
        MemoryClientWithDelay memoryClientWithDelay = new MemoryClientWithDelay();
        AlwaysSuccessClient client = new AlwaysSuccessClient(memoryClientWithDelay);
        List<byte[]> payloads = List.of("hello".getBytes(), "world".getBytes());

        long slowFetchDelay = SLOW_FETCH_TIMEOUT_MILLIS + 1;
        memoryClientWithDelay.setDelayMillis(slowFetchDelay);
        Stream stream = client
            .streamClient()
            .createAndOpenStream(CreateStreamOptions.newBuilder().epoch(0).replicaCount(1).build())
            .get();
        payloads.forEach(payload -> stream.append(RawPayloadRecordBatch.of(ByteBuffer.wrap(payload))));

        FetchResult fetched = null;
        try {
            fetched = stream.fetch(0, 100, 1000)
                .orTimeout(slowFetchDelay + 3, TimeUnit.MILLISECONDS)
                .get();
            checkAppendAndFetch(payloads, fetched);
        } catch (ExecutionException e) {
            // should throw SlowFetchHintException after SLOW_FETCH_TIMEOUT_MILLIS ms
            assertEquals(SlowFetchHintException.class, e.getCause().getClass());
            SeparateSlowAndQuickFetchHint.reset();
            // It should reuse the fetching future above, therefore only (SLOW_FETCH_TIMEOUT_MILLIS / 2) ms is tolerable.
            fetched = stream.fetch(0, 100, 1000)
                .orTimeout(SLOW_FETCH_TIMEOUT_MILLIS / 2, TimeUnit.MILLISECONDS)
                .get();
        }
        checkAppendAndFetch(payloads, fetched);
        stream.destroy();
    }

    private void checkAppendAndFetch(List<byte[]> rawPayloads, FetchResult fetched) {
        for (int i = 0; i < fetched.recordBatchList().size(); i++) {
            assertEquals(rawPayloads.get(i), fetched.recordBatchList().get(i).rawPayload().array());
        }
    }

    static final class MemoryClientWithDelay extends MemoryClient {
        private final StreamClientImpl streamClient = new StreamClientImpl();
        public void setDelayMillis(long delayMillis) {
            streamClient.setDelayMillis(delayMillis);
        }

        @Override
        public StreamClient streamClient() {
            return streamClient;
        }

        static class StreamClientImpl implements StreamClient {
            private final AtomicLong streamIdAlloc = new AtomicLong();
            private long delayMillis = 0;

            public StreamClientImpl() {
            }

            public void setDelayMillis(long delayMillis) {
                this.delayMillis = delayMillis;
            }

            @Override
            public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions createStreamOptions) {
                return CompletableFuture.completedFuture(new TestStreamImpl(streamIdAlloc.incrementAndGet(), delayMillis));
            }

            @Override
            public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
                return CompletableFuture.completedFuture(new TestStreamImpl(streamId, delayMillis));
            }
        }

        static class TestStreamImpl implements Stream {
            private final AtomicLong nextOffsetAlloc = new AtomicLong();
            private NavigableMap<Long, RecordBatchWithContext> recordMap = new ConcurrentSkipListMap<>();
            private final long streamId;
            /**
             * The additional fetching delay
             */
            private long delayMillis;

            public TestStreamImpl(long streamId, long delayMillis) {
                this.streamId = streamId;
                this.delayMillis = delayMillis;
            }

            @Override
            public long streamId() {
                return streamId;
            }

            @Override
            public long startOffset() {
                return 0;
            }

            @Override
            public long nextOffset() {
                return nextOffsetAlloc.get();
            }

            @Override
            public synchronized CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
                long baseOffset = nextOffsetAlloc.getAndAdd(recordBatch.count());
                recordMap.put(baseOffset, new RecordBatchWithContextWrapper(recordBatch, baseOffset));
                return CompletableFuture.completedFuture(() -> baseOffset);
            }

            @Override
            public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxSizeHint) {
                Long floorKey = recordMap.floorKey(startOffset);
                if (floorKey == null) {
                    return CompletableFuture.completedFuture(ArrayList::new);
                }
                List<RecordBatchWithContext> records = new ArrayList<>(recordMap.subMap(floorKey, endOffset).values());
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return () -> records;
                });
            }

            @Override
            public CompletableFuture<Void> trim(long newStartOffset) {
                recordMap = new ConcurrentSkipListMap<>(recordMap.tailMap(newStartOffset));
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> close() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> destroy() {
                recordMap.clear();
                return CompletableFuture.completedFuture(null);
            }
        }
    }
}