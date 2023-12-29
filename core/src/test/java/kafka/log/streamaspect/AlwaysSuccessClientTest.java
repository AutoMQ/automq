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

import com.automq.stream.RecordBatchWithContextWrapper;
import com.automq.stream.api.AppendResult;
import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static kafka.log.streamaspect.AlwaysSuccessClient.HALT_ERROR_CODES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class AlwaysSuccessClientTest {
    private AlwaysSuccessClient client;

    @AfterEach
    public void teardown() {
        client.shutdown();
    }

    @Test
    public void basicAppendAndFetch() throws ExecutionException, InterruptedException {
        client = new AlwaysSuccessClient(new MemoryClient());
        client.start();
        Stream stream = client
                .streamClient()
                .createAndOpenStream(CreateStreamOptions.builder().epoch(0).replicaCount(1).build())
                .get();
        List<byte[]> payloads = List.of("hello".getBytes(), "world".getBytes());
        CompletableFuture.allOf(
                payloads
                        .stream()
                        .map(payload -> stream.append(RawPayloadRecordBatch.of(ByteBuffer.wrap(payload)))).toArray(CompletableFuture[]::new)
        ).get();

        FetchResult fetched = stream.fetch(0, 100, 1000).get();
        checkAppendAndFetch(payloads, fetched);

        stream.destroy();
    }

    @Test
    public void testOpenStream() {
        MemoryClientWithDelay memoryClientWithDelay = new MemoryClientWithDelay();
        ((MemoryClientWithDelay.StreamClientImpl) memoryClientWithDelay.streamClient()).setHaltOpeningStream(true);
        client = new AlwaysSuccessClient(memoryClientWithDelay);
        client.start();

        AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        openStream(1)
                .exceptionally(e -> {
                    assertEquals(IOException.class, e.getClass());
                    exceptionThrown.set(true);
                    return null;
                })
                .join();

        assertTrue(exceptionThrown.get(), "should throw IOException");
    }

//    @Test
    public void testStreamOperationHalt() {
        // FIXME: fix the fetch halt
        MemoryClientWithDelay memoryClientWithDelay = new MemoryClientWithDelay();
        ((MemoryClientWithDelay.StreamClientImpl) memoryClientWithDelay.streamClient()).setExceptionHint(ExceptionHint.HALT_EXCEPTION);
        client = new AlwaysSuccessClient(memoryClientWithDelay);
        client.start();

        Stream stream = client
                .streamClient()
                .createAndOpenStream(CreateStreamOptions.builder().epoch(0).replicaCount(1).build())
                .join();

        AtomicInteger exceptionCount = new AtomicInteger(0);
        stream
                .append(RawPayloadRecordBatch.of(ByteBuffer.wrap("hello".getBytes())))
                .exceptionally(e -> {
                    assertEquals(IOException.class, e.getClass());
                    exceptionCount.incrementAndGet();
                    return null;
                })
                .join();
        stream.fetch(0, 100, 1000)
                .exceptionally(e -> {
                    assertEquals(IOException.class, e.getClass());
                    exceptionCount.incrementAndGet();
                    return null;
                })
                .join();
        stream.trim(0)
                .exceptionally(e -> {
                    assertEquals(IOException.class, e.getClass());
                    exceptionCount.incrementAndGet();
                    return null;
                })
                .join();
        stream.close()
                .exceptionally(e -> {
                    assertEquals(IOException.class, e.getClass());
                    exceptionCount.incrementAndGet();
                    return null;
                })
                .join();
        assertEquals(4, exceptionCount.get(), "should throw IOException 4 times");
        stream.destroy();
    }

//    @Test
    public void testNormalExceptionHandling() {
        // FIXME: fix the normal exception retry
        MemoryClientWithDelay memoryClientWithDelay = new MemoryClientWithDelay();
        ((MemoryClientWithDelay.StreamClientImpl) memoryClientWithDelay.streamClient()).setExceptionHint(ExceptionHint.OTHER_EXCEPTION);
        client = new AlwaysSuccessClient(memoryClientWithDelay);
        client.start();

        Stream stream = openStream(1).join();
        stream.append(RawPayloadRecordBatch.of(ByteBuffer.wrap("hello".getBytes()))).join();
        stream.destroy();

        stream = openStream(1).join();

        stream.fetch(0, 100, 1000).join();

        stream.destroy();

        stream = openStream(1).join();
        stream.trim(0).join();
        stream.destroy();

        stream = openStream(1).join();
        stream.close().join();
        stream.destroy();
    }

    private CompletableFuture<Stream> openStream(long streamId) {
        return client
                .streamClient()
                .openStream(streamId, OpenStreamOptions.builder().epoch(1).build());
    }

    private void checkAppendAndFetch(List<byte[]> rawPayloads, FetchResult fetched) {
        for (int i = 0; i < fetched.recordBatchList().size(); i++) {
            assertEquals(rawPayloads.get(i), fetched.recordBatchList().get(i).rawPayload().array());
        }
    }

    static final class MemoryClientWithDelay extends MemoryClient {
        private final StreamClientImpl streamClient = new StreamClientImpl();

        /**
         * Set the additional fetching delay
         *
         * @param delayMillis
         */
        public void setDelayMillis(long delayMillis) {
            streamClient.setDelayMillis(delayMillis);
        }

        @Override
        public StreamClient streamClient() {
            return streamClient;
        }

        static class StreamClientImpl implements StreamClient {
            private final AtomicLong streamIdAlloc = new AtomicLong();
            /**
             * The additional fetching delay
             */
            private long delayMillis = 0;
            /**
             * If ture, open stream with ElasticStreamClientException, whose code is within HALT_ERROR_CODES.
             */
            private boolean haltOpeningStream = false;
            /**
             * If not OK, throw Exceptions when operating on the stream.
             */
            private ExceptionHint exceptionHint = ExceptionHint.OK;

            public StreamClientImpl() {
            }

            public void setDelayMillis(long delayMillis) {
                this.delayMillis = delayMillis;
            }

            public void setExceptionHint(ExceptionHint exceptionHint) {
                this.exceptionHint = exceptionHint;
            }

            public void setHaltOpeningStream(boolean haltOpeningStream) {
                this.haltOpeningStream = haltOpeningStream;
            }

            @Override
            public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions createStreamOptions) {
                return CompletableFuture.completedFuture(new TestStreamImpl(streamIdAlloc.incrementAndGet(), delayMillis, exceptionHint));
            }

            @Override
            public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions openStreamOptions) {
                if (haltOpeningStream) {
                    return CompletableFuture.failedFuture(new StreamClientException(HALT_ERROR_CODES.iterator().next(), "halt opening stream"));
                }
                return CompletableFuture.completedFuture(new TestStreamImpl(streamId, delayMillis, exceptionHint));
            }

            @Override
            public Optional<Stream> getStream(long streamId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void shutdown() {

            }
        }

        static class TestStreamImpl implements Stream {
            private final AtomicLong nextOffsetAlloc = new AtomicLong();
            private NavigableMap<Long, RecordBatchWithContext> recordMap = new ConcurrentSkipListMap<>();
            private final long streamId;
            /**
             * The additional fetching delay
             */
            private final long delayMillis;
            /**
             * Hint what exception to throw.
             */
            private volatile ExceptionHint exceptionHint;

            public TestStreamImpl(long streamId, long delayMillis, ExceptionHint exceptionHint) {
                this.streamId = streamId;
                this.delayMillis = delayMillis;
                this.exceptionHint = exceptionHint;
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
            public long confirmOffset() {
                return nextOffset();
            }

            @Override
            public long nextOffset() {
                return nextOffsetAlloc.get();
            }

            @Override
            public synchronized CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
                Exception exception = exceptionHint.generateException();
                if (exception != null) {
                    exceptionHint = exceptionHint.moveToNext();
                    return CompletableFuture.failedFuture(exception);
                }
                long baseOffset = nextOffsetAlloc.getAndAdd(recordBatch.count());
                recordMap.put(baseOffset, new RecordBatchWithContextWrapper(recordBatch, baseOffset));
                return CompletableFuture.completedFuture(() -> baseOffset);
            }

            @Override
            public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxSizeHint) {
                Exception exception = exceptionHint.generateException();
                if (exception != null) {
                    exceptionHint = exceptionHint.moveToNext();
                    return CompletableFuture.failedFuture(exception);
                }
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
                Exception exception = exceptionHint.generateException();
                if (exception != null) {
                    exceptionHint = exceptionHint.moveToNext();
                    return CompletableFuture.failedFuture(exception);
                }
                recordMap = new ConcurrentSkipListMap<>(recordMap.tailMap(newStartOffset));
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> close() {
                Exception exception = exceptionHint.generateException();
                if (exception != null) {
                    exceptionHint = exceptionHint.moveToNext();
                    return CompletableFuture.failedFuture(exception);
                }
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> destroy() {
                recordMap.clear();
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    static enum ExceptionHint {
        HALT_EXCEPTION,
        OTHER_EXCEPTION,
        OK;

        private static final List<Exception> OTHER_EXCEPTION_LIST = List.of(
                new IOException("io exception"),
                new RuntimeException("runtime exception"),
                new StreamClientException(-1, "other exception")
        );

        public Exception generateException() {
            switch (this) {
                case HALT_EXCEPTION:
                    return new StreamClientException(HALT_ERROR_CODES.iterator().next(), "halt operation");
                case OTHER_EXCEPTION:
                    return OTHER_EXCEPTION_LIST.get(new Random().nextInt(OTHER_EXCEPTION_LIST.size()));
                case OK:
                    return null;
                default:
                    throw new IllegalStateException("unknown exception hint");
            }
        }

        public ExceptionHint moveToNext() {
            switch (this) {
                case HALT_EXCEPTION:
                    return HALT_EXCEPTION;
                // move to OK to ensure break out
                case OTHER_EXCEPTION:
                case OK:
                    return OK;
                default:
                    throw new IllegalStateException("unknown exception hint");
            }
        }
    }
}
