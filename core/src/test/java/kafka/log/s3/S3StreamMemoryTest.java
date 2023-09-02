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

package kafka.log.s3;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.automq.elasticstream.client.api.AppendResult;
import com.automq.elasticstream.client.api.CreateStreamOptions;
import com.automq.elasticstream.client.api.FetchResult;
import com.automq.elasticstream.client.api.OpenStreamOptions;
import com.automq.elasticstream.client.api.RecordBatch;
import com.automq.elasticstream.client.api.RecordBatchWithContext;
import com.automq.elasticstream.client.api.Stream;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import kafka.log.s3.cache.DefaultS3BlockCache;
import kafka.log.s3.cache.S3BlockCache;
import kafka.log.s3.memory.MemoryMetadataManager;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.MemoryS3Operator;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.streams.StreamManager;
import kafka.log.s3.wal.MemoryWriteAheadLog;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("S3Unit")
public class S3StreamMemoryTest {

    static class MockRecordBatch implements RecordBatch {

        ByteBuffer byteBuffer;

        int count;

        public MockRecordBatch(String payload, int count) {
            this.byteBuffer = ByteBuffer.wrap(payload.getBytes());
            this.count = count;
        }

        @Override
        public int count() {
            return count;
        }

        @Override
        public long baseTimestamp() {
            return 0;
        }

        @Override
        public Map<String, String> properties() {
            return Collections.emptyMap();
        }

        @Override
        public ByteBuffer rawPayload() {
            return this.byteBuffer.duplicate();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(S3StreamMemoryTest.class);
    MemoryMetadataManager manager;
    Storage storage;
    S3BlockCache blockCache;
    S3Operator operator;
    StreamManager streamManager;
    ObjectManager objectManager;
    S3StreamClient streamClient;
    private final static long MAX_APPENDED_OFFSET = 200;

    Random random = new Random();

    @BeforeEach
    public void setUp() {
        manager = new MemoryMetadataManager();
        manager.start();
        streamManager = manager;
        objectManager = manager;
        operator = new MemoryS3Operator();
        blockCache = new DefaultS3BlockCache(objectManager, operator);
        storage = new S3Storage(KafkaConfig.fromProps(TestUtils.defaultBrokerConfig()), new MemoryWriteAheadLog(), objectManager, blockCache, operator);
        streamClient = new S3StreamClient(streamManager, storage);
    }

    @Test
    public void testOpenAndClose() throws Exception {
        CreateStreamOptions options = CreateStreamOptions.newBuilder().epoch(0L).build();
        Stream stream = this.streamClient.createAndOpenStream(options).get();
        long streamId = stream.streamId();
        assertNotNull(stream);
        // duplicate open
        stream = this.streamClient.openStream(streamId, OpenStreamOptions.newBuilder().epoch(0L).build()).get();
        assertNotNull(stream);
        // open with new epoch but current epoch is not closed
        assertThrows(ExecutionException.class,
                () -> this.streamClient.openStream(streamId, OpenStreamOptions.newBuilder().epoch(1L).build()).get());
        stream.close().get();
        // duplicate close
        stream.close().get();
        // reopen with stale epoch
        assertThrows(ExecutionException.class,
                () -> this.streamClient.openStream(streamId, OpenStreamOptions.newBuilder().epoch(0L).build()).get());
        // reopen with new epoch
        Stream newStream = this.streamClient.openStream(streamId, OpenStreamOptions.newBuilder().epoch(1L).build()).get();
        assertEquals(streamId, newStream.streamId());
        // close with stale epoch
        final Stream oldStream = stream;
        assertThrows(ExecutionException.class, () -> oldStream.close().get());
    }

    @Test
    public void testFetch() throws Exception {
        CreateStreamOptions options = CreateStreamOptions.newBuilder().epoch(1L).build();
        Stream stream = this.streamClient.createAndOpenStream(options).get();
        RecordBatch recordBatch = new MockRecordBatch("hello", 1);
        CompletableFuture<AppendResult> append0 = stream.append(recordBatch);
        RecordBatch recordBatch1 = new MockRecordBatch("world", 1);
        CompletableFuture<AppendResult> append1 = stream.append(recordBatch1);
        CompletableFuture.allOf(append0, append1).get();
        // fetch
        FetchResult result0 = stream.fetch(0, 1, 100).get();
        assertEquals(1, result0.recordBatchList().size());
        RecordBatchWithContext record0 = result0.recordBatchList().get(0);
        assertEquals(0, record0.baseOffset());
        assertEquals(1, record0.lastOffset());
        assertEquals("hello", new String(buf2array(record0.rawPayload())));
        FetchResult result1 = stream.fetch(1, 2, 100).get();
        assertEquals(1, result1.recordBatchList().size());
        RecordBatchWithContext record1 = result1.recordBatchList().get(0);
        assertEquals(1, record1.baseOffset());
        assertEquals(2, record1.lastOffset());
        assertEquals("world", new String(buf2array(record1.rawPayload())));
        // fetch all
        FetchResult result = stream.fetch(0, 2, 100000).get();
        assertEquals(2, result.recordBatchList().size());
        RecordBatchWithContext record = result.recordBatchList().get(0);
        assertEquals("hello", new String(buf2array(record.rawPayload())));
        RecordBatchWithContext record2 = result.recordBatchList().get(1);
        assertEquals("world", new String(buf2array(record2.rawPayload())));
    }

    @Test
    public void testPressure() throws Exception {
        CreateStreamOptions options = CreateStreamOptions.newBuilder().epoch(1L).build();
        Stream stream0 = this.streamClient.createAndOpenStream(options).get();
        Stream stream1 = this.streamClient.createAndOpenStream(options).get();
        Stream stream2 = this.streamClient.createAndOpenStream(options).get();
        List<Stream> streams = List.of(stream0, stream1, stream2);
        CountDownLatch latch = new CountDownLatch(1 * 3 + 5 * 3);
        CyclicBarrier barrier = new CyclicBarrier(1 * 3 + 5 * 3);
        for (int i = 0; i < 3; i++) {
            AtomicLong appendedOffset = new AtomicLong(-1);
            final Stream stream = streams.get(i);
            new Thread(() -> {
                Producer producer = new Producer(stream, latch, appendedOffset);
                try {
                    barrier.await();
                    producer.run();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

            }).start();
            for (int j = 0; j < 5; j++) {
                final int id = j;
                new Thread(() -> {
                    Consumer consumer = new Consumer(id, stream, latch, appendedOffset);
                    try {
                        barrier.await();
                        consumer.run();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }

                }).start();
            }
        }
        latch.await();
    }

    private static byte[] buf2array(ByteBuffer buffer) {
        byte[] array = new byte[buffer.remaining()];
        buffer.get(array);
        return array;
    }

    static class Producer implements Runnable {

        private long nextAppendOffset = 0;
        private Stream stream;
        private CountDownLatch latch;
        private AtomicLong appendedOffset;
        private Random random = new Random();
        private volatile boolean start = true;

        public Producer(Stream stream, CountDownLatch latch, AtomicLong appendedOffset) {
            this.stream = stream;
            this.latch = latch;
            this.appendedOffset = appendedOffset;
        }


        @Override
        public void run() {
            while (start) {
                try {
                    append();
                } catch (Exception e) {
                    LOGGER.error("Error in producer", e);
                }
            }
            latch.countDown();
        }

        public void append() throws InterruptedException {
            if (nextAppendOffset > MAX_APPENDED_OFFSET) {
                start = false;
                return;
            }
            MockRecordBatch recordBatch = new MockRecordBatch("hello[" + stream.streamId() + "][" + nextAppendOffset++ + "]", 1);
            stream.append(recordBatch).whenCompleteAsync((result, error) -> {
                assertNull(error);
                LOGGER.info("[Producer-{}]: produce: {}", stream.streamId(), result.baseOffset());
                this.appendedOffset.incrementAndGet();
            });
            Thread.sleep(random.nextInt(30));
        }
    }

    static class Consumer implements Runnable {

        private long consumeOffset = 0;
        private int id;
        private Stream stream;
        private CountDownLatch latch;
        private AtomicLong appendedOffset;
        private Random random = new Random();
        private volatile boolean start = true;

        public Consumer(int id, Stream stream, CountDownLatch latch, AtomicLong appendedOffset) {
            this.id = id;
            this.stream = stream;
            this.latch = latch;
            this.appendedOffset = appendedOffset;
        }

        @Override
        public void run() {
            while (start) {
                try {
                    fetch();
                } catch (Exception e) {
                    LOGGER.error("Error in consumer", e);
                }
            }
            latch.countDown();
        }

        public void fetch() throws InterruptedException, ExecutionException {
            if (consumeOffset >= MAX_APPENDED_OFFSET) {
                start = false;
                return;
            }
            Thread.sleep(random.nextInt(200));
            long appendEndOffset = appendedOffset.get();
            if (consumeOffset > appendEndOffset) {
                return;
            }
            FetchResult result = stream.fetch(consumeOffset, appendEndOffset + 1, Integer.MAX_VALUE).get();
            LOGGER.info("[Consumer-{}-{}] fetch records: {}", stream.streamId(), id, result.recordBatchList().size());
            result.recordBatchList().forEach(
                    record -> {
                        long offset = record.baseOffset();
                        assertEquals("hello[" + stream.streamId() + "][" + offset + "]", new String(buf2array(record.rawPayload())));
                        LOGGER.info("[Consumer-{}-{}] consume: {}", stream.streamId(), id, offset);
                        consumeOffset = Math.max(consumeOffset, offset + 1);
                    }
            );
        }

    }

}
