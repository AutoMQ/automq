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

package kafka.automq.table.worker;

import kafka.automq.table.Channel;
import kafka.automq.table.events.CommitRequest;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Errors;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.WorkerOffset;
import kafka.automq.table.worker.TopicPartitionsWorker.OffsetBound;
import kafka.automq.table.worker.TopicPartitionsWorker.SyncTask;
import kafka.cluster.Partition;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.storage.internals.log.FetchDataInfo;

import com.automq.stream.utils.Threads;
import com.automq.stream.utils.threads.EventLoop;

import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static kafka.automq.table.worker.TestUtils.newMemoryRecord;
import static kafka.automq.table.worker.TestUtils.partition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
@Timeout(10)
@Execution(SAME_THREAD)
public class TopicPartitionsWorkerTest {
    private static final String TOPIC = "TP_TEST";

    TopicPartitionsWorker topicPartitionsWorker;
    WriterFactory writerFactory;
    List<MemoryWriter> writers;
    Channel channel;
    WorkerConfig config;

    EventLoop eventLoop = new EventLoop("test");
    ExecutorService flushExecutor = Threads.newFixedThreadPoolWithMonitor(1, "test-flush", true, LoggerFactory.getLogger(TopicPartitionsWorkerTest.class));
    EventLoops eventLoops;
    Semaphore commitLimiter;

    volatile double avgRecordRecord = 1;

    @BeforeEach
    public void setup() {
        config = mock(WorkerConfig.class);
        when(config.microSyncBatchSize()).thenReturn(1024);
        when(config.incrementSyncThreshold()).thenReturn(1024L);

        channel = mock(Channel.class);
        when(channel.asyncSend(eq(TOPIC), any())).thenReturn(CompletableFuture.completedFuture(null));

        writerFactory = mock(WriterFactory.class);
        when(writerFactory.partitionSpec()).thenReturn(PartitionSpec.unpartitioned());
        writers = new ArrayList<>();
        when(writerFactory.newWriter()).thenAnswer(invocation -> {
            MemoryWriter writer = new MemoryWriter(config);
            writers.add(writer);
            return writer;
        });

        avgRecordRecord = 1;
        eventLoops = new EventLoops(new EventLoop[] {eventLoop, new EventLoop("worker-test")});
        commitLimiter = new Semaphore(4);
        topicPartitionsWorker = new TopicPartitionsWorker(TOPIC, config, writerFactory, channel, eventLoop, eventLoops, flushExecutor, commitLimiter);
    }

    @Test
    public void testCommit() throws Exception {
        Partition p1 = partition(1, 1);

        Partition p2 = partition(2, 3);

        // 1. commit with p0 missing, p1 mismatch, p2 match
        {
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, newMemoryRecord(30, 3));
            when(p2.log().get().readAsync(eq(30L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().highWatermark()).thenReturn(33L);
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(0, 1, 10),
                new WorkerOffset(1, 2, 20),
                new WorkerOffset(2, 3, 30)));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            eventLoop.submit(() -> {
                topicPartitionsWorker.add(p1);
                topicPartitionsWorker.add(p2);
                commitCf.set(topicPartitionsWorker.commit(commitRequest));
            }).get();
            commitCf.get().get();

            verify(p2.log().get(), times(1)).readAsync(anyLong(), anyInt(), any(), anyBoolean());

            ArgumentCaptor<Event> ac1 = ArgumentCaptor.forClass(Event.class);
            verify(channel, times(2)).asyncSend(eq(TOPIC), ac1.capture());
            CommitResponse resp1 = ac1.getAllValues().get(0).payload();
            assertEquals(commitRequest.commitId(), resp1.commitId());
            assertEquals(Errors.EPOCH_MISMATCH, resp1.code());
            assertEquals(List.of(new WorkerOffset(1, 1, -1)), resp1.nextOffsets());

            CommitResponse resp2 = ac1.getAllValues().get(1).payload();
            assertEquals(commitRequest.commitId(), resp2.commitId());
            assertEquals(Errors.NONE, resp2.code());
            assertEquals(List.of(new WorkerOffset(2, 3, 33)), resp2.nextOffsets());
            assertTrue(writers.get(0).isCompleted());
            assertEquals(3, writers.get(0).records.size());
            for (int i = 0; i < 3; i++) {
                assertEquals(30 + i, writers.get(0).records.get(i).offset());
            }
        }

        // 2. await commit response timeout, resend a new commit request
        for (int i = 0; i < 2; i++) {
            CommitRequest commitRequest2 = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(0, 1, 10),
                new WorkerOffset(1, 2, 20),
                new WorkerOffset(2, 3, 30)));
            AtomicReference<CompletableFuture<Void>> commitCf2 = new AtomicReference<>();
            eventLoop.submit(() -> commitCf2.set(topicPartitionsWorker.commit(commitRequest2))).get();
            commitCf2.get().get();
            // expect no more read
            verify(p2.log().get(), times(1)).readAsync(anyLong(), anyInt(), any(), anyBoolean());
            ArgumentCaptor<Event> ac3 = ArgumentCaptor.forClass(Event.class);
            verify(channel, times((i + 1) * 2 + 2)).asyncSend(eq(TOPIC), ac3.capture());
            CommitResponse resp3 = ac3.getValue().payload();
            assertEquals(commitRequest2.commitId(), resp3.commitId());
            assertEquals(Errors.MORE_DATA, resp3.code());
            assertEquals(List.of(new WorkerOffset(2, 3, 33)), resp3.nextOffsets());
            assertEquals(1, topicPartitionsWorker.writers.size());
            assertTrue(topicPartitionsWorker.writers.get(0).isCompleted());
        }

        // 3. move to the next commit
        {
            CommitRequest commitRequest3 = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(0, 1, 10),
                new WorkerOffset(1, 2, 20),
                new WorkerOffset(2, 3, 33)));
            AtomicReference<CompletableFuture<Void>> commitCf3 = new AtomicReference<>();
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, newMemoryRecord(33, 10));
            when(p2.log().get().readAsync(eq(33L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().highWatermark()).thenReturn(43L);
            eventLoop.submit(() -> commitCf3.set(topicPartitionsWorker.commit(commitRequest3))).get();
            commitCf3.get().get();
            verify(p2.log().get(), times(2)).readAsync(anyLong(), anyInt(), any(), anyBoolean());
            ArgumentCaptor<Event> ac4 = ArgumentCaptor.forClass(Event.class);
            verify(channel, times(8)).asyncSend(eq(TOPIC), ac4.capture());
            CommitResponse resp4 = ac4.getValue().payload();
            assertEquals(commitRequest3.commitId(), resp4.commitId());
            assertEquals(Errors.NONE, resp4.code());
            assertEquals(List.of(new WorkerOffset(2, 3, 43)), resp4.nextOffsets());
            assertEquals(1, topicPartitionsWorker.writers.size());
            MemoryWriter writer = (MemoryWriter) topicPartitionsWorker.writers.get(0);
            assertEquals(10, writer.records.size());
            for (int j = 0; j < 10; j++) {
                assertEquals(33 + j, writer.records.get(j).offset());
            }
        }
        eventLoop.submit(() -> assertEquals(4, commitLimiter.availablePermits())).get();
    }

    @Test
    public void testAdvanceSync() throws Exception {
        Partition p2 = partition(2, 3);

        {
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, newMemoryRecord(30, 3));
            when(p2.log().get().readAsync(eq(30L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().highWatermark()).thenReturn(33L);
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(1, 2, 20),
                new WorkerOffset(2, 3, 30)
            ));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            eventLoop.submit(() -> {
                topicPartitionsWorker.add(p2);
                commitCf.set(topicPartitionsWorker.commit(commitRequest));
            }).get();
            commitCf.get().get();
        }

        {
            avgRecordRecord = 512;
            MemoryRecords records = newMemoryRecord(33, 4, 512);
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, records);
            when(p2.log().get().readAsync(eq(33L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().readAsync(eq(35L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().readAsync(eq(37L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(FetchDataInfo.empty(37L)));
            when(p2.log().get().highWatermark()).thenReturn(37L);

            topicPartitionsWorker.advanceSync.onAppend(new TopicPartition(TOPIC, 2), records);

            eventLoop.submit(() -> topicPartitionsWorker.run()).get();

            for (; ; ) {
                if (topicPartitionsWorker.status == TopicPartitionsWorker.Status.IDLE) {
                    break;
                }
                Threads.sleep(10);
            }
            assertEquals(3, writers.size());
            assertWriter(writers.get(1), 33, 35);
            assertWriter(writers.get(2), 35, 37);
            assertEquals(4, commitLimiter.availablePermits());
        }

        // new commit request
        {
            avgRecordRecord = 1;
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(2, 3, 33)));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, newMemoryRecord(37, 10));
            when(p2.log().get().readAsync(eq(37L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().highWatermark()).thenReturn(47L);
            eventLoop.submit(() -> commitCf.set(topicPartitionsWorker.commit(commitRequest))).get();
            commitCf.get().get();
            ArgumentCaptor<Event> ac = ArgumentCaptor.forClass(Event.class);
            verify(channel, times(2)).asyncSend(eq(TOPIC), ac.capture());
            CommitResponse resp = ac.getValue().payload();
            assertEquals(commitRequest.commitId(), resp.commitId());
            assertEquals(Errors.NONE, resp.code());
            assertEquals(List.of(new WorkerOffset(2, 3, 47)), resp.nextOffsets());
            assertEquals(3, topicPartitionsWorker.writers.size());
            MemoryWriter writer = (MemoryWriter) topicPartitionsWorker.writers.get(2);
            assertEquals(10, writer.records.size());
            assertWriter((MemoryWriter) topicPartitionsWorker.writers.get(2), 37, 47);
        }
    }

    @Test
    public void testPartitionChange() throws Exception {
        Partition p2 = partition(2, 3);

        {
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, newMemoryRecord(30, 3));
            when(p2.log().get().readAsync(eq(30L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().highWatermark()).thenReturn(33L);
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(1, 1, 10),
                new WorkerOffset(2, 3, 30)
            ));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            eventLoop.submit(() -> {
                topicPartitionsWorker.add(p2);
                commitCf.set(topicPartitionsWorker.commit(commitRequest));
            }).get();
            commitCf.get().get();
        }

        Partition p1 = partition(1, 1);
        {
            when(p1.log().get().readAsync(eq(10L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(new FetchDataInfo(null, newMemoryRecord(10, 5))));
            when(p1.log().get().highWatermark()).thenReturn(15L);
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(1, 1, 10),
                new WorkerOffset(2, 3, 30)
            ));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            eventLoop.submit(() -> {
                topicPartitionsWorker.add(p1);
                commitCf.set(topicPartitionsWorker.commit(commitRequest));
            }).get();
            commitCf.get().get();
            ArgumentCaptor<Event> ac = ArgumentCaptor.forClass(Event.class);
            verify(channel, times(2)).asyncSend(eq(TOPIC), ac.capture());
            CommitResponse resp = ac.getValue().payload();
            assertEquals(commitRequest.commitId(), resp.commitId());
            assertEquals(Errors.NONE, resp.code());
            assertEquals(List.of(new WorkerOffset(1, 1, 15), new WorkerOffset(2, 3, 33)), resp.nextOffsets());
            assertEquals(1, topicPartitionsWorker.writers.size());
            verify(p1.log().get(), times(1)).readAsync(anyLong(), anyInt(), any(), anyBoolean());
            // reset the writer and re-read
            verify(p2.log().get(), times(2)).readAsync(anyLong(), anyInt(), any(), anyBoolean());
        }
    }

    @Test
    public void testFlushFail() throws Exception {
        MemoryWriter writer = spy(new MemoryWriter(config));
        when(writerFactory.newWriter()).thenReturn(writer);
        doReturn(CompletableFuture.failedFuture(new IOException("test io exception"))).when(writer).flush(any(), any(), any());

        Partition p2 = partition(2, 3);

        {
            FetchDataInfo fetchDataInfo = new FetchDataInfo(null, newMemoryRecord(30, 3));
            when(p2.log().get().readAsync(eq(30L), anyInt(), any(), eq(true))).thenReturn(CompletableFuture.completedFuture(fetchDataInfo));
            when(p2.log().get().highWatermark()).thenReturn(33L);
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(2, 3, 30)
            ));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            eventLoop.submit(() -> {
                topicPartitionsWorker.add(p2);
                commitCf.set(topicPartitionsWorker.commit(commitRequest));
            }).get();
            commitCf.get().get();
            ArgumentCaptor<Event> ac = ArgumentCaptor.forClass(Event.class);
            verify(channel, times(1)).asyncSend(eq(TOPIC), ac.capture());
            CommitResponse resp = ac.getValue().payload();
            assertEquals(commitRequest.commitId(), resp.commitId());
            assertEquals(Errors.MORE_DATA, resp.code());
            assertEquals(List.of(new WorkerOffset(2, 3, 30)), resp.nextOffsets());
        }

        MemoryWriter writer2 = new MemoryWriter(config);
        when(writerFactory.newWriter()).thenReturn(writer2);
        {
            CommitRequest commitRequest = new CommitRequest(UUID.randomUUID(), TOPIC, List.of(
                new WorkerOffset(2, 3, 30)
            ));
            AtomicReference<CompletableFuture<Void>> commitCf = new AtomicReference<>();
            eventLoop.submit(() -> {
                topicPartitionsWorker.add(p2);
                commitCf.set(topicPartitionsWorker.commit(commitRequest));
            }).get();
            commitCf.get().get();
            ArgumentCaptor<Event> ac = ArgumentCaptor.forClass(Event.class);
            verify(channel, times(2)).asyncSend(eq(TOPIC), ac.capture());
            CommitResponse resp = ac.getValue().payload();
            assertEquals(commitRequest.commitId(), resp.commitId());
            assertEquals(Errors.NONE, resp.code());
            assertEquals(List.of(new WorkerOffset(2, 3, 33)), resp.nextOffsets());
        }

    }

    @Test
    public void testSyncTask() throws ExecutionException, InterruptedException {
        Partition p1 = partition(1, 2);
        Partition p2 = partition(2, 3);
        eventLoop.submit(() -> {
            topicPartitionsWorker.add(p1);
            topicPartitionsWorker.add(p2);

            avgRecordRecord = 128;
            when(p1.log().get().highWatermark()).thenReturn(50L);
            when(p2.log().get().highWatermark()).thenReturn(100L);

            SyncTask syncTask = topicPartitionsWorker.newSyncTask("test", Map.of(1, 10L, 2, 20L), 0);
            syncTask.plan();
            assertTrue(syncTask.hasMoreData());
            List<kafka.automq.table.worker.TopicPartitionsWorker.MicroSyncBatchTask> tasks = syncTask.microSyncBatchTasks;
            assertEquals(2, tasks.size());
            assertEquals(List.of(new OffsetBound(p2, 20L, 28L)), tasks.get(0).offsetBounds());
            assertEquals(List.of(new OffsetBound(p1, 10L, 18L)), tasks.get(1).offsetBounds());
        }).get();
    }

    @Test
    public void testMicroSyncBatchTask() throws ExecutionException, InterruptedException {
        Partition p1 = partition(1, 2);
        Partition p2 = partition(2, 3);
        {
            EventLoops.EventLoopRef eventLoopRef = new EventLoops.EventLoopRef(new EventLoops.EventLoopWrapper(eventLoop));
            PartitionWriteTaskContext ctx = new PartitionWriteTaskContext(new MemoryWriter(config), eventLoopRef, flushExecutor, config, 0);
            MicroSyncBatchTask task = spy(new MicroSyncBatchTask("test", ctx));
            task.addPartition(new OffsetBound(p1, 100, 200));
            task.addPartition(new OffsetBound(p2, 10, 50));
            task.startOffsets(Map.of(1, 110L, 2, 30L));
            task.endOffsets(100, 1);
            assertEquals(List.of(new OffsetBound(p1, 110L, 190L), new OffsetBound(p2, 30L, 50L)), task.offsetBounds());
            assertTrue(task.hasMoreData());
            task.run().get();
            verify(task, times(1)).runPartitionWriteTask(eq(p1), eq(110L), eq(190L));
            verify(task, times(1)).runPartitionWriteTask(eq(p2), eq(30L), eq(50L));
            assertEquals(0, eventLoopRef.inflight.get());
        }
        {
            EventLoops.EventLoopRef eventLoopRef = new EventLoops.EventLoopRef(new EventLoops.EventLoopWrapper(eventLoop));
            PartitionWriteTaskContext ctx = new PartitionWriteTaskContext(new MemoryWriter(config), eventLoopRef, flushExecutor, config, 0);
            MicroSyncBatchTask task = spy(new MicroSyncBatchTask("test", ctx));
            task.addPartition(new OffsetBound(p1, 100, 200));
            task.addPartition(new OffsetBound(p2, 10, 50));
            task.startOffsets(Map.of(1, 110L, 2, 30L));
            task.endOffsets(200, 1);
            assertEquals(List.of(new OffsetBound(p1, 110L, 200L), new OffsetBound(p2, 30L, 50L)), task.offsetBounds());
            assertFalse(task.hasMoreData());
        }
    }

    private static void assertWriter(MemoryWriter writer, long start, long end) {
        assertTrue(writer.isCompleted());
        assertEquals(end - start, writer.records.size());
        for (int i = 0; i < writer.records.size(); i++) {
            assertEquals(start + i, writer.records.get(i).offset());
        }
    }

    class TopicPartitionsWorker extends kafka.automq.table.worker.TopicPartitionsWorker {

        public TopicPartitionsWorker(String topic, WorkerConfig config, WriterFactory writerFactory, Channel channel,
            EventLoop eventLoop, EventLoops eventLoops, ExecutorService flushExecutors, Semaphore commitLimiter) {
            super(topic, config, writerFactory, channel, eventLoop, eventLoops, flushExecutors, commitLimiter);
        }

        @Override
        protected double getAvgRecordSize() {
            return avgRecordRecord;
        }

        @Override
        protected double getDecompressedRatio() {
            return 1.0;
        }
    }

    class MicroSyncBatchTask extends kafka.automq.table.worker.TopicPartitionsWorker.MicroSyncBatchTask {
        public MicroSyncBatchTask(String logContext, PartitionWriteTaskContext ctx) {
            super(logContext, ctx);
        }

        @Override
        protected CompletableFuture<Void> runPartitionWriteTask(Partition partition, long start, long end) {
            return CompletableFuture.completedFuture(null);
        }
    }

}
