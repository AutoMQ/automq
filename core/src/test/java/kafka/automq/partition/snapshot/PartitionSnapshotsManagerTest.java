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

package kafka.automq.partition.snapshot;

import kafka.automq.AutoMQConfig;
import kafka.cluster.Partition;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotRequestData;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotRequest;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.automq.AutoMQVersion;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.TimestampOffset;

import com.automq.stream.s3.ConfirmWAL;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.Unpooled;
import scala.Function0;
import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionSnapshotsManagerTest {
    private MockTime time;
    private ConfirmWAL confirmWAL;
    private AtomicReference<ConfirmWAL.AppendListener> appendListener;
    private PartitionSnapshotsManager manager;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        AutoMQConfig config = mock(AutoMQConfig.class);
        when(config.zoneRouterChannels()).thenReturn(Optional.empty());
        confirmWAL = mock(ConfirmWAL.class);
        appendListener = new AtomicReference<>();
        when(confirmWAL.addAppendListener(any())).thenAnswer(invocation -> {
            appendListener.set(invocation.getArgument(0));
            return (ConfirmWAL.ListenerHandle) () -> {
            };
        });
        when(confirmWAL.commit(anyLong(), eq(false))).thenReturn(CompletableFuture.completedFuture(null));
        manager = new PartitionSnapshotsManager(time, config, confirmWAL, () -> AutoMQVersion.V3);
    }

    @Test
    public void testIdleExistingSessionLongPollsUntilTimeout() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);

        CompletableFuture<AutomqGetPartitionSnapshotResponse> second = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));

        assertFalse(second.isDone());

        AutomqGetPartitionSnapshotResponse timeoutResponse = second.get(2, TimeUnit.SECONDS);
        assertEquals(first.data().sessionId(), timeoutResponse.data().sessionId());
        assertEquals(first.data().sessionEpoch() + 1, timeoutResponse.data().sessionEpoch());
        assertTrue(timeoutResponse.data().topics().isEmpty());
    }

    @Test
    public void testRequestCommitBypassesLongPoll() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);

        CompletableFuture<AutomqGetPartitionSnapshotResponse> second = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), true));

        assertTrue(second.isDone());
        AutomqGetPartitionSnapshotResponse response = second.get(1, TimeUnit.SECONDS);
        assertEquals(first.data().sessionEpoch() + 1, response.data().sessionEpoch());
    }

    @Test
    public void testPartitionOpenWakesPendingLongPoll() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);
        CompletableFuture<AutomqGetPartitionSnapshotResponse> second = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));

        assertFalse(second.isDone());

        Uuid topicId = Uuid.randomUuid();
        manager.onPartitionOpen(partition(topicId, 3, 7, 9));

        AutomqGetPartitionSnapshotResponse response = second.get(1, TimeUnit.SECONDS);
        assertEquals(first.data().sessionEpoch() + 1, response.data().sessionEpoch());
        assertEquals(1, response.data().topics().size());
        assertEquals(topicId, response.data().topics().find(topicId).topicId());
        assertEquals(3, response.data().topics().find(topicId).partitions().get(0).partitionIndex());
    }

    @Test
    public void testConfirmWalDeltaWakesPendingLongPoll() throws Exception {
        when(confirmWAL.confirmOffset()).thenReturn(DefaultRecordOffset.of(1, 0, 0));
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false, (short) 1, (short) 2))
            .get(1, TimeUnit.SECONDS);

        CompletableFuture<AutomqGetPartitionSnapshotResponse> second = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false, (short) 1, (short) 2));
        assertFalse(second.isDone());
        appendWalRecord(0, 0);

        AutomqGetPartitionSnapshotResponse walOffsetResponse = second.get(1, TimeUnit.SECONDS);
        assertTrue(walOffsetResponse.data().topics().isEmpty());
        assertEquals(1, DefaultRecordOffset.of(Unpooled.wrappedBuffer(walOffsetResponse.data().confirmWalEndOffset())).offset());

        CompletableFuture<AutomqGetPartitionSnapshotResponse> third = manager.handle(
            request(walOffsetResponse.data().sessionId(), walOffsetResponse.data().sessionEpoch(), false, (short) 1, (short) 2));
        assertFalse(third.isDone());
        appendWalRecord(1, 1);

        AutomqGetPartitionSnapshotResponse walDeltaResponse = third.get(1, TimeUnit.SECONDS);
        assertTrue(walDeltaResponse.data().topics().isEmpty());
        assertTrue(walDeltaResponse.data().confirmWalDeltaData().length > 0);
    }

    @Test
    public void testAlreadyFailedSnapshotFutureStillCompletesCollectedDelta() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> failedSnapshotCf = new CompletableFuture<>();
        failedSnapshotCf.completeExceptionally(new IllegalStateException("snapshot failed"));
        manager.onPartitionOpen(partition(topicId, 3, 7, 9, failedSnapshotCf));

        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);

        CompletableFuture<AutomqGetPartitionSnapshotResponse> second = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));

        AutomqGetPartitionSnapshotResponse response = second.get(1, TimeUnit.SECONDS);
        assertEquals(first.data().sessionId(), response.data().sessionId());
        assertEquals(first.data().sessionEpoch() + 1, response.data().sessionEpoch());
        assertEquals(1, response.data().topics().size());
        assertEquals(topicId, response.data().topics().find(topicId).topicId());
        assertEquals(3, response.data().topics().find(topicId).partitions().get(0).partitionIndex());
    }

    @Test
    public void testConcurrentRequestReusesPendingLongPoll() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);
        CompletableFuture<AutomqGetPartitionSnapshotResponse> pending = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));
        assertFalse(pending.isDone());

        CompletableFuture<AutomqGetPartitionSnapshotResponse> retry = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), true));

        assertFalse(retry.isDone());

        AutomqGetPartitionSnapshotResponse retryResponse = retry.get(2, TimeUnit.SECONDS);
        AutomqGetPartitionSnapshotResponse pendingResponse = pending.get(1, TimeUnit.SECONDS);
        assertEquals(retryResponse.data().sessionId(), pendingResponse.data().sessionId());
        assertEquals(retryResponse.data().sessionEpoch(), pendingResponse.data().sessionEpoch());

        AutomqGetPartitionSnapshotResponse next = manager.handle(
            request(retryResponse.data().sessionId(), retryResponse.data().sessionEpoch(), true)).get(1, TimeUnit.SECONDS);
        assertEquals(retryResponse.data().sessionId(), next.data().sessionId());
        assertEquals(retryResponse.data().sessionEpoch() + 1, next.data().sessionEpoch());
    }

    @Test
    public void testTimeoutDoesNotDropInFlightCollectedDeltas() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);

        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> slowSnapshotCf = new CompletableFuture<>();
        manager.onPartitionOpen(partition(topicId, 3, 7, 9, slowSnapshotCf));

        CompletableFuture<AutomqGetPartitionSnapshotResponse> pending = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));
        assertFalse(pending.isDone());

        Thread.sleep(PartitionSnapshotsManager.LONG_POLL_TIMEOUT_MS + 100);
        assertFalse(pending.isDone());

        slowSnapshotCf.complete(null);
        AutomqGetPartitionSnapshotResponse response = pending.get(1, TimeUnit.SECONDS);
        assertEquals(first.data().sessionEpoch() + 1, response.data().sessionEpoch());
        assertEquals(1, response.data().topics().size());
        assertEquals(topicId, response.data().topics().find(topicId).topicId());
        assertEquals(3, response.data().topics().find(topicId).partitions().get(0).partitionIndex());
    }

    @Test
    public void testConcurrentRequestReusesCompletingLongPoll() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);
        CompletableFuture<AutomqGetPartitionSnapshotResponse> pending = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));
        assertFalse(pending.isDone());

        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> slowSnapshotCf = new CompletableFuture<>();
        manager.onPartitionOpen(partition(topicId, 3, 7, 9, slowSnapshotCf));
        Thread.sleep(100);
        assertFalse(pending.isDone());

        CompletableFuture<AutomqGetPartitionSnapshotResponse> retry = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), true));
        assertFalse(retry.isDone());

        slowSnapshotCf.complete(null);
        AutomqGetPartitionSnapshotResponse retryResponse = retry.get(1, TimeUnit.SECONDS);
        AutomqGetPartitionSnapshotResponse pendingResponse = pending.get(1, TimeUnit.SECONDS);
        assertEquals(retryResponse.data().sessionId(), pendingResponse.data().sessionId());
        assertEquals(retryResponse.data().sessionEpoch(), pendingResponse.data().sessionEpoch());
        assertEquals(1, retryResponse.data().topics().size());
        assertEquals(topicId, retryResponse.data().topics().find(topicId).topicId());
    }

    @Test
    public void testInFlightCollectionCompletesConcurrentPendingRetry() throws Exception {
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false)).get(1, TimeUnit.SECONDS);

        Uuid topicId = Uuid.randomUuid();
        CompletableFuture<Void> slowSnapshotCf = new CompletableFuture<>();
        manager.onPartitionOpen(partition(topicId, 3, 7, 9, slowSnapshotCf));

        CompletableFuture<AutomqGetPartitionSnapshotResponse> collecting = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));
        assertFalse(collecting.isDone());

        CompletableFuture<AutomqGetPartitionSnapshotResponse> retry = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false));
        assertFalse(retry.isDone());

        slowSnapshotCf.complete(null);
        AutomqGetPartitionSnapshotResponse collectingResponse = collecting.get(1, TimeUnit.SECONDS);
        AutomqGetPartitionSnapshotResponse retryResponse = retry.get(1, TimeUnit.SECONDS);
        assertEquals(collectingResponse.data().sessionId(), retryResponse.data().sessionId());
        assertEquals(collectingResponse.data().sessionEpoch(), retryResponse.data().sessionEpoch());
        assertEquals(1, retryResponse.data().topics().size());
        assertEquals(topicId, retryResponse.data().topics().find(topicId).topicId());
        assertEquals(3, retryResponse.data().topics().find(topicId).partitions().get(0).partitionIndex());
    }

    @Test
    public void testStaleWalNotificationForceCompletesIdleLongPoll() throws Exception {
        when(confirmWAL.confirmOffset()).thenReturn(DefaultRecordOffset.of(1, 0, 0));
        AutomqGetPartitionSnapshotResponse first = manager.handle(request(0, 0, false, (short) 1, (short) 2))
            .get(1, TimeUnit.SECONDS);

        CountDownLatch executorBlocked = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        longPollExecutor().execute(() -> {
            executorBlocked.countDown();
            try {
                releaseExecutor.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue(executorBlocked.await(1, TimeUnit.SECONDS));

        appendWalRecord(0, 0);
        AutomqGetPartitionSnapshotResponse walResponse = manager.handle(
            request(first.data().sessionId(), first.data().sessionEpoch(), false, (short) 1, (short) 2))
            .get(1, TimeUnit.SECONDS);

        CompletableFuture<AutomqGetPartitionSnapshotResponse> pending = manager.handle(
            request(walResponse.data().sessionId(), walResponse.data().sessionEpoch(), false, (short) 1, (short) 2));
        assertFalse(pending.isDone());

        releaseExecutor.countDown();

        AutomqGetPartitionSnapshotResponse response = pending.get(1, TimeUnit.SECONDS);
        assertEquals(walResponse.data().sessionEpoch() + 1, response.data().sessionEpoch());
        assertTrue(response.data().topics().isEmpty());
    }

    private static AutomqGetPartitionSnapshotRequest request(int sessionId, int sessionEpoch, boolean requestCommit) {
        return request(sessionId, sessionEpoch, requestCommit, (short) 0, (short) 0);
    }

    private static AutomqGetPartitionSnapshotRequest request(int sessionId, int sessionEpoch, boolean requestCommit,
        short dataVersion, short requestVersion) {
        return new AutomqGetPartitionSnapshotRequest(
            new AutomqGetPartitionSnapshotRequestData()
                .setSessionId(sessionId)
                .setSessionEpoch(sessionEpoch)
                .setRequestCommit(requestCommit)
                .setVersion(dataVersion),
            requestVersion
        );
    }

    private void appendWalRecord(long baseOffset, long walOffset) {
        StreamRecordBatch record = StreamRecordBatch.of(1, 2, baseOffset, 1, Unpooled.wrappedBuffer(new byte[1]));
        appendListener.get().onAppend(
            record,
            DefaultRecordOffset.of(1, walOffset, 1),
            DefaultRecordOffset.of(1, walOffset + 1, 0)
        );
        record.release();
    }

    private ScheduledExecutorService longPollExecutor() throws Exception {
        Field field = PartitionSnapshotsManager.class.getDeclaredField("longPollExecutor");
        field.setAccessible(true);
        return (ScheduledExecutorService) field.get(manager);
    }

    private static Partition partition(Uuid topicId, int partitionId, int leaderEpoch, long endOffset) {
        return partition(topicId, partitionId, leaderEpoch, endOffset, CompletableFuture.completedFuture(null));
    }

    private static Partition partition(Uuid topicId, int partitionId, int leaderEpoch, long endOffset,
        CompletableFuture<Void> completeCf) {
        Partition partition = mock(Partition.class);
        when(partition.topicId()).thenReturn(Option.apply(topicId));
        when(partition.partitionId()).thenReturn(partitionId);
        when(partition.getLeaderEpoch()).thenReturn(leaderEpoch);
        when(partition.maybeAddListener(any())).thenReturn(true);
        doAnswer(invocation -> ((Function0<?>) invocation.getArgument(0)).apply())
            .when(partition).withReadLock(any());
        when(partition.snapshot()).thenReturn(new kafka.cluster.PartitionSnapshot(
            leaderEpoch,
            null,
            new LogOffsetMetadata(endOffset),
            new LogOffsetMetadata(endOffset),
            Map.of(1L, endOffset),
            new TimestampOffset(1, endOffset),
            completeCf
        ));
        return partition;
    }
}
