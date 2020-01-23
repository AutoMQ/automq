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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransactionManagerTest {
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = Integer.MAX_VALUE;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;
    private final String transactionalId = "foobar";
    private final int transactionTimeoutMs = 1121;

    private final String topic = "test";
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final ProducerMetadata metadata = new ProducerMetadata(0, Long.MAX_VALUE, logContext,
            new ClusterResourceListeners(), time);
    private final MockClient client = new MockClient(time, metadata);
    private final ApiVersions apiVersions = new ApiVersions();

    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TransactionManager transactionManager = null;
    private Node brokerNode = null;

    @Before
    public void setup() {
        this.metadata.add("test", time.milliseconds());
        this.client.updateMetadata(TestUtils.metadataUpdateWith(1, singletonMap("test", 2)));
        this.brokerNode = new Node(0, "localhost", 2211);

        initializeTransactionManager(Optional.of(transactionalId));
    }

    private void initializeTransactionManager(Optional<String> transactionalId) {
        Metrics metrics = new Metrics(time);

        this.transactionManager = new TransactionManager(logContext, transactionalId.orElse(null),
                transactionTimeoutMs, DEFAULT_RETRY_BACKOFF_MS);

        int batchSize = 16 * 1024;
        int deliveryTimeoutMs = 3000;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";

        this.accumulator = new RecordAccumulator(logContext, batchSize, CompressionType.NONE, 0, 0L,
                deliveryTimeoutMs, metrics, metricGrpName, time, apiVersions, transactionManager,
                new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, true,
                MAX_REQUEST_SIZE, ACKS_ALL, MAX_RETRIES, new SenderMetricsRegistry(metrics), this.time, REQUEST_TIMEOUT,
                50, transactionManager, apiVersions);
    }

    @Test
    public void testSenderShutdownWithPendingTransactions() throws Exception {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        FutureRecordMetadata sendFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(() -> !client.hasPendingResponses());

        sender.initiateClose();
        sender.runOnce();

        TransactionalRequestResult result = transactionManager.beginCommit();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(result::isCompleted);
        runUntil(sendFuture::isDone);
    }

    @Test
    public void testEndTxnNotSentIfIncompleteBatches() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> transactionManager.isPartitionAdded(tp0));

        transactionManager.beginCommit();
        assertNull(transactionManager.nextRequest(true));
        assertTrue(transactionManager.nextRequest(false).isEndTxn());
    }

    @Test(expected = IllegalStateException.class)
    public void testFailIfNotReadyForSendNoProducerId() {
        transactionManager.failIfNotReadyForSend();
    }

    @Test
    public void testFailIfNotReadyForSendIdempotentProducer() {
        initializeTransactionManager(Optional.empty());
        transactionManager.failIfNotReadyForSend();
    }

    @Test(expected = KafkaException.class)
    public void testFailIfNotReadyForSendIdempotentProducerFatalError() {
        initializeTransactionManager(Optional.empty());
        transactionManager.transitionToFatalError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
    }

    @Test(expected = IllegalStateException.class)
    public void testFailIfNotReadyForSendNoOngoingTransaction() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.failIfNotReadyForSend();
    }

    @Test(expected = KafkaException.class)
    public void testFailIfNotReadyForSendAfterAbortableError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.transitionToAbortableError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
    }

    @Test(expected = KafkaException.class)
    public void testFailIfNotReadyForSendAfterFatalError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.transitionToFatalError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
    }

    @Test
    public void testHasOngoingTransactionSuccessfulAbort() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        runUntil(transactionManager::hasOngoingTransaction);

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.isPartitionAdded(partition));

        transactionManager.beginAbort();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionSuccessfulCommit() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.isPartitionAdded(partition));

        transactionManager.beginCommit();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionAbortableError() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.isPartitionAdded(partition));

        transactionManager.transitionToAbortableError(new KafkaException());
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.beginAbort();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionFatalError() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions(pid, epoch);
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.isPartitionAdded(partition));

        transactionManager.transitionToFatalError(new KafkaException());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testMaybeAddPartitionToTransaction() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        assertTrue(transactionManager.hasPartitionsToAdd());

        runUntil(() -> transactionManager.isPartitionAdded(partition));
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionPendingAdd(partition));

        // adding the partition again should not have any effect
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertTrue(transactionManager.isPartitionAdded(partition));
        assertFalse(transactionManager.isPartitionPendingAdd(partition));
    }

    @Test
    public void testAddPartitionToTransactionOverridesRetryBackoffForConcurrentTransactions() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.CONCURRENT_TRANSACTIONS);
        runUntil(() -> !client.hasPendingResponses());

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);
        assertNotNull(handler);
        assertEquals(20, handler.retryBackoffMs());
    }

    @Test
    public void testAddPartitionToTransactionRetainsRetryBackoffForRegularRetriableError() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.COORDINATOR_NOT_AVAILABLE);
        runUntil(() -> !client.hasPendingResponses());

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);
        assertNotNull(handler);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs());
    }

    @Test
    public void testAddPartitionToTransactionRetainsRetryBackoffWhenPartitionsAlreadyAdded() {
        long pid = 13131L;
        short epoch = 1;
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionAdded(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.isPartitionAdded(partition));

        TopicPartition otherPartition = new TopicPartition("foo", 1);
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(otherPartition);

        prepareAddPartitionsToTxn(otherPartition, Errors.CONCURRENT_TRANSACTIONS);
        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);
        assertNotNull(handler);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs());
    }

    @Test(expected = IllegalStateException.class)
    public void testMaybeAddPartitionToTransactionBeforeInitTransactions() {
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testMaybeAddPartitionToTransactionBeforeBeginTransaction() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test(expected = KafkaException.class)
    public void testMaybeAddPartitionToTransactionAfterAbortableError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.transitionToAbortableError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test(expected = KafkaException.class)
    public void testMaybeAddPartitionToTransactionAfterFatalError() {
        long pid = 13131L;
        short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.transitionToFatalError(new KafkaException());
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(new TopicPartition("foo", 0));
    }

    @Test
    public void testIsSendToPartitionAllowedWithPendingPartitionAfterAbortableError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.transitionToAbortableError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithInFlightPartitionAddAfterAbortableError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        // Send the AddPartitionsToTxn request and leave it in-flight
        runUntil(transactionManager::hasInFlightRequest);
        transactionManager.transitionToAbortableError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithPendingPartitionAfterFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithInFlightPartitionAddAfterFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        // Send the AddPartitionsToTxn request and leave it in-flight
        runUntil(transactionManager::hasInFlightRequest);
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithAddedPartitionAfterAbortableError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        runUntil(() -> !transactionManager.hasPartitionsToAdd());
        transactionManager.transitionToAbortableError(new KafkaException());

        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithAddedPartitionAfterFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        runUntil(() -> !transactionManager.hasPartitionsToAdd());
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithPartitionNotAdded() {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
    }

    @Test
    public void testDefaultSequenceNumber() {
        initializeTransactionManager(Optional.empty());
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, 3);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 3);
    }

    @Test
    public void testResetSequenceNumbersAfterUnknownProducerId() {
        final long producerId = 13131L;
        final short epoch = 1;

        initializeTransactionManager(Optional.empty());
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        ProducerBatch b4 = writeIdempotentBatchWithValue(transactionManager, tp0, "4");
        ProducerBatch b5 = writeIdempotentBatchWithValue(transactionManager, tp0, "5");
        assertEquals(5, transactionManager.sequenceNumber(tp0).intValue());

        // First batch succeeds
        long b1AppendTime = time.milliseconds();
        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        b1.done(500L, b1AppendTime, null);
        transactionManager.handleCompletedBatch(b1, b1Response);

        // Retention caused log start offset to jump forward. We set sequence numbers back to 0
        ProduceResponse.PartitionResponse b2Response = new ProduceResponse.PartitionResponse(
                Errors.UNKNOWN_PRODUCER_ID, -1, -1, 600L);
        assertTrue(transactionManager.canRetry(b2Response, b2));
        assertEquals(4, transactionManager.sequenceNumber(tp0).intValue());
        assertEquals(0, b2.baseSequence());
        assertEquals(1, b3.baseSequence());
        assertEquals(2, b4.baseSequence());
        assertEquals(3, b5.baseSequence());
    }

    @Test
    public void testAdjustSequenceNumbersAfterFatalError() {
        final long producerId = 13131L;
        final short epoch = 1;

        initializeTransactionManager(Optional.empty());
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        ProducerBatch b4 = writeIdempotentBatchWithValue(transactionManager, tp0, "4");
        ProducerBatch b5 = writeIdempotentBatchWithValue(transactionManager, tp0, "5");
        assertEquals(5, transactionManager.sequenceNumber(tp0).intValue());

        // First batch succeeds
        long b1AppendTime = time.milliseconds();
        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        b1.done(500L, b1AppendTime, null);
        transactionManager.handleCompletedBatch(b1, b1Response);

        // Second batch fails with a fatal error. Sequence numbers are adjusted by one for remaining
        // inflight batches.
        ProduceResponse.PartitionResponse b2Response = new ProduceResponse.PartitionResponse(
                Errors.MESSAGE_TOO_LARGE, -1, -1, 0L);
        assertFalse(transactionManager.canRetry(b2Response, b2));

        b2.done(-1L, -1L, Errors.MESSAGE_TOO_LARGE.exception());
        transactionManager.handleFailedBatch(b2, Errors.MESSAGE_TOO_LARGE.exception(), true);
        assertEquals(4, transactionManager.sequenceNumber(tp0).intValue());
        assertEquals(1, b3.baseSequence());
        assertEquals(2, b4.baseSequence());
        assertEquals(3, b5.baseSequence());

        // The remaining batches are doomed to fail, but they can be retried. Expected
        // sequence numbers should remain the same.
        ProduceResponse.PartitionResponse b3Response = new ProduceResponse.PartitionResponse(
                Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1, -1, 0L);
        assertTrue(transactionManager.canRetry(b3Response, b3));
        assertEquals(4, transactionManager.sequenceNumber(tp0).intValue());
        assertEquals(1, b3.baseSequence());
        assertEquals(2, b4.baseSequence());
        assertEquals(3, b5.baseSequence());
    }

    @Test
    public void testBatchFailureAfterProducerReset() {
        // This tests a scenario where the producerId is reset while pending requests are still inflight.
        // The returned responses should not update internal state.

        final long producerId = 13131L;
        final short epoch = 1;

        initializeTransactionManager(Optional.empty());
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");

        transactionManager.resetIdempotentProducerId();
        initializeIdempotentProducerId(producerId + 1, epoch);

        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        assertEquals(1, transactionManager.sequenceNumber(tp0).intValue());

        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.UNKNOWN_PRODUCER_ID, -1, -1, 400L);
        assertFalse(transactionManager.canRetry(b1Response, b1));
        transactionManager.handleFailedBatch(b1, Errors.UNKNOWN_PRODUCER_ID.exception(), true);

        assertEquals(1, transactionManager.sequenceNumber(tp0).intValue());
        assertEquals(b2, transactionManager.nextBatchBySequence(tp0));
    }

    @Test
    public void testBatchCompletedAfterProducerReset() {
        final long producerId = 13131L;
        final short epoch = 1;

        initializeTransactionManager(Optional.empty());
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");

        // The producerId might be reset due to a failure on another partition
        transactionManager.resetIdempotentProducerId();
        initializeIdempotentProducerId(producerId + 1, epoch);

        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        assertEquals(1, transactionManager.sequenceNumber(tp0).intValue());

        // If the request returns successfully, we should ignore the response and not update any state
        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L);
        transactionManager.handleCompletedBatch(b1, b1Response);

        assertEquals(1, transactionManager.sequenceNumber(tp0).intValue());
        assertEquals(b2, transactionManager.nextBatchBySequence(tp0));
    }

    private ProducerBatch writeIdempotentBatchWithValue(TransactionManager manager,
                                                        TopicPartition tp,
                                                        String value) {
        int seq = manager.sequenceNumber(tp);
        manager.incrementSequenceNumber(tp, 1);
        ProducerBatch batch = batchWithValue(tp, value);
        batch.setProducerState(manager.producerIdAndEpoch(), seq, false);
        manager.addInFlightBatch(batch);
        batch.close();
        return batch;
    }

    private ProducerBatch batchWithValue(TopicPartition tp, String value) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(64),
                CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        long currentTimeMs = time.milliseconds();
        ProducerBatch batch = new ProducerBatch(tp, builder, currentTimeMs);
        batch.tryAppend(currentTimeMs, new byte[0], value.getBytes(), new Header[0], null, currentTimeMs);
        return batch;
    }

    @Test
    public void testSequenceNumberOverflow() {
        initializeTransactionManager(Optional.empty());
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, Integer.MAX_VALUE);
        assertEquals((int) transactionManager.sequenceNumber(tp0), Integer.MAX_VALUE);
        transactionManager.incrementSequenceNumber(tp0, 100);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 99);
        transactionManager.incrementSequenceNumber(tp0, Integer.MAX_VALUE);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 98);
    }

    @Test
    public void testProducerIdReset() {
        initializeTransactionManager(Optional.empty());
        initializeIdempotentProducerId(15L, (short) 0);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, 3);
        assertEquals((int) transactionManager.sequenceNumber(tp0), 3);
        transactionManager.resetIdempotentProducerId();
        assertEquals((int) transactionManager.sequenceNumber(tp0), 0);
    }

    @Test
    public void testBasicTransaction() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        prepareProduceResponse(Errors.NONE, pid, epoch);
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());
        runUntil(responseFuture::isDone);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));

        final String consumerGroupId = "myconsumergroup";
        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId));

        assertFalse(transactionManager.hasPendingOffsetCommits());

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);

        runUntil(transactionManager::hasPendingOffsetCommits);
        assertFalse(addOffsetsResult.isCompleted()); // the result doesn't complete until TxnOffsetCommit returns

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp1, Errors.NONE);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, txnOffsetCommitResponse);

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);
        assertTrue(transactionManager.hasPendingOffsetCommits());

        runUntil(() -> !transactionManager.hasPendingOffsetCommits());
        assertTrue(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete.

        transactionManager.beginCommit();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testDisconnectAndRetry() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, true, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
    }

    @Test
    public void testInitializeTransactionsTwiceRaisesError() {
        final long producerId = 13131L;
        final short epoch = 1;
        doInitTransactions(producerId, epoch);
        assertTrue(transactionManager.hasProducerId());
        assertThrows(KafkaException.class, () -> transactionManager.initializeTransactions());
    }

    @Test
    public void testUnsupportedFindCoordinator() {
        transactionManager.initializeTransactions();
        client.prepareUnsupportedVersionResponse(body -> {
            FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
            assertEquals(CoordinatorType.forId(findCoordinatorRequest.data().keyType()), CoordinatorType.TRANSACTION);
            assertEquals(findCoordinatorRequest.data().key(), transactionalId);
            return true;
        });

        runUntil(transactionManager::hasFatalError);
        assertTrue(transactionManager.hasFatalError());
        assertTrue(transactionManager.lastError() instanceof UnsupportedVersionException);
    }

    @Test
    public void testUnsupportedInitTransactions() {
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertFalse(transactionManager.hasError());

        client.prepareUnsupportedVersionResponse(body -> {
            InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
            assertEquals(initProducerIdRequest.data.transactionalId(), transactionalId);
            assertEquals(initProducerIdRequest.data.transactionTimeoutMs(), transactionTimeoutMs);
            return true;
        });

        runUntil(transactionManager::hasFatalError);
        assertTrue(transactionManager.hasFatalError());
        assertTrue(transactionManager.lastError() instanceof UnsupportedVersionException);
    }

    @Test
    public void testUnsupportedForMessageFormatInTxnOffsetCommit() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT));
        runUntil(transactionManager::hasError);

        assertTrue(transactionManager.lastError() instanceof UnsupportedForMessageFormatException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof UnsupportedForMessageFormatException);
        assertFatalError(UnsupportedForMessageFormatException.class);
    }

    @Test
    public void testFencedInstanceIdInTxnOffsetCommitByGroupMetadata() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);
        final String memberId = "member";
        final String fencedMemberId = "fenced_member";
        final String instanceId = "instance";

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)),
            new ConsumerGroupMetadata(consumerGroupId, 5, fencedMemberId, Optional.of(instanceId)));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);

        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data.groupId());
            assertEquals(pid, txnOffsetCommitRequest.data.producerId());
            assertEquals(epoch, txnOffsetCommitRequest.data.producerEpoch());
            return txnOffsetCommitRequest.data.groupInstanceId().equals(instanceId)
                && !txnOffsetCommitRequest.data.memberId().equals(memberId);
        }, new TxnOffsetCommitResponse(0, singletonMap(tp, Errors.FENCED_INSTANCE_ID)));

        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof FencedInstanceIdException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof FencedInstanceIdException);
        assertAbortableError(FencedInstanceIdException.class);
    }

    @Test
    public void testUnknownMemberIdInTxnOffsetCommitByGroupMetadata() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);
        final String memberId = "member";
        final String unknownMemberId = "unknownMember";

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)),
            new ConsumerGroupMetadata(consumerGroupId, 5, unknownMemberId, Optional.empty()));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);

        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data.groupId());
            assertEquals(pid, txnOffsetCommitRequest.data.producerId());
            assertEquals(epoch, txnOffsetCommitRequest.data.producerEpoch());
            return !txnOffsetCommitRequest.data.memberId().equals(memberId);
        }, new TxnOffsetCommitResponse(0, singletonMap(tp, Errors.UNKNOWN_MEMBER_ID)));
        sender.runOnce();  // TxnOffsetCommit Handled

        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof CommitFailedException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof CommitFailedException);
        assertAbortableError(CommitFailedException.class);
    }

    @Test
    public void testIllegalGenerationInTxnOffsetCommitByGroupMetadata() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);
        final int generationId = 5;
        final int illegalGenerationId = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)),
            new ConsumerGroupMetadata(consumerGroupId, illegalGenerationId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
                Optional.empty()));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);

        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp, Errors.ILLEGAL_GENERATION));
        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data.groupId());
            assertEquals(pid, txnOffsetCommitRequest.data.producerId());
            assertEquals(epoch, txnOffsetCommitRequest.data.producerEpoch());
            return txnOffsetCommitRequest.data.generationId() != generationId;
        }, new TxnOffsetCommitResponse(0, singletonMap(tp, Errors.ILLEGAL_GENERATION)));
        sender.runOnce();  // TxnOffsetCommit Handled

        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof CommitFailedException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof CommitFailedException);
        assertAbortableError(CommitFailedException.class);
    }

    @Test
    public void testLookupCoordinatorOnDisconnectAfterSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, true, pid, epoch);
        // send pid to coordinator, should get disconnected before receiving the response, and resend the
        // FindCoordinator and InitPid requests.
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        assertNull(transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);

        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        runUntil(initPidResult::isCompleted);

        assertTrue(initPidResult.isCompleted()); // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasProducerId());
        assertEquals(pid, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testLookupCoordinatorOnDisconnectBeforeSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        client.disconnect(brokerNode.idString());
        client.blackout(brokerNode, 100);
        // send pid to coordinator. Should get disconnected before the send and resend the FindCoordinator
        // and InitPid requests.
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);
        time.sleep(110);  // waiting for the blackout period for the node to expire.

        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        runUntil(initPidResult::isCompleted);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(pid, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testLookupCoordinatorOnNotCoordinatorError() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        final long pid = 13131L;
        final short epoch = 1;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NOT_COORDINATOR, false, pid, epoch);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, pid, epoch);

        runUntil(initPidResult::isCompleted);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(pid, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInFindCoordinator() {
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, false,
                CoordinatorType.TRANSACTION, transactionalId);

        runUntil(transactionManager::hasError);

        assertTrue(transactionManager.hasFatalError());
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);
        assertFalse(initPidResult.isSuccessful());
        assertTrue(initPidResult.error() instanceof TransactionalIdAuthorizationException);
        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInInitProducerId() {
        final long pid = 13131L;
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, false, pid, RecordBatch.NO_PRODUCER_EPOCH);
        runUntil(transactionManager::hasError);
        assertTrue(initPidResult.isCompleted());
        assertFalse(initPidResult.isSuccessful());
        assertTrue(initPidResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testGroupAuthorizationFailureInFindCoordinator() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof GroupAuthorizationException);

        runUntil(sendOffsetsResult::isCompleted);
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof GroupAuthorizationException);

        GroupAuthorizationException exception = (GroupAuthorizationException) sendOffsetsResult.error();
        assertEquals(consumerGroupId, exception.groupId());

        assertAbortableError(GroupAuthorizationException.class);
    }

    @Test
    public void testGroupAuthorizationFailureInTxnOffsetCommit() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp1 = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp1, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp1, Errors.GROUP_AUTHORIZATION_FAILED));

        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof GroupAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof GroupAuthorizationException);
        assertFalse(transactionManager.hasPendingOffsetCommits());

        GroupAuthorizationException exception = (GroupAuthorizationException) sendOffsetsResult.error();
        assertEquals(consumerGroupId, exception.groupId());

        assertAbortableError(GroupAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInAddOffsetsToTxn() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, consumerGroupId, pid, epoch);
        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInTxnOffsetCommit() {
        final String consumerGroupId = "consumer";
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, singletonMap(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        runUntil(transactionManager::hasError);

        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertTrue(sendOffsetsResult.error() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTopicAuthorizationFailureInAddPartitions() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp1);

        FutureRecordMetadata firstPartitionAppend = appendToAccumulator(tp0);
        FutureRecordMetadata secondPartitionAppend = appendToAccumulator(tp1);

        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        errors.put(tp1, Errors.OPERATION_NOT_ATTEMPTED);

        prepareAddPartitionsToTxn(errors);
        runUntil(transactionManager::hasError);

        assertTrue(transactionManager.lastError() instanceof TopicAuthorizationException);
        assertFalse(transactionManager.isPartitionPendingAdd(tp0));
        assertFalse(transactionManager.isPartitionPendingAdd(tp1));
        assertFalse(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.isPartitionAdded(tp1));
        assertFalse(transactionManager.hasPartitionsToAdd());

        TopicAuthorizationException exception = (TopicAuthorizationException) transactionManager.lastError();
        assertEquals(singleton(tp0.topic()), exception.unauthorizedTopics());
        assertAbortableError(TopicAuthorizationException.class);
        sender.runOnce();

        TestUtils.assertFutureThrows(firstPartitionAppend, KafkaException.class);
        TestUtils.assertFutureThrows(secondPartitionAppend, KafkaException.class);
    }

    @Test
    public void testCommitWithTopicAuthorizationFailureInAddPartitionsInFlight() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);

        doInitTransactions(pid, epoch);

        // Begin a transaction, send two records, and begin commit
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.maybeAddPartitionToTransaction(tp1);
        FutureRecordMetadata firstPartitionAppend = appendToAccumulator(tp0);
        FutureRecordMetadata secondPartitionAppend = appendToAccumulator(tp1);
        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // We send the AddPartitionsToTxn request in the first sender call
        sender.runOnce();
        assertFalse(transactionManager.hasError());
        assertFalse(commitResult.isCompleted());
        assertFalse(firstPartitionAppend.isDone());

        // The AddPartitionsToTxn response returns in the next call with the error
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        errors.put(tp1, Errors.OPERATION_NOT_ATTEMPTED);
        client.respond(body -> {
            AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) body;
            assertEquals(new HashSet<>(request.partitions()), new HashSet<>(errors.keySet()));
            return true;
        }, new AddPartitionsToTxnResponse(0, errors));

        sender.runOnce();
        assertTrue(transactionManager.hasError());
        assertFalse(commitResult.isCompleted());
        assertFalse(firstPartitionAppend.isDone());
        assertFalse(secondPartitionAppend.isDone());

        // The next call aborts the records, which have not yet been sent. It should
        // not block because there are no requests pending and we still need to cancel
        // the pending transaction commit.
        sender.runOnce();
        assertTrue(commitResult.isCompleted());
        TestUtils.assertFutureThrows(firstPartitionAppend, KafkaException.class);
        TestUtils.assertFutureThrows(secondPartitionAppend, KafkaException.class);
        assertTrue(commitResult.error() instanceof TopicAuthorizationException);
    }

    @Test
    public void testRecoveryFromAbortableErrorTransactionNotStarted() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(unauthorizedPartition);

        Future<RecordMetadata> responseFuture = appendToAccumulator(unauthorizedPartition);

        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        runUntil(() -> !client.hasPendingResponses());

        assertTrue(transactionManager.hasAbortableError());
        transactionManager.beginAbort();
        runUntil(responseFuture::isDone);
        assertFutureFailed(responseFuture);

        // No partitions added, so no need to prepare EndTxn response
        runUntil(transactionManager::isReady);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        runUntil(() -> transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(responseFuture::isDone);
        assertNotNull(responseFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(transactionManager::isReady);
    }

    @Test
    public void testRecoveryFromAbortableErrorTransactionStarted() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);

        Future<RecordMetadata> authorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition);
        runUntil(() -> transactionManager.isPartitionAdded(tp0));

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(unauthorizedPartition);
        Future<RecordMetadata> unauthorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition);
        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        runUntil(transactionManager::hasAbortableError);
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.isPartitionAdded(unauthorizedPartition));
        assertFalse(authorizedTopicProduceFuture.isDone());
        assertFalse(unauthorizedTopicProduceFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        transactionManager.beginAbort();
        runUntil(transactionManager::isReady);
        // neither produce request has been sent, so they should both be failed immediately
        assertFutureFailed(authorizedTopicProduceFuture);
        assertFutureFailed(unauthorizedTopicProduceFuture);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        FutureRecordMetadata nextTransactionFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        runUntil(() -> transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(nextTransactionFuture::isDone);
        assertNotNull(nextTransactionFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(transactionManager::isReady);
    }

    @Test
    public void testRecoveryFromAbortableErrorProduceRequestInRetry() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);

        Future<RecordMetadata> authorizedTopicProduceFuture = appendToAccumulator(tp0);
        runUntil(() -> transactionManager.isPartitionAdded(tp0));

        accumulator.beginFlush();
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, pid, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(authorizedTopicProduceFuture.isDone());
        assertTrue(accumulator.hasIncomplete());

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(unauthorizedPartition);
        Future<RecordMetadata> unauthorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition);
        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        runUntil(transactionManager::hasAbortableError);
        assertTrue(transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.isPartitionAdded(unauthorizedPartition));
        assertFalse(authorizedTopicProduceFuture.isDone());

        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(authorizedTopicProduceFuture::isDone);

        assertFutureFailed(unauthorizedTopicProduceFuture);
        assertNotNull(authorizedTopicProduceFuture.get());
        assertTrue(authorizedTopicProduceFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        transactionManager.beginAbort();
        runUntil(transactionManager::isReady);
        // neither produce request has been sent, so they should both be failed immediately
        assertTrue(transactionManager.isReady());
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        FutureRecordMetadata nextTransactionFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        runUntil(() -> transactionManager.isPartitionAdded(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(nextTransactionFuture::isDone);
        assertNotNull(nextTransactionFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(transactionManager::isReady);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInAddPartitions() {
        final long pid = 13131L;
        final short epoch = 1;
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp);

        prepareAddPartitionsToTxn(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        runUntil(transactionManager::hasError);
        assertTrue(transactionManager.lastError() instanceof TransactionalIdAuthorizationException);

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testFlushPendingPartitionsOnCommit() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // we have an append, an add partitions request, and now also an endtxn.
        // The order should be:
        //  1. Add Partitions
        //  2. Produce
        //  3. EndTxn.
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertFalse(responseFuture.isDone());
        assertFalse(commitResult.isCompleted());

        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(responseFuture::isDone);

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        assertFalse(commitResult.isCompleted());
        assertTrue(transactionManager.hasOngoingTransaction());
        assertTrue(transactionManager.isCompleting());

        runUntil(commitResult::isCompleted);
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testMultipleAddPartitionsPerForOneProduce() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        // User does one producer.send
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));

        // Sender flushes one add partitions. The produce goes next.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        // In the mean time, the user does a second produce to a different partition
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp1);
        Future<RecordMetadata> secondResponseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp1, epoch, pid);
        prepareProduceResponse(Errors.NONE, pid, epoch);

        assertFalse(transactionManager.transactionContainsPartition(tp1));

        assertFalse(responseFuture.isDone());
        assertFalse(secondResponseFuture.isDone());

        // The second add partitions should go out here.
        runUntil(() -> transactionManager.transactionContainsPartition(tp1));

        assertFalse(responseFuture.isDone());
        assertFalse(secondResponseFuture.isDone());

        // Finally we get to the produce.
        runUntil(responseFuture::isDone);
        assertTrue(secondResponseFuture.isDone());
    }

    @Test
    public void testProducerFencedException() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.INVALID_PRODUCER_EPOCH, pid, epoch);

        runUntil(responseFuture::isDone);
        assertTrue(transactionManager.hasError());

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a ExecutionException from the response");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ProducerFencedException);
        }

        // make sure the exception was thrown directly from the follow-up calls.
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginTransaction());
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginCommit());
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginAbort());
        assertThrows(ProducerFencedException.class, () -> transactionManager.sendOffsetsToTransaction(
            Collections.emptyMap(), new ConsumerGroupMetadata("dummyId")));
    }

    @Test
    public void testDisallowCommitOnProduceFailure() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        TransactionalRequestResult commitResult = transactionManager.beginCommit();
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);

        runUntil(commitResult::isCompleted);  // commit should be cancelled with exception without being sent.

        try {
            commitResult.await();
            fail();  // the get() must throw an exception.
        } catch (KafkaException e) {
            // Expected
        }

        try {
            responseFuture.get();
            fail("Expected produce future to raise an exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof OutOfOrderSequenceException);
        }

        // Commit is not allowed, so let's abort and try again.
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testAllowAbortOnProduceFailure() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);

        runUntil(transactionManager::hasAbortableError);
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testAbortableErrorWhileAbortInProgress() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        runUntil(() -> !accumulator.hasUndrained());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        assertTrue(transactionManager.isAborting());
        assertFalse(transactionManager.hasError());

        sendProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, pid, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        runUntil(responseFuture::isDone);

        // we do not transition to ABORTABLE_ERROR since we were already aborting
        assertTrue(transactionManager.isAborting());
        assertFalse(transactionManager.hasError());

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testCommitTransactionWithUnsentProduceRequest() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> !client.hasPendingResponses());
        assertTrue(accumulator.hasUndrained());

        // committing the transaction should cause the unsent batch to be flushed
        transactionManager.beginCommit();
        runUntil(() -> !accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());
        assertFalse(responseFuture.isDone());

        // until the produce future returns, we will not send EndTxn
        AtomicInteger numRuns = new AtomicInteger(0);
        runUntil(() -> numRuns.incrementAndGet() >= 4);
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());
        assertFalse(responseFuture.isDone());

        // now the produce response returns
        sendProduceResponse(Errors.NONE, pid, epoch);
        runUntil(responseFuture::isDone);
        assertFalse(accumulator.hasUndrained());
        assertFalse(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());

        // now we send EndTxn
        runUntil(transactionManager::hasInFlightRequest);
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);

        runUntil(transactionManager::isReady);
        assertFalse(transactionManager.hasInFlightRequest());
    }

    @Test
    public void testCommitTransactionWithInFlightProduceRequest() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());
        assertTrue(accumulator.hasUndrained());

        accumulator.beginFlush();
        runUntil(() -> !accumulator.hasUndrained());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());

        // now we begin the commit with the produce request still pending
        transactionManager.beginCommit();
        AtomicInteger numRuns = new AtomicInteger(0);
        runUntil(() -> numRuns.incrementAndGet() >= 4);
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());
        assertFalse(responseFuture.isDone());

        // now the produce response returns
        sendProduceResponse(Errors.NONE, pid, epoch);
        runUntil(responseFuture::isDone);
        assertFalse(accumulator.hasUndrained());
        assertFalse(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());

        // now we send EndTxn
        runUntil(transactionManager::hasInFlightRequest);
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, pid, epoch);
        runUntil(transactionManager::isReady);
        assertFalse(transactionManager.hasInFlightRequest());
    }

    @Test
    public void testFindCoordinatorAllowedInAbortableErrorState() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        runUntil(transactionManager::hasInFlightRequest);

        transactionManager.transitionToAbortableError(new KafkaException());
        sendAddPartitionsToTxnResponse(Errors.NOT_COORDINATOR, tp0, epoch, pid);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testCancelUnsentAddPartitionsAndProduceOnAbort() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        // note since no partitions were added to the transaction, no EndTxn will be sent

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        try {
            responseFuture.get();
            fail("Expected produce future to raise an exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof KafkaException);
        }
    }

    @Test
    public void testAbortResendsAddPartitionErrorIfRetried() throws InterruptedException {
        final long producerId = 13131L;
        final short producerEpoch = 1;

        doInitTransactions(producerId, producerEpoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, producerEpoch, producerId);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        runUntil(() -> !client.hasPendingResponses());
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        // we should resend the AddPartitions
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, producerEpoch, producerId);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, producerEpoch);

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        try {
            responseFuture.get();
            fail("Expected produce future to raise an exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof KafkaException);
        }
    }

    @Test
    public void testAbortResendsProduceRequestIfRetried() throws Exception {
        final long producerId = 13131L;
        final short producerEpoch = 1;

        doInitTransactions(producerId, producerEpoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, producerEpoch, producerId);
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, producerId, producerEpoch);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        runUntil(() -> !client.hasPendingResponses());
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        // we should resend the ProduceRequest before aborting
        prepareProduceResponse(Errors.NONE, producerId, producerEpoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, producerEpoch);

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        RecordMetadata recordMetadata = responseFuture.get();
        assertEquals(tp0.topic(), recordMetadata.topic());
    }

    @Test
    public void testHandlingOfUnknownTopicPartitionErrorOnAddPartitions() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, epoch, pid);

        runUntil(() -> !client.hasPendingResponses());
        assertFalse(transactionManager.transactionContainsPartition(tp0));  // The partition should not yet be added.

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        runUntil(responseFuture::isDone);
    }

    @Test
    public void testHandlingOfUnknownTopicPartitionErrorOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testHandlingOfCoordinatorLoadingErrorOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.COORDINATOR_LOAD_IN_PROGRESS);
    }

    private void testRetriableErrorInTxnOffsetCommit(Errors error) {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(1));
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId));
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(addOffsetsResult.isCompleted());  // The request should complete only after the TxnOffsetCommit completes.

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp0, Errors.NONE);
        txnOffsetCommitResponse.put(tp1, error);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, txnOffsetCommitResponse);

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);
        assertTrue(transactionManager.hasPendingOffsetCommits());

        runUntil(transactionManager::hasPendingOffsetCommits);  // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete successfully.

        txnOffsetCommitResponse.put(tp1, Errors.NONE);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, txnOffsetCommitResponse);
        runUntil(addOffsetsResult::isCompleted);
        assertTrue(addOffsetsResult.isSuccessful());
    }

    @Test
    public void shouldNotAddPartitionsToTransactionWhenTopicAuthorizationFailed() throws Exception {
        verifyAddPartitionsFailsWithPartitionLevelError(Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test
    public void shouldNotSendAbortTxnRequestWhenOnlyAddPartitionsRequestFailed() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        prepareAddPartitionsToTxnResponse(Errors.TOPIC_AUTHORIZATION_FAILED, tp0, epoch, pid);
        runUntil(() -> !client.hasPendingResponses());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        assertFalse(abortResult.isCompleted());

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void shouldNotSendAbortTxnRequestWhenOnlyAddOffsetsRequestFailed() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";

        transactionManager.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareAddOffsetsToTxnResponse(Errors.GROUP_AUTHORIZATION_FAILED, consumerGroupId, pid, epoch);
        runUntil(abortResult::isCompleted);
        assertTrue(transactionManager.isReady());
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void shouldFailAbortIfAddOffsetsFailsWithFatalError() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";

        transactionManager.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareAddOffsetsToTxnResponse(Errors.UNKNOWN_SERVER_ERROR, consumerGroupId, pid, epoch);

        runUntil(abortResult::isCompleted);
        assertFalse(abortResult.isSuccessful());
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testSendOffsetsWithGroupMetadata() {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(1));
        offsets.put(tp1, new OffsetAndMetadata(1));
        final String consumerGroupId = "myconsumergroup";
        final String groupInstanceId = "instance";
        final String memberId = "member";
        final int generationId = 5;

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId, generationId, memberId, Optional.of(groupInstanceId)));
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, pid, epoch);

        sender.runOnce();  // send AddOffsetsToTxnResult

        assertFalse(addOffsetsResult.isCompleted());  // The request should complete only after the TxnOffsetCommit completes.

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp0, Errors.NONE);
        txnOffsetCommitResponse.put(tp1, Errors.COORDINATOR_LOAD_IN_PROGRESS);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, groupInstanceId, memberId, generationId, txnOffsetCommitResponse);

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        sender.runOnce();  // try to send TxnOffsetCommitRequest, but find we don't have a group coordinator.
        sender.runOnce();  // send find coordinator for group request
        assertNotNull(transactionManager.coordinator(CoordinatorType.GROUP));
        assertTrue(transactionManager.hasPendingOffsetCommits());

        sender.runOnce();  // send TxnOffsetCommitRequest request.

        assertTrue(transactionManager.hasPendingOffsetCommits());  // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete successfully.

        txnOffsetCommitResponse.put(tp1, Errors.NONE);
        prepareTxnOffsetCommitResponse(consumerGroupId, pid, epoch, groupInstanceId, memberId, generationId, txnOffsetCommitResponse);
        sender.runOnce();  // Send TxnOffsetCommitRequest again.

        assertTrue(addOffsetsResult.isCompleted());
        assertTrue(addOffsetsResult.isSuccessful());
    }

    @Test
    public void testNoDrainWhenPartitionsPending() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        appendToAccumulator(tp0);
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp1);
        appendToAccumulator(tp1);

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp1));

        Node node1 = new Node(0, "localhost", 1111);
        Node node2 = new Node(1, "localhost", 1112);
        PartitionInfo part1 = new PartitionInfo(topic, 0, node1, null, null);
        PartitionInfo part2 = new PartitionInfo(topic, 1, node2, null, null);

        Cluster cluster = new Cluster(null, Arrays.asList(node1, node2), Arrays.asList(part1, part2),
                Collections.emptySet(), Collections.emptySet());
        Set<Node> nodes = new HashSet<>();
        nodes.add(node1);
        nodes.add(node2);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(cluster, nodes, Integer.MAX_VALUE,
                time.milliseconds());

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());
        assertTrue(drainedBatches.containsKey(node2.id()));
        assertTrue(drainedBatches.get(node2.id()).isEmpty());
        assertFalse(transactionManager.hasError());
    }

    @Test
    public void testAllowDrainInAbortableErrorState() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp1);
        prepareAddPartitionsToTxn(tp1, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(tp1));

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        runUntil(transactionManager::hasAbortableError);
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));

        // Try to drain a message destined for tp1, it should get drained.
        Node node1 = new Node(1, "localhost", 1112);
        PartitionInfo part1 = new PartitionInfo(topic, 1, node1, null, null);
        Cluster cluster = new Cluster(null, Collections.singletonList(node1), Collections.singletonList(part1),
                Collections.emptySet(), Collections.emptySet());
        appendToAccumulator(tp1);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(cluster, Collections.singleton(node1),
                Integer.MAX_VALUE,
                time.milliseconds());

        // We should drain the appended record since we are in abortable state and the partition has already been
        // added to the transaction.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertEquals(1, drainedBatches.get(node1.id()).size());
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testRaiseErrorWhenNoPartitionsPendingOnDrain() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();
        // Don't execute transactionManager.maybeAddPartitionToTransaction(tp0). This should result in an error on drain.
        appendToAccumulator(tp0);
        Node node1 = new Node(0, "localhost", 1111);
        PartitionInfo part1 = new PartitionInfo(topic, 0, node1, null, null);

        Cluster cluster = new Cluster(null, Collections.singletonList(node1), Collections.singletonList(part1),
                Collections.emptySet(), Collections.emptySet());
        Set<Node> nodes = new HashSet<>();
        nodes.add(node1);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(cluster, nodes, Integer.MAX_VALUE,
                time.milliseconds());

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());
    }

    @Test
    public void resendFailedProduceRequestAfterAbortableError() throws Exception {
        final long pid = 13131L;
        final short epoch = 1;
        doInitTransactions(pid, epoch);
        transactionManager.beginTransaction();

        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.NOT_LEADER_FOR_PARTITION, pid, epoch);
        runUntil(() -> !client.hasPendingResponses());

        assertFalse(responseFuture.isDone());

        transactionManager.transitionToAbortableError(new KafkaException());
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(responseFuture::isDone);
        assertNotNull(responseFuture.get()); // should throw the exception which caused the transaction to be aborted.
    }

    @Test
    public void testTransitionToAbortableErrorOnBatchExpiry() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        runUntil(responseFuture::isDone);

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testTransitionToAbortableErrorOnMultipleBatchExpiry() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp1);

        Future<RecordMetadata> firstBatchResponse = appendToAccumulator(tp0);
        Future<RecordMetadata> secondBatchResponse = appendToAccumulator(tp1);

        assertFalse(firstBatchResponse.isDone());
        assertFalse(secondBatchResponse.isDone());

        Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
        partitionErrors.put(tp0, Errors.NONE);
        partitionErrors.put(tp1, Errors.NONE);
        prepareAddPartitionsToTxn(partitionErrors);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.transactionContainsPartition(tp1));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));
        assertFalse(firstBatchResponse.isDone());
        assertFalse(secondBatchResponse.isDone());

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        runUntil(firstBatchResponse::isDone);
        runUntil(secondBatchResponse::isDone);

        try {
            // make sure the produce was expired.
            firstBatchResponse.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }

        try {
            // make sure the produce was expired.
            secondBatchResponse.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testDropCommitOnBatchExpiry() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());

        runUntil(responseFuture::isDone);  // We should try to flush the produce, but expire it instead without sending anything.

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        runUntil(commitResult::isCompleted);  // the commit shouldn't be completed without being sent since the produce request failed.
        assertFalse(commitResult.isSuccessful());  // the commit shouldn't succeed since the produce request failed.

        assertTrue(transactionManager.hasAbortableError());
        assertTrue(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
        assertTrue(transactionManager.transactionContainsPartition(tp0));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, pid, epoch);
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertFalse(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testTransitionToFatalErrorWhenRetriedBatchIsExpired() throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));

        prepareProduceResponse(Errors.NOT_LEADER_FOR_PARTITION, pid, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.blackout(clusterNode, 100);

        runUntil(responseFuture::isDone);  // We should try to flush the produce, but expire it instead without sending anything.

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof  TimeoutException);
        }
        runUntil(commitResult::isCompleted);
        assertFalse(commitResult.isSuccessful());  // the commit should have been dropped.

        assertTrue(transactionManager.hasFatalError());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testResetProducerIdAfterWithoutPendingInflightRequests() {
        initializeTransactionManager(Optional.empty());
        long producerId = 15L;
        short epoch = 5;
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, epoch);
        initializeIdempotentProducerId(producerId, epoch);

        // Nothing to resolve, so no reset is needed
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());

        TopicPartition tp0 = new TopicPartition("foo", 0);
        assertEquals(Integer.valueOf(0), transactionManager.sequenceNumber(tp0));

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        assertEquals(Integer.valueOf(1), transactionManager.sequenceNumber(tp0));
        transactionManager.handleCompletedBatch(b1, new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        // Marking sequence numbers unresolved without inflight requests is basically a no-op.
        transactionManager.markSequenceUnresolved(tp0);
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertFalse(transactionManager.hasUnresolvedSequences());

        // We have a new batch which fails with a timeout
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        assertEquals(Integer.valueOf(2), transactionManager.sequenceNumber(tp0));
        transactionManager.markSequenceUnresolved(tp0);
        transactionManager.handleFailedBatch(b2, new TimeoutException(), false);
        assertTrue(transactionManager.hasUnresolvedSequences());

        // We only had one inflight batch, so we should be able to clear the unresolved status
        // and reset the producerId
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertFalse(transactionManager.hasUnresolvedSequences());
        assertFalse(transactionManager.hasProducerId());
    }

    @Test
    public void testNoProducerIdResetAfterLastInFlightBatchSucceeds() {
        initializeTransactionManager(Optional.empty());
        long producerId = 15L;
        short epoch = 5;
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, epoch);
        initializeIdempotentProducerId(producerId, epoch);

        TopicPartition tp0 = new TopicPartition("foo", 0);
        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        assertEquals(3, transactionManager.sequenceNumber(tp0).intValue());

        // The first batch fails with a timeout
        transactionManager.markSequenceUnresolved(tp0);
        transactionManager.handleFailedBatch(b1, new TimeoutException(), false);
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The reset should not occur until sequence numbers have been resolved
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The second batch fails as well with a timeout
        transactionManager.handleFailedBatch(b2, new TimeoutException(), false);
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The third batch succeeds, which should resolve the sequence number without
        // requiring a producerId reset.
        transactionManager.handleCompletedBatch(b3, new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L));
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertFalse(transactionManager.hasUnresolvedSequences());
        assertEquals(3, transactionManager.sequenceNumber(tp0).intValue());
    }

    @Test
    public void testProducerIdResetAfterLastInFlightBatchFails() {
        initializeTransactionManager(Optional.empty());
        long producerId = 15L;
        short epoch = 5;
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, epoch);
        initializeIdempotentProducerId(producerId, epoch);

        TopicPartition tp0 = new TopicPartition("foo", 0);
        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        assertEquals(Integer.valueOf(3), transactionManager.sequenceNumber(tp0));

        // The first batch fails with a timeout
        transactionManager.markSequenceUnresolved(tp0);
        transactionManager.handleFailedBatch(b1, new TimeoutException(), false);
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The second batch succeeds, but sequence numbers are still not resolved
        transactionManager.handleCompletedBatch(b2, new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L));
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertTrue(transactionManager.hasUnresolvedSequences());

        // When the last inflight batch fails, we have to reset the producerId
        transactionManager.handleFailedBatch(b3, new TimeoutException(), false);
        transactionManager.resetIdempotentProducerIdIfNeeded();
        assertFalse(transactionManager.hasProducerId());
        assertFalse(transactionManager.hasUnresolvedSequences());
        assertEquals(0, transactionManager.sequenceNumber(tp0).intValue());
    }

    @Test
    public void testRetryAbortTransaction() throws InterruptedException {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.ABORT, TransactionResult.ABORT);
    }

    @Test
    public void testRetryCommitTransaction() throws InterruptedException {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.COMMIT, TransactionResult.COMMIT);
    }

    @Test(expected = KafkaException.class)
    public void testRetryAbortTransactionAfterCommitTimeout() throws InterruptedException {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.COMMIT, TransactionResult.ABORT);
    }

    @Test(expected = KafkaException.class)
    public void testRetryCommitTransactionAfterAbortTimeout() throws InterruptedException {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.ABORT, TransactionResult.COMMIT);
    }

    private FutureRecordMetadata appendToAccumulator(TopicPartition tp) throws InterruptedException {
        final long nowMs = time.milliseconds();
        return accumulator.append(tp, nowMs, "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS,
                null, MAX_BLOCK_TIMEOUT, false, nowMs).future;
    }

    private void verifyCommitOrAbortTransactionRetriable(TransactionResult firstTransactionResult,
                                                         TransactionResult retryTransactionResult)
            throws InterruptedException {
        final long pid = 13131L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        appendToAccumulator(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, pid);
        prepareProduceResponse(Errors.NONE, pid, epoch);
        runUntil(() -> !client.hasPendingResponses());

        TransactionalRequestResult result = firstTransactionResult == TransactionResult.COMMIT ?
                transactionManager.beginCommit() : transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, firstTransactionResult, pid, epoch, true);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(result.isCompleted());

        try {
            result.await(MAX_BLOCK_TIMEOUT, TimeUnit.MILLISECONDS);
            fail("Should have raised TimeoutException");
        } catch (TimeoutException e) {
        }

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> !client.hasPendingResponses());
        TransactionalRequestResult retryResult = retryTransactionResult == TransactionResult.COMMIT ?
                transactionManager.beginCommit() : transactionManager.beginAbort();
        assertEquals(retryResult, result); // check if cached result is reused.

        prepareEndTxnResponse(Errors.NONE, retryTransactionResult, pid, epoch, false);
        runUntil(retryResult::isCompleted);
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    private void verifyAddPartitionsFailsWithPartitionLevelError(final Errors error) throws InterruptedException {
        final long pid = 1L;
        final short epoch = 1;

        doInitTransactions(pid, epoch);

        transactionManager.beginTransaction();
        transactionManager.failIfNotReadyForSend();
        transactionManager.maybeAddPartitionToTransaction(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxn(tp0, error);
        runUntil(transactionManager::hasError);
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    private void prepareAddPartitionsToTxn(final Map<TopicPartition, Errors> errors) {
        client.prepareResponse(body -> {
            AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) body;
            assertEquals(new HashSet<>(request.partitions()), new HashSet<>(errors.keySet()));
            return true;
        }, new AddPartitionsToTxnResponse(0, errors));
    }

    private void prepareAddPartitionsToTxn(final TopicPartition tp, final Errors error) {
        prepareAddPartitionsToTxn(Collections.singletonMap(tp, error));
    }

    private void prepareFindCoordinatorResponse(Errors error, boolean shouldDisconnect,
                                                final CoordinatorType coordinatorType,
                                                final String coordinatorKey) {
        client.prepareResponse(body -> {
            FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
            assertEquals(CoordinatorType.forId(findCoordinatorRequest.data().keyType()), coordinatorType);
            assertEquals(findCoordinatorRequest.data().key(), coordinatorKey);
            return true;
        }, FindCoordinatorResponse.prepareResponse(error, brokerNode), shouldDisconnect);
    }

    private void prepareInitPidResponse(Errors error, boolean shouldDisconnect, long producerId, short producerEpoch) {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(error.code())
                .setProducerEpoch(producerEpoch)
                .setProducerId(producerId)
                .setThrottleTimeMs(0);
        client.prepareResponse(body -> {
            InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
            assertEquals(transactionalId, initProducerIdRequest.data.transactionalId());
            assertEquals(transactionTimeoutMs, initProducerIdRequest.data.transactionTimeoutMs());
            return true;
        }, new InitProducerIdResponse(responseData), shouldDisconnect);
    }

    private void sendProduceResponse(Errors error, final long producerId, final short producerEpoch) {
        client.respond(produceRequestMatcher(producerId, producerEpoch), produceResponse(tp0, 0, error, 0));
    }

    private void prepareProduceResponse(Errors error, final long producerId, final short producerEpoch) {
        client.prepareResponse(produceRequestMatcher(producerId, producerEpoch), produceResponse(tp0, 0, error, 0));
    }

    private MockClient.RequestMatcher produceRequestMatcher(final long pid, final short epoch) {
        return body -> {
            ProduceRequest produceRequest = (ProduceRequest) body;
            MemoryRecords records = produceRequest.partitionRecordsOrFail().get(tp0);
            assertNotNull(records);
            Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
            assertTrue(batchIterator.hasNext());
            MutableRecordBatch batch = batchIterator.next();
            assertFalse(batchIterator.hasNext());
            assertTrue(batch.isTransactional());
            assertEquals(pid, batch.producerId());
            assertEquals(epoch, batch.producerEpoch());
            assertEquals(transactionalId, produceRequest.transactionalId());
            return true;
        };
    }

    private void prepareAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition,
                                                   final short epoch, final long pid) {
        client.prepareResponse(addPartitionsRequestMatcher(topicPartition, epoch, pid),
                new AddPartitionsToTxnResponse(0, singletonMap(topicPartition, error)));
    }

    private void sendAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition,
                                                final short epoch, final long pid) {
        client.respond(addPartitionsRequestMatcher(topicPartition, epoch, pid),
                new AddPartitionsToTxnResponse(0, singletonMap(topicPartition, error)));
    }

    private MockClient.RequestMatcher addPartitionsRequestMatcher(final TopicPartition topicPartition,
                                                                  final short epoch, final long pid) {
        return body -> {
            AddPartitionsToTxnRequest addPartitionsToTxnRequest = (AddPartitionsToTxnRequest) body;
            assertEquals(pid, addPartitionsToTxnRequest.producerId());
            assertEquals(epoch, addPartitionsToTxnRequest.producerEpoch());
            assertEquals(singletonList(topicPartition), addPartitionsToTxnRequest.partitions());
            assertEquals(transactionalId, addPartitionsToTxnRequest.transactionalId());
            return true;
        };
    }

    private void prepareEndTxnResponse(Errors error, final TransactionResult result, final long pid, final short epoch) {
        this.prepareEndTxnResponse(error, result, pid, epoch, false);
    }

    private void prepareEndTxnResponse(Errors error,
                                       final TransactionResult result,
                                       final long pid,
                                       final short epoch,
                                       final boolean shouldDisconnect) {
        client.prepareResponse(endTxnMatcher(result, pid, epoch),
                               new EndTxnResponse(new EndTxnResponseData()
                                                      .setErrorCode(error.code())
                                                      .setThrottleTimeMs(0)), shouldDisconnect);
    }

    private void sendEndTxnResponse(Errors error, final TransactionResult result, final long pid, final short epoch) {
        client.respond(endTxnMatcher(result, pid, epoch), new EndTxnResponse(
            new EndTxnResponseData()
                .setErrorCode(error.code())
                .setThrottleTimeMs(0)
        ));
    }

    private MockClient.RequestMatcher endTxnMatcher(final TransactionResult result, final long pid, final short epoch) {
        return body -> {
            EndTxnRequest endTxnRequest = (EndTxnRequest) body;
            assertEquals(transactionalId, endTxnRequest.data.transactionalId());
            assertEquals(pid, endTxnRequest.data.producerId());
            assertEquals(epoch, endTxnRequest.data.producerEpoch());
            assertEquals(result, endTxnRequest.result());
            return true;
        };
    }

    private void prepareAddOffsetsToTxnResponse(final Errors error,
                                                final String consumerGroupId,
                                                final long producerId,
                                                final short producerEpoch) {
        client.prepareResponse(body -> {
            AddOffsetsToTxnRequest addOffsetsToTxnRequest = (AddOffsetsToTxnRequest) body;
            assertEquals(consumerGroupId, addOffsetsToTxnRequest.consumerGroupId());
            assertEquals(transactionalId, addOffsetsToTxnRequest.transactionalId());
            assertEquals(producerId, addOffsetsToTxnRequest.producerId());
            assertEquals(producerEpoch, addOffsetsToTxnRequest.producerEpoch());
            return true;
        }, new AddOffsetsToTxnResponse(0, error));
    }

    private void prepareTxnOffsetCommitResponse(final String consumerGroupId,
                                                final long producerId,
                                                final short producerEpoch,
                                                Map<TopicPartition, Errors> txnOffsetCommitResponse) {
        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data.groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data.producerId());
            assertEquals(producerEpoch, txnOffsetCommitRequest.data.producerEpoch());
            return true;
        }, new TxnOffsetCommitResponse(0, txnOffsetCommitResponse));
    }

    private void prepareTxnOffsetCommitResponse(final String consumerGroupId,
                                                final long producerId,
                                                final short producerEpoch,
                                                final String groupInstanceId,
                                                final String memberId,
                                                final int generationId,
                                                Map<TopicPartition, Errors> txnOffsetCommitResponse) {
        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data.groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data.producerId());
            assertEquals(producerEpoch, txnOffsetCommitRequest.data.producerEpoch());
            assertEquals(groupInstanceId, txnOffsetCommitRequest.data.groupInstanceId());
            assertEquals(memberId, txnOffsetCommitRequest.data.memberId());
            assertEquals(generationId, txnOffsetCommitRequest.data.generationId());
            return true;
        }, new TxnOffsetCommitResponse(0, txnOffsetCommitResponse));
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP, 10);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = singletonMap(tp, resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    private void initializeIdempotentProducerId(long producerId, short epoch) {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerEpoch(epoch)
                .setProducerId(producerId)
                .setThrottleTimeMs(0);
        client.prepareResponse(body -> {
            InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
            assertNull(initProducerIdRequest.data.transactionalId());
            return true;
        }, new InitProducerIdResponse(responseData), false);

        runUntil(transactionManager::hasProducerId);
    }

    private void doInitTransactions(long pid, short epoch) {
        transactionManager.initializeTransactions();
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, pid, epoch);
        runUntil(transactionManager::hasProducerId);
    }

    private void assertAbortableError(Class<? extends RuntimeException> cause) {
        try {
            transactionManager.beginCommit();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }

        assertTrue(transactionManager.hasError());
        transactionManager.beginAbort();
        assertFalse(transactionManager.hasError());
    }

    private void assertFatalError(Class<? extends RuntimeException> cause) {
        assertTrue(transactionManager.hasError());

        try {
            transactionManager.beginAbort();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }

        // Transaction abort cannot clear fatal error state
        try {
            transactionManager.beginAbort();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }
    }

    private void assertFutureFailed(Future<RecordMetadata> future) throws InterruptedException {
        assertTrue(future.isDone());

        try {
            future.get();
            fail("Expected produce future to throw");
        } catch (ExecutionException e) {
            // expected
        }
    }

    private void runUntil(Supplier<Boolean> condition) {
        for (int i = 0; i < 5; i++) {
            if (condition.get())
                break;
            sender.runOnce();
        }
        if (!condition.get())
            throw new AssertionError("Condition was not satisfied after multiple runs");
    }

}
