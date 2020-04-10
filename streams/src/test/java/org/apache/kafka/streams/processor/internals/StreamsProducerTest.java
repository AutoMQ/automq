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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.MockClientSupplier;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StreamsProducerTest {

    private final LogContext logContext = new LogContext("test ");
    private final String topic = "topic";
    private final Cluster cluster = new Cluster(
        "cluster",
        Collections.singletonList(Node.noNode()),
        Collections.singletonList(new PartitionInfo(topic, 0, Node.noNode(), new Node[0], new Node[0])),
        Collections.emptySet(),
        Collections.emptySet()
    );

    private final StreamsConfig nonEosConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"))
    );

    private final StreamsConfig eosAlphaConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE))
    );

    private final StreamsConfig eosBetaConfig = new StreamsConfig(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "appId"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA))
    );

    final Producer<byte[], byte[]> mockedProducer = mock(Producer.class);
    final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
        @Override
        public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
            return mockedProducer;
        }
    };
    final StreamsProducer streamsProducerWithMock = new StreamsProducer(
        nonEosConfig,
        "threadId",
        clientSupplier,
        null,
        null,
        logContext
    );
    final StreamsProducer eosAlphaStreamsProducerWithMock = new StreamsProducer(
        eosAlphaConfig,
        "threadId",
        clientSupplier,
        new TaskId(0, 0),
        null,
        logContext
    );

    private final MockClientSupplier mockClientSupplier = new MockClientSupplier();
    private StreamsProducer nonEosStreamsProducer;
    private MockProducer<byte[], byte[]> nonEosMockProducer;

    private final MockClientSupplier eosAlphaMockClientSupplier = new MockClientSupplier();
    private StreamsProducer eosAlphaStreamsProducer;
    private MockProducer<byte[], byte[]> eosAlphaMockProducer;

    private final MockClientSupplier eosBetaMockClientSupplier = new MockClientSupplier();
    private StreamsProducer eosBetaStreamsProducer;
    private MockProducer<byte[], byte[]> eosBetaMockProducer;

    private final ProducerRecord<byte[], byte[]> record =
        new ProducerRecord<>(topic, 0, 0L, new byte[0], new byte[0], new RecordHeaders());

    private final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = mkMap(
        mkEntry(new TopicPartition(topic, 0), new OffsetAndMetadata(0L, null))
    );



    @Before
    public void before() {
        mockClientSupplier.setCluster(cluster);
        nonEosStreamsProducer =
            new StreamsProducer(
                nonEosConfig,
                "threadId-StreamThread-0",
                mockClientSupplier,
                null,
                null,
                logContext
            );
        nonEosMockProducer = mockClientSupplier.producers.get(0);

        eosAlphaMockClientSupplier.setCluster(cluster);
        eosAlphaMockClientSupplier.setApplicationIdForProducer("appId");
        eosAlphaStreamsProducer =
            new StreamsProducer(
                eosAlphaConfig,
                "threadId-StreamThread-0",
                eosAlphaMockClientSupplier,
                new TaskId(0, 0),
                null,
                logContext
            );
        eosAlphaStreamsProducer.initTransaction();
        eosAlphaMockProducer = eosAlphaMockClientSupplier.producers.get(0);

        eosBetaMockClientSupplier.setCluster(cluster);
        eosBetaMockClientSupplier.setApplicationIdForProducer("appId");
        eosBetaStreamsProducer =
            new StreamsProducer(
                eosBetaConfig,
                "threadId-StreamThread-0",
                eosBetaMockClientSupplier,
                null,
                UUID.randomUUID(),
                logContext
            );
        eosBetaStreamsProducer.initTransaction();
        eosBetaMockProducer = eosBetaMockClientSupplier.producers.get(0);
    }



    // common tests (non-EOS and EOS-alpha/beta)

    // functional tests

    @Test
    public void shouldCreateProducer() {
        assertThat(mockClientSupplier.producers.size(), is(1));
        assertThat(eosAlphaMockClientSupplier.producers.size(), is(1));
    }

    @Test
    public void shouldForwardCallToPartitionsFor() {
        final List<PartitionInfo> expectedPartitionInfo = Collections.emptyList();
        expect(mockedProducer.partitionsFor("topic")).andReturn(expectedPartitionInfo);
        replay(mockedProducer);

        final List<PartitionInfo> partitionInfo = streamsProducerWithMock.partitionsFor(topic);

        assertThat(partitionInfo, sameInstance(expectedPartitionInfo));
        verify(mockedProducer);
    }

    @Test
    public void shouldForwardCallToFlush() {
        mockedProducer.flush();
        expectLastCall();
        replay(mockedProducer);

        streamsProducerWithMock.flush();

        verify(mockedProducer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldForwardCallToMetrics() {
        final Map metrics = new HashMap<>();
        expect(mockedProducer.metrics()).andReturn(metrics);
        replay(mockedProducer);

        assertSame(metrics, streamsProducerWithMock.metrics());

        verify(mockedProducer);
    }

    @Test
    public void shouldForwardCallToClose() {
        mockedProducer.close();
        expectLastCall();
        replay(mockedProducer);

        streamsProducerWithMock.close();

        verify(mockedProducer);
    }

    // error handling tests

    @Test
    public void shouldFailIfStreamsConfigIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                null,
                "threadId",
                mockClientSupplier,
                new TaskId(0, 0),
                UUID.randomUUID(),
                logContext)
        );

        assertThat(thrown.getMessage(), is("config cannot be null"));
    }

    @Test
    public void shouldFailIfThreadIdIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                nonEosConfig,
                null,
                mockClientSupplier,
                new TaskId(0, 0),
                UUID.randomUUID(),
                logContext)
        );

        assertThat(thrown.getMessage(), is("threadId cannot be null"));
    }

    @Test
    public void shouldFailIfClientSupplierIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                nonEosConfig,
                "threadId",
                null,
                new TaskId(0, 0),
                UUID.randomUUID(),
                logContext)
        );

        assertThat(thrown.getMessage(), is("clientSupplier cannot be null"));
    }

    @Test
    public void shouldFailIfLogContextIsNull() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                nonEosConfig,
                "threadId",
                mockClientSupplier,
                new TaskId(0, 0),
                UUID.randomUUID(),
                null)
        );

        assertThat(thrown.getMessage(), is("logContext cannot be null"));
    }

    @Test
    public void shouldFailOnResetProducerForAtLeastOnce() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.resetProducer()
        );

        assertThat(thrown.getMessage(), is("Exactly-once beta is not enabled [test]"));
    }

    @Test
    public void shouldFailOnResetProducerForExactlyOnceAlpha() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> eosAlphaStreamsProducer.resetProducer()
        );

        assertThat(thrown.getMessage(), is("Exactly-once beta is not enabled [test]"));
    }


    // non-EOS tests

    // functional tests

    @Test
    public void shouldNotSetTransactionIdIfEosDisable() {
        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        expect(mockConfig.getProducerConfigs("threadId-producer")).andReturn(mock(Map.class));
        expect(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.AT_LEAST_ONCE).anyTimes();
        replay(mockConfig);

        new StreamsProducer(
            mockConfig,
            "threadId",
            mockClientSupplier,
            null,
            null,
            logContext
        );
    }

    @Test
    public void shouldNotHaveEosEnabledIfEosDisabled() {
        assertThat(nonEosStreamsProducer.eosEnabled(), is(false));
    }

    @Test
    public void shouldNotInitTxIfEosDisable() {
        assertThat(nonEosMockProducer.transactionInitialized(), is(false));
    }

    @Test
    public void shouldNotBeginTxOnSendIfEosDisable() {
        nonEosStreamsProducer.send(record, null);
        assertThat(nonEosMockProducer.transactionInFlight(), is(false));
    }

    @Test
    public void shouldForwardRecordOnSend() {
        nonEosStreamsProducer.send(record, null);
        assertThat(nonEosMockProducer.history().size(), is(1));
        assertThat(nonEosMockProducer.history().get(0), is(record));
    }

    // error handling tests

    @Test
    public void shouldFailOnInitTxIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("Exactly-once is not enabled [test]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnSendError() {
        nonEosMockProducer.sendException  = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(nonEosMockProducer.sendException));
        assertThat(thrown.getMessage(), is("Error encountered trying to send record to topic topic [test]"));
    }

    @Test
    public void shouldFailOnSendFatal() {
        nonEosMockProducer.sendException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> nonEosStreamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldFailOnCommitIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> nonEosStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getMessage(), is("Exactly-once is not enabled [test]"));
    }

    @Test
    public void shouldFailOnAbortIfEosDisabled() {
        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            nonEosStreamsProducer::abortTransaction
        );

        assertThat(thrown.getMessage(), is("Exactly-once is not enabled [test]"));
    }


    // EOS tests (alpha and beta)

    // functional tests

    @Test
    public void shouldEnableEosIfEosAlphaEnabled() {
        assertThat(eosAlphaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldEnableEosIfEosBetaEnabled() {
        assertThat(eosBetaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldSetTransactionIdUsingTaskIdIfEosAlphaEnabled() {
        final Map<String, Object> mockMap = mock(Map.class);
        expect(mockMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "appId-0_0")).andReturn(null);
        expect(mockMap.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).andReturn("appId-0_0");

        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        expect(mockConfig.getProducerConfigs("threadId-0_0-producer")).andReturn(mockMap);
        expect(mockConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("appId");
        expect(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.EXACTLY_ONCE);

        replay(mockMap, mockConfig);

        new StreamsProducer(
            mockConfig,
            "threadId",
            eosAlphaMockClientSupplier,
            new TaskId(0, 0),
            null,
            logContext
        );

        verify(mockMap);
    }

    @Test
    public void shouldSetTransactionIdUsingProcessIdIfEosBetaEnable() {
        final UUID processId = UUID.randomUUID();

        final Map<String, Object> mockMap = mock(Map.class);
        expect(mockMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "appId-" + processId + "-0")).andReturn(null);
        expect(mockMap.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)).andReturn("appId-" + processId);

        final StreamsConfig mockConfig = mock(StreamsConfig.class);
        expect(mockConfig.getProducerConfigs("threadId-StreamThread-0-producer")).andReturn(mockMap);
        expect(mockConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("appId");
        expect(mockConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).andReturn(StreamsConfig.EXACTLY_ONCE_BETA).anyTimes();

        replay(mockMap, mockConfig);

        new StreamsProducer(
            mockConfig,
            "threadId-StreamThread-0",
            eosAlphaMockClientSupplier,
            null,
            processId,
            logContext
        );

        verify(mockMap);
    }

    @Test
    public void shouldNotHaveEosEnabledIfEosAlphaEnable() {
        assertThat(eosAlphaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldHaveEosEnabledIfEosBetaEnabled() {
        assertThat(eosBetaStreamsProducer.eosEnabled(), is(true));
    }

    @Test
    public void shouldInitTxOnEos() {
        assertThat(eosAlphaMockProducer.transactionInitialized(), is(true));
    }

    @Test
    public void shouldBeginTxOnEosSend() {
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
    }

    @Test
    public void shouldContinueTxnSecondEosSend() {
        eosAlphaStreamsProducer.send(record, null);
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedRecords().size(), is(2));
    }

    @Test
    public void shouldForwardRecordButNotCommitOnEosSend() {
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
        assertThat(eosAlphaMockProducer.history().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosAlphaMockProducer.uncommittedRecords().get(0), is(record));
    }

    @Test
    public void shouldBeginTxOnEosCommit() {
        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        mockedProducer.sendOffsetsToTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        eosAlphaStreamsProducerWithMock.initTransaction();

        eosAlphaStreamsProducerWithMock.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer);
    }

    @Test
    public void shouldSendOffsetToTxOnEosCommit() {
        eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));
        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
    }

    @Test
    public void shouldCommitTxOnEosCommit() {
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));

        eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"));

        assertThat(eosAlphaMockProducer.transactionInFlight(), is(false));
        assertThat(eosAlphaMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.history().size(), is(1));
        assertThat(eosAlphaMockProducer.history().get(0), is(record));
        assertThat(eosAlphaMockProducer.consumerGroupOffsetsHistory().size(), is(1));
        assertThat(eosAlphaMockProducer.consumerGroupOffsetsHistory().get(0).get("appId"), is(offsetsAndMetadata));
    }

    @Test
    public void shouldCommitTxWithApplicationIdOnEosAlphaCommit() {
        mockedProducer.initTransactions();
        expectLastCall();
        mockedProducer.beginTransaction();
        expectLastCall();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        eosAlphaStreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosAlphaStreamsProducerWithMock.send(record, null);

        eosAlphaStreamsProducerWithMock.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer);
    }

    @Test
    public void shouldCommitTxWithConsumerGroupMetadataOnEosBetaCommit() {
        mockedProducer.initTransactions();
        expectLastCall();
        mockedProducer.beginTransaction();
        expectLastCall();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.sendOffsetsToTransaction(null, new ConsumerGroupMetadata("appId"));
        expectLastCall();
        mockedProducer.commitTransaction();
        expectLastCall();
        replay(mockedProducer);

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosBetaConfig,
            "threadId-StreamThread-0",
            clientSupplier,
            null,
            UUID.randomUUID(),
            logContext
        );
        streamsProducer.initTransaction();
        // call `send()` to start a transaction
        streamsProducer.send(record, null);

        streamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"));

        verify(mockedProducer);
    }

    @Test
    public void shouldAbortTxOnEosAbort() {
        // call `send()` to start a transaction
        eosAlphaStreamsProducer.send(record, null);
        assertThat(eosAlphaMockProducer.transactionInFlight(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedRecords().size(), is(1));
        assertThat(eosAlphaMockProducer.uncommittedRecords().get(0), is(record));

        eosAlphaStreamsProducer.abortTransaction();

        assertThat(eosAlphaMockProducer.transactionInFlight(), is(false));
        assertThat(eosAlphaMockProducer.uncommittedRecords().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.uncommittedOffsets().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.history().isEmpty(), is(true));
        assertThat(eosAlphaMockProducer.consumerGroupOffsetsHistory().isEmpty(), is(true));
    }

    @Test
    public void shouldSkipAbortTxOnEosAbortIfNotTxInFlight() {
        mockedProducer.initTransactions();
        expectLastCall();
        replay(mockedProducer);

        eosAlphaStreamsProducerWithMock.initTransaction();

        eosAlphaStreamsProducerWithMock.abortTransaction();

        verify(mockedProducer);
    }

    // error handling tests

    @Test
    public void shouldFailIfTaskIdIsNullForEosAlpha() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                eosAlphaConfig,
                "threadId",
                mockClientSupplier,
                null,
                UUID.randomUUID(),
                logContext)
        );

        assertThat(thrown.getMessage(), is("taskId cannot be null for exactly-once alpha"));
    }

    @Test
    public void shouldFailIfProcessIdNullForEosBeta() {
        final NullPointerException thrown = assertThrows(
            NullPointerException.class,
            () -> new StreamsProducer(
                eosBetaConfig,
                "threadId",
                mockClientSupplier,
                new TaskId(0, 0),
                null,
                logContext)
        );

        assertThat(thrown.getMessage(), is("processId cannot be null for exactly-once beta"));
    }

    @Test
    public void shouldThrowTimeoutExceptionOnEosInitTxTimeout() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new TimeoutException("KABOOM!");
        final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return nonEosMockProducer;
            }
        };

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosAlphaConfig,
            "threadId",
            clientSupplier,
            new TaskId(0, 0),
            null,
            logContext
        );

        final TimeoutException thrown = assertThrows(
            TimeoutException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceAlpha() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(
                eosAlphaConfig,
                "threadId",
                eosAlphaMockClientSupplier,
                new TaskId(0, 0),
                null,
                logContext
            );

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> streamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("MockProducer hasn't been initialized for transactions."));
    }

    @Test
    public void shouldFailOnMaybeBeginTransactionIfTransactionsNotInitializedForExactlyOnceBeta() {
        final StreamsProducer streamsProducer =
            new StreamsProducer(
                eosBetaConfig,
                "threadId-StreamThread-0",
                eosBetaMockClientSupplier,
                null,
                UUID.randomUUID(),
                logContext
            );

        final IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> streamsProducer.send(record, null)
        );

        assertThat(thrown.getMessage(), is("MockProducer hasn't been initialized for transactions."));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosInitError() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new KafkaException("KABOOM!");
        final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return nonEosMockProducer;
            }
        };

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosAlphaConfig,
            "threadId",
            clientSupplier,
            new TaskId(0, 0),
            null,
            logContext
        );

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getCause(), is(nonEosMockProducer.initTransactionException));
        assertThat(thrown.getMessage(), is("Error encountered trying to initialize transactions [test]"));
    }

    @Test
    public void shouldFailOnEosInitFatal() {
        // use `nonEosMockProducer` instead of `eosMockProducer` to avoid double Tx-Init
        nonEosMockProducer.initTransactionException = new RuntimeException("KABOOM!");
        final KafkaClientSupplier clientSupplier = new MockClientSupplier() {
            @Override
            public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
                return nonEosMockProducer;
            }
        };

        final StreamsProducer streamsProducer = new StreamsProducer(
            eosAlphaConfig,
            "threadId",
            clientSupplier,
            new TaskId(0, 0),
            null,
            logContext
        );

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            streamsProducer::initTransaction
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnFenced() {
        eosAlphaMockProducer.fenceProducer();

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.send(null, null)
        );

        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to begin a new transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosBeginTxnError() {
        eosAlphaMockProducer.beginTransactionException = new KafkaException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosAlphaStreamsProducer.send(null, null));

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.beginTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to begin a new transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosBeginTxnFatal() {
        eosAlphaMockProducer.beginTransactionException = new RuntimeException("KABOOM!");

        // calling `send()` implicitly starts a new transaction
        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosAlphaStreamsProducer.send(null, null));

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        final ProducerFencedException exception = new ProducerFencedException("KABOOM!");
        // we need to mimic that `send()` always wraps error in a KafkaException
        eosAlphaMockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(exception));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to send a record [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigratedExceptionOnEosSendUnknownPid() {
        final UnknownProducerIdException exception = new UnknownProducerIdException("KABOOM!");
        // we need to mimic that `send()` always wraps error in a KafkaException
        eosAlphaMockProducer.sendException = new KafkaException(exception);

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.send(record, null)
        );

        assertThat(thrown.getCause(), is(exception));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to send a record [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosSendOffsetFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.sendOffsetsToTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosAlphaStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosSendOffsetError() {
        eosAlphaMockProducer.sendOffsetsToTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosAlphaStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.sendOffsetsToTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosSendOffsetFatal() {
        eosAlphaMockProducer.sendOffsetsToTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            // we pass in `null` to verify that `sendOffsetsToTransaction()` fails instead of `commitTransaction()`
            // `sendOffsetsToTransaction()` would throw an NPE on `null` offsets
            () -> eosAlphaStreamsProducer.commitTransaction(null, new ConsumerGroupMetadata("appId"))
        );

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldThrowTaskMigrateExceptionOnEosCommitTxFenced() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.commitTransactionException = new ProducerFencedException("KABOOM!");

        final TaskMigratedException thrown = assertThrows(
            TaskMigratedException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosAlphaMockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Producer got fenced trying to commit a transaction [test];" +
                   " it means all tasks belonging to this thread should be migrated.")
        );
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxTimeout() {
        // cannot use `eosMockProducer.fenceProducer()` because this would already trigger in `beginTransaction()`
        eosAlphaMockProducer.commitTransactionException = new TimeoutException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosAlphaMockProducer.commitTransactionException));
        assertThat(thrown.getMessage(), is("Timed out trying to commit a transaction [test]"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosCommitTxError() {
        eosAlphaMockProducer.commitTransactionException = new KafkaException("KABOOM!");

        final StreamsException thrown = assertThrows(
            StreamsException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getCause(), is(eosAlphaMockProducer.commitTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encountered trying to commit a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosCommitTxFatal() {
        eosAlphaMockProducer.commitTransactionException = new RuntimeException("KABOOM!");

        final RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> eosAlphaStreamsProducer.commitTransaction(offsetsAndMetadata, new ConsumerGroupMetadata("appId"))
        );

        assertThat(eosAlphaMockProducer.sentOffsets(), is(true));
        assertThat(thrown.getMessage(), is("KABOOM!"));
    }

    @Test
    public void shouldSwallowExceptionOnEosAbortTxFenced() {
        mockedProducer.initTransactions();
        mockedProducer.beginTransaction();
        expect(mockedProducer.send(record, null)).andReturn(null);
        mockedProducer.abortTransaction();
        expectLastCall().andThrow(new ProducerFencedException("KABOOM!"));
        replay(mockedProducer);

        eosAlphaStreamsProducerWithMock.initTransaction();
        // call `send()` to start a transaction
        eosAlphaStreamsProducerWithMock.send(record, null);

        eosAlphaStreamsProducerWithMock.abortTransaction();

        verify(mockedProducer);
    }

    @Test
    public void shouldThrowStreamsExceptionOnEosAbortTxError() {
        eosAlphaMockProducer.abortTransactionException = new KafkaException("KABOOM!");
        // call `send()` to start a transaction
        eosAlphaStreamsProducer.send(record, null);

        final StreamsException thrown = assertThrows(StreamsException.class, eosAlphaStreamsProducer::abortTransaction);

        assertThat(thrown.getCause(), is(eosAlphaMockProducer.abortTransactionException));
        assertThat(
            thrown.getMessage(),
            is("Error encounter trying to abort a transaction [test]")
        );
    }

    @Test
    public void shouldFailOnEosAbortTxFatal() {
        eosAlphaMockProducer.abortTransactionException = new RuntimeException("KABOOM!");
        // call `send()` to start a transaction
        eosAlphaStreamsProducer.send(record, null);

        final RuntimeException thrown = assertThrows(RuntimeException.class, eosAlphaStreamsProducer::abortTransaction);

        assertThat(thrown.getMessage(), is("KABOOM!"));
    }


    // EOS beta test

    // functional tests

    @Test
    public void shouldCloseExistingProducerOnResetProducer() {
        eosBetaStreamsProducer.resetProducer();

        assertTrue(eosBetaMockProducer.closed());
    }

    @Test
    public void shouldSetNewProducerOnResetProducer() {
        eosBetaStreamsProducer.resetProducer();

        assertThat(eosBetaMockClientSupplier.producers.size(), is(2));
        assertThat(eosBetaStreamsProducer.kafkaProducer(), is(eosBetaMockClientSupplier.producers.get(1)));
    }

    @Test
    public void shouldResetTransactionInitializedOnResetProducer() {
        final StreamsProducer streamsProducer = new StreamsProducer(
            eosBetaConfig,
            "threadId-StreamThread-0",
            clientSupplier,
            null,
            UUID.randomUUID(),
            logContext
        );
        streamsProducer.initTransaction();

        reset(mockedProducer);
        mockedProducer.close();
        mockedProducer.initTransactions();
        expectLastCall();
        replay(mockedProducer);

        streamsProducer.resetProducer();
        streamsProducer.initTransaction();

        verify(mockedProducer);
    }

}
