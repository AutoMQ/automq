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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.stubbing.Answer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrototypeAsyncConsumerTest {

    private PrototypeAsyncConsumer<?, ?> consumer;
    private final Map<String, Object> consumerProps = new HashMap<>();

    private final Time time = new MockTime();
    private LogContext logContext;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private EventHandler eventHandler;
    private Metrics metrics;

    private String groupId = "group.id";
    private ConsumerConfig config;

    @BeforeEach
    public void setup() {
        injectConsumerConfigs();
        this.config = new ConsumerConfig(consumerProps);
        this.logContext = new LogContext();
        this.subscriptions = mock(SubscriptionState.class);
        this.metadata = mock(ConsumerMetadata.class);
        final DefaultBackgroundThread bt = mock(DefaultBackgroundThread.class);
        final BlockingQueue<ApplicationEvent> aq = new LinkedBlockingQueue<>();
        final BlockingQueue<BackgroundEvent> bq = new LinkedBlockingQueue<>();
        this.eventHandler = spy(new DefaultEventHandler(bt, aq, bq));
        this.metrics = new Metrics(time);
    }

    @AfterEach
    public void cleanup() {
        if (consumer != null) {
            consumer.close(Duration.ZERO);
        }
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testInvalidGroupId() {
        this.groupId = null;
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(InvalidGroupIdException.class, () -> consumer.committed(new HashSet<>()));
    }

    @Test
    public void testCommitAsync_NullCallback() throws InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        PrototypeAsyncConsumer<?, ?> mockedConsumer = spy(newConsumer(time, new StringDeserializer(), new StringDeserializer()));
        doReturn(future).when(mockedConsumer).commit(offsets, false);
        mockedConsumer.commitAsync(offsets, null);
        future.complete(null);
        TestUtils.waitForCondition(() -> future.isDone(),
                2000,
                "commit future should complete");

        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testCommitAsync_UserSuppliedCallback() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        PrototypeAsyncConsumer<?, ?> mockedConsumer = spy(consumer);
        doReturn(future).when(mockedConsumer).commit(offsets, false);
        OffsetCommitCallback customCallback = mock(OffsetCommitCallback.class);
        mockedConsumer.commitAsync(offsets, customCallback);
        future.complete(null);
        verify(customCallback).onComplete(offsets, null);
    }

    @Test
    public void testCommitted() {
        Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.complete(offsets);

        try (MockedConstruction<OffsetFetchApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            this.consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
            assertDoesNotThrow(() -> consumer.committed(offsets.keySet(), Duration.ofMillis(1000)));
            verify(eventHandler).add(ArgumentMatchers.isA(OffsetFetchApplicationEvent.class));
        }
    }

    @Test
    public void testCommitted_ExceptionThrown() {
        Map<TopicPartition, OffsetAndMetadata> offsets = mockTopicPartitionOffset();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.completeExceptionally(new KafkaException("Test exception"));

        try (MockedConstruction<OffsetFetchApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            this.consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
            assertThrows(KafkaException.class, () -> consumer.committed(offsets.keySet(), Duration.ofMillis(1000)));
            verify(eventHandler).add(ArgumentMatchers.isA(OffsetFetchApplicationEvent.class));
        }
    }

    /**
     * This is a rather ugly bit of code. Not my choice :(
     *
     * <p/>
     *
     * Inside the {@link org.apache.kafka.clients.consumer.Consumer#committed(Set, Duration)} call we create an
     * instance of {@link OffsetFetchApplicationEvent} that holds the partitions and internally holds a
     * {@link CompletableFuture}. We want to test different behaviours of the {@link Future#get()}, such as
     * returning normally, timing out, throwing an error, etc. By mocking the construction of the event object that
     * is created, we can affect that behavior.
     */
    private static MockedConstruction<OffsetFetchApplicationEvent> offsetFetchEventMocker(CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
        // This "answer" is where we pass the future to be invoked by the ConsumerUtils.getResult() method
        Answer<Map<TopicPartition, OffsetAndMetadata>> getInvocationAnswer = invocation -> {
            // This argument captures the actual argument value that was passed to the event's get() method, so we
            // just "forward" that value to our mocked call
            Timer timer = invocation.getArgument(0);
            return ConsumerUtils.getResult(future, timer);
        };

        MockedConstruction.MockInitializer<OffsetFetchApplicationEvent> mockInitializer = (mock, ctx) -> {
            // When the event's get() method is invoked, we call the "answer" method just above
            when(mock.get(any())).thenAnswer(getInvocationAnswer);

            // When the event's type() method is invoked, we have to return the type as it will be null in the mock
            when(mock.type()).thenReturn(ApplicationEvent.Type.FETCH_COMMITTED_OFFSET);

            // This is needed for the WakeupTrigger code that keeps track of the active task
            when(mock.future()).thenReturn(future);
        };

        return mockConstruction(OffsetFetchApplicationEvent.class, mockInitializer);
    }

    @Test
    public void testAssign() {
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        final TopicPartition tp = new TopicPartition("foo", 3);
        consumer.assign(singleton(tp));
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().contains(tp));
        verify(eventHandler).add(any(AssignmentChangeApplicationEvent.class));
        verify(eventHandler).add(any(NewTopicsMetadataUpdateRequestEvent.class));
    }

    @Test
    public void testAssignOnNullTopicPartition() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(null));
    }

    @Test
    public void testAssignOnEmptyTopicPartition() {
        consumer = spy(newConsumer(time, new StringDeserializer(), new StringDeserializer()));
        consumer.assign(Collections.emptyList());
        assertTrue(consumer.subscription().isEmpty());
        assertTrue(consumer.assignment().isEmpty());
    }

    @Test
    public void testAssignOnNullTopicInPartition() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition(null, 0))));
    }

    @Test
    public void testAssignOnEmptyTopicInPartition() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(IllegalArgumentException.class, () -> consumer.assign(singleton(new TopicPartition("  ", 0))));
    }

    @Test
    public void testBeginningOffsetsFailsIfNullPartitions() {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(NullPointerException.class, () -> consumer.beginningOffsets(null,
                Duration.ofMillis(1)));
    }

    @Test
    public void testBeginningOffsets() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        Map<TopicPartition, OffsetAndTimestamp> expectedOffsetsAndTimestamp =
                mockOffsetAndTimestamp();
        Set<TopicPartition> partitions = expectedOffsetsAndTimestamp.keySet();
        doReturn(expectedOffsetsAndTimestamp).when(eventHandler).addAndGet(any(), any());
        Map<TopicPartition, Long> result =
                assertDoesNotThrow(() -> consumer.beginningOffsets(partitions,
                        Duration.ofMillis(1)));
        Map<TopicPartition, Long> expectedOffsets = expectedOffsetsAndTimestamp.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().offset()));
        assertEquals(expectedOffsets, result);
        verify(eventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsThrowsKafkaExceptionForUnderlyingExecutionFailure() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        Set<TopicPartition> partitions = mockTopicPartitionOffset().keySet();
        Throwable eventProcessingFailure = new KafkaException("Unexpected failure " +
                "processing List Offsets event");
        doThrow(eventProcessingFailure).when(eventHandler).addAndGet(any(), any());
        Throwable consumerError = assertThrows(KafkaException.class,
                () -> consumer.beginningOffsets(partitions,
                        Duration.ofMillis(1)));
        assertEquals(eventProcessingFailure, consumerError);
        verify(eventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class), ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testBeginningOffsetsTimeoutOnEventProcessingTimeout() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        doThrow(new TimeoutException()).when(eventHandler).addAndGet(any(), any());
        assertThrows(TimeoutException.class,
                () -> consumer.beginningOffsets(
                        Collections.singletonList(new TopicPartition("t1", 0)),
                        Duration.ofMillis(1)));
        verify(eventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testOffsetsForTimesOnNullPartitions() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(NullPointerException.class, () -> consumer.offsetsForTimes(null,
                Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimesFailsOnNegativeTargetTimes() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        assertThrows(IllegalArgumentException.class,
                () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                                "topic1", 1), ListOffsetsRequest.EARLIEST_TIMESTAMP),
                        Duration.ofMillis(1)));

        assertThrows(IllegalArgumentException.class,
                () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                                "topic1", 1), ListOffsetsRequest.LATEST_TIMESTAMP),
                        Duration.ofMillis(1)));

        assertThrows(IllegalArgumentException.class,
                () -> consumer.offsetsForTimes(Collections.singletonMap(new TopicPartition(
                                "topic1", 1), ListOffsetsRequest.MAX_TIMESTAMP),
                        Duration.ofMillis(1)));
    }

    @Test
    public void testOffsetsForTimes() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        Map<TopicPartition, OffsetAndTimestamp> expectedResult = mockOffsetAndTimestamp();
        Map<TopicPartition, Long> timestampToSearch = mockTimestampToSearch();

        doReturn(expectedResult).when(eventHandler).addAndGet(any(), any());
        Map<TopicPartition, OffsetAndTimestamp> result =
                assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch, Duration.ofMillis(1)));
        assertEquals(expectedResult, result);
        verify(eventHandler).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    // This test ensures same behaviour as the current consumer when offsetsForTimes is called
    // with 0 timeout. It should return map with all requested partitions as keys, with null
    // OffsetAndTimestamp as value.
    @Test
    public void testOffsetsForTimesWithZeroTimeout() {
        PrototypeAsyncConsumer<?, ?> consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());
        TopicPartition tp = new TopicPartition("topic1", 0);
        Map<TopicPartition, OffsetAndTimestamp> expectedResult =
                Collections.singletonMap(tp, null);
        Map<TopicPartition, Long> timestampToSearch = Collections.singletonMap(tp, 5L);

        Map<TopicPartition, OffsetAndTimestamp> result =
                assertDoesNotThrow(() -> consumer.offsetsForTimes(timestampToSearch,
                        Duration.ofMillis(0)));
        assertEquals(expectedResult, result);
        verify(eventHandler, never()).addAndGet(ArgumentMatchers.isA(ListOffsetsApplicationEvent.class),
                ArgumentMatchers.isA(Timer.class));
    }

    @Test
    public void testWakeup_committed() {
        consumer = newConsumer(time, new StringDeserializer(),
            new StringDeserializer());
        consumer.wakeup();
        assertThrows(WakeupException.class, () -> consumer.committed(mockTopicPartitionOffset().keySet()));
        assertNoPendingWakeup(consumer.wakeupTrigger());
    }

    @Test
    public void testRefreshCommittedOffsetsSuccess() {
        Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                Collections.singletonMap(new TopicPartition("t1", 1), new OffsetAndMetadata(10L));
        testRefreshCommittedOffsetsSuccess(committedOffsets);
    }

    @Test
    public void testRefreshCommittedOffsetsSuccessButNoCommittedOffsetsFound() {
        testRefreshCommittedOffsetsSuccess(Collections.emptyMap());
    }

    @Test
    public void testRefreshCommittedOffsetsShouldNotResetIfFailedWithTimeout() {
        // Create consumer with group id to enable committed offset usage
        this.groupId = "consumer-group-1";

        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(true);
    }

    @Test
    public void testRefreshCommittedOffsetsNotCalledIfNoGroupId() {
        // Create consumer without group id so committed offsets are not used for updating positions
        this.groupId = null;
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());

        testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(false);
    }

    private void testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout(boolean committedOffsetsEnabled) {
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());

        // Uncompleted future that will time out if used
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();

        when(subscriptions.initializingPartitions()).thenReturn(Collections.singleton(new TopicPartition("t1", 1)));

        try (MockedConstruction<OffsetFetchApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {

            // Poll with 0 timeout to run a single iteration of the poll loop
            consumer.poll(Duration.ofMillis(0));

            verify(eventHandler).add(ArgumentMatchers.isA(ValidatePositionsApplicationEvent.class));

            if (committedOffsetsEnabled) {
                // Verify there was an OffsetFetch event and no ResetPositions event
                verify(eventHandler).add(ArgumentMatchers.isA(OffsetFetchApplicationEvent.class));
                verify(eventHandler,
                        never()).add(ArgumentMatchers.isA(ResetPositionsApplicationEvent.class));
            } else {
                // Verify there was not any OffsetFetch event but there should be a ResetPositions
                verify(eventHandler,
                        never()).add(ArgumentMatchers.isA(OffsetFetchApplicationEvent.class));
                verify(eventHandler).add(ArgumentMatchers.isA(ResetPositionsApplicationEvent.class));
            }
        }
    }

    private void testRefreshCommittedOffsetsSuccess(Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> committedFuture = new CompletableFuture<>();
        committedFuture.complete(committedOffsets);

        // Create consumer with group id to enable committed offset usage
        this.groupId = "consumer-group-1";
        consumer = newConsumer(time, new StringDeserializer(), new StringDeserializer());

        try (MockedConstruction<OffsetFetchApplicationEvent> ignored = offsetFetchEventMocker(committedFuture)) {
            when(subscriptions.initializingPartitions()).thenReturn(committedOffsets.keySet());

            // Poll with 0 timeout to run a single iteration of the poll loop
            consumer.poll(Duration.ofMillis(0));

            verify(eventHandler).add(ArgumentMatchers.isA(ValidatePositionsApplicationEvent.class));
            verify(eventHandler).add(ArgumentMatchers.isA(OffsetFetchApplicationEvent.class));
            verify(eventHandler).add(ArgumentMatchers.isA(ResetPositionsApplicationEvent.class));
        }
    }

    private void assertNoPendingWakeup(final WakeupTrigger wakeupTrigger) {
        assertTrue(wakeupTrigger.getPendingTask() == null);
    }

    private Map<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private Map<TopicPartition, OffsetAndTimestamp> mockOffsetAndTimestamp() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = new HashMap<>();
        offsetAndTimestamp.put(t0, new OffsetAndTimestamp(5L, 1L));
        offsetAndTimestamp.put(t1, new OffsetAndTimestamp(6L, 3L));
        return offsetAndTimestamp;
    }

    private Map<TopicPartition, Long> mockTimestampToSearch() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
        timestampToSearch.put(t0, 1L);
        timestampToSearch.put(t1, 2L);
        return timestampToSearch;
    }

    private void injectConsumerConfigs() {
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        consumerProps.put(DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    private PrototypeAsyncConsumer<?, ?> newConsumer(final Time time,
                                                     final Deserializer<?> keyDeserializer,
                                                     final Deserializer<?> valueDeserializer) {
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());

        return new PrototypeAsyncConsumer<>(
                time,
                logContext,
                config,
                subscriptions,
                metadata,
                eventHandler,
                metrics,
                Optional.ofNullable(this.groupId),
                config.getInt(DEFAULT_API_TIMEOUT_MS_CONFIG));
    }
}

