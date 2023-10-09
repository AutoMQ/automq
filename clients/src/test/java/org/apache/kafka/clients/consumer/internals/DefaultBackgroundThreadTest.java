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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class DefaultBackgroundThreadTest {
    private static final long RETRY_BACKOFF_MS = 100;
    private final Properties properties = new Properties();
    private MockTime time;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<BackgroundEvent> backgroundEventsQueue;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor applicationEventProcessor;
    private CoordinatorRequestManager coordinatorManager;
    private OffsetsRequestManager offsetsRequestManager;
    private ErrorEventHandler errorEventHandler;
    private final int requestTimeoutMs = 500;
    private GroupState groupState;
    private CommitRequestManager commitManager;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private HeartbeatRequestManager heartbeatRequestManager;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        this.time = new MockTime(0);
        this.metadata = mock(ConsumerMetadata.class);
        this.networkClient = mock(NetworkClientDelegate.class);
        this.applicationEventsQueue = (BlockingQueue<ApplicationEvent>) mock(BlockingQueue.class);
        this.backgroundEventsQueue = (BlockingQueue<BackgroundEvent>) mock(BlockingQueue.class);
        this.applicationEventProcessor = mock(ApplicationEventProcessor.class);
        this.coordinatorManager = mock(CoordinatorRequestManager.class);
        this.offsetsRequestManager = mock(OffsetsRequestManager.class);
        this.heartbeatRequestManager = mock(HeartbeatRequestManager.class);
        this.errorEventHandler = mock(ErrorEventHandler.class);
        GroupRebalanceConfig rebalanceConfig = new GroupRebalanceConfig(
                100,
                100,
                100,
                "group_id",
                Optional.empty(),
                100,
                1000,
                true);
        this.groupState = new GroupState(rebalanceConfig);
        this.commitManager = mock(CommitRequestManager.class);
        this.topicMetadataRequestManager = mock(TopicMetadataRequestManager.class);
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        backgroundThread.start();
        TestUtils.waitForCondition(backgroundThread::isRunning, "Failed awaiting for the background thread to be running");
        backgroundThread.close();
        assertFalse(backgroundThread.isRunning());
    }

    @Test
    public void testApplicationEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NoopApplicationEvent("noop event");
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor, times(1)).process(e);
        backgroundThread.close();
    }

    @Test
    public void testMetadataUpdateEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        this.applicationEventProcessor = new ApplicationEventProcessor(
                this.backgroundEventsQueue,
                mockRequestManagers(),
                metadata);
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NewTopicsMetadataUpdateRequestEvent();
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(metadata).requestUpdateForNewTopics();
        backgroundThread.close();
    }

    @Test
    public void testCommitEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new CommitApplicationEvent(new HashMap<>());
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(CommitApplicationEvent.class));
        backgroundThread.close();
    }


    @Test
    public void testListOffsetsEventIsProcessed() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        Map<TopicPartition, Long> timestamps = Collections.singletonMap(new TopicPartition("topic1", 1), 5L);
        ApplicationEvent e = new ListOffsetsApplicationEvent(timestamps, true);
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ListOffsetsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
        backgroundThread.close();
    }

    @Test
    public void testResetPositionsEventIsProcessed() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ResetPositionsApplicationEvent e = new ResetPositionsApplicationEvent();
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ResetPositionsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
        backgroundThread.close();
    }

    @Test
    public void testResetPositionsProcessFailure() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        applicationEventProcessor = spy(new ApplicationEventProcessor(
                this.backgroundEventsQueue,
                mockRequestManagers(),
                metadata));
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();

        TopicAuthorizationException authException = new TopicAuthorizationException("Topic authorization failed");
        doThrow(authException).when(offsetsRequestManager).resetPositionsIfNeeded();

        ResetPositionsApplicationEvent event = new ResetPositionsApplicationEvent();
        this.applicationEventsQueue.add(event);
        assertThrows(TopicAuthorizationException.class, backgroundThread::runOnce);

        verify(applicationEventProcessor).process(any(ResetPositionsApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    public void testValidatePositionsEventIsProcessed() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        ValidatePositionsApplicationEvent e = new ValidatePositionsApplicationEvent();
        this.applicationEventsQueue.add(e);
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(ValidatePositionsApplicationEvent.class));
        assertTrue(applicationEventsQueue.isEmpty());
        backgroundThread.close();
    }

    @Test
    public void testValidatePositionsProcessFailure() {
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        applicationEventProcessor = spy(new ApplicationEventProcessor(
                this.backgroundEventsQueue,
                mockRequestManagers(),
                metadata));
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();

        LogTruncationException logTruncationException = new LogTruncationException(Collections.emptyMap(), Collections.emptyMap());
        doThrow(logTruncationException).when(offsetsRequestManager).validatePositionsIfNeeded();

        ValidatePositionsApplicationEvent event = new ValidatePositionsApplicationEvent();
        this.applicationEventsQueue.add(event);
        assertThrows(LogTruncationException.class, backgroundThread::runOnce);

        verify(applicationEventProcessor).process(any(ValidatePositionsApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    public void testAssignmentChangeEvent() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        this.applicationEventProcessor = spy(new ApplicationEventProcessor(
                this.backgroundEventsQueue,
                mockRequestManagers(),
                metadata));

        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        HashMap<TopicPartition, OffsetAndMetadata> offset = mockTopicPartitionOffset();

        final long currentTimeMs = time.milliseconds();
        ApplicationEvent e = new AssignmentChangeApplicationEvent(offset, currentTimeMs);
        this.applicationEventsQueue.add(e);

        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());

        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(AssignmentChangeApplicationEvent.class));
        verify(networkClient, times(1)).poll(anyLong(), anyLong());
        verify(commitManager, times(1)).updateAutoCommitTimer(currentTimeMs);
        verify(commitManager, times(1)).maybeAutoCommit(offset);

        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        backgroundThread.runOnce();
        Mockito.verify(coordinatorManager, times(1)).poll(anyLong());
        Mockito.verify(networkClient, times(1)).poll(anyLong(), anyLong());
        backgroundThread.close();
    }

    @Test
    void testFetchTopicMetadata() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        when(coordinatorManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        when(offsetsRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(topicMetadataRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        when(heartbeatRequestManager.poll(anyLong())).thenReturn(emptyPollResults());
        this.applicationEventsQueue.add(new TopicMetadataApplicationEvent("topic"));
        backgroundThread.runOnce();
        verify(applicationEventProcessor).process(any(TopicMetadataApplicationEvent.class));
        backgroundThread.close();
    }

    @Test
    void testPollResultTimer() {
        DefaultBackgroundThread backgroundThread = mockBackgroundThread();
        // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
        NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                10,
                Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
        assertEquals(10, backgroundThread.handlePollResult(success));

        NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                10,
                new ArrayList<>());
        assertEquals(10, backgroundThread.handlePollResult(failure));
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private RequestManagers mockRequestManagers() {
        return new RequestManagers(
            offsetsRequestManager,
            topicMetadataRequestManager,
            Optional.of(coordinatorManager),
            Optional.of(commitManager),
            Optional.of(heartbeatRequestManager));
    }

    private static NetworkClientDelegate.UnsentRequest findCoordinatorUnsentRequest(
            final Time time,
            final long timeout
    ) {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
                Optional.empty());
        req.setTimer(time, timeout);
        return req;
    }

    private DefaultBackgroundThread mockBackgroundThread() {
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);

        return new DefaultBackgroundThread(
            this.time,
            new ConsumerConfig(properties),
            new LogContext(),
            applicationEventsQueue,
            backgroundEventsQueue,
            this.errorEventHandler,
            applicationEventProcessor,
            this.metadata,
            this.networkClient,
            this.groupState,
            this.coordinatorManager,
            this.commitManager,
            this.offsetsRequestManager,
            this.topicMetadataRequestManager,
            this.heartbeatRequestManager);
    }

    private NetworkClientDelegate.PollResult mockPollCoordinatorResult() {
        return new NetworkClientDelegate.PollResult(
            RETRY_BACKOFF_MS,
            Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
    }

    private NetworkClientDelegate.PollResult mockPollCommitResult() {
        return new NetworkClientDelegate.PollResult(
            RETRY_BACKOFF_MS,
            Collections.singletonList(findCoordinatorUnsentRequest(time, requestTimeoutMs)));
    }

    private NetworkClientDelegate.PollResult emptyPollResults() {
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList());
    }
}