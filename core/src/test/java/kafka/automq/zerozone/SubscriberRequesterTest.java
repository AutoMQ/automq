/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.zerozone;

import kafka.automq.zerozone.SnapshotReadPartitionsManager.Subscriber;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.AutomqGetPartitionSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.s3.AutomqGetPartitionSnapshotResponse;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.utils.threads.EventLoop;

import org.apache.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Verifies requester logging isolation and recovery without changing retry scheduling or response handling. */
@Tag("S3Unit")
@Timeout(30)
public class SubscriberRequesterTest {
    private final Node node = new Node(1, "localhost", 9092);
    private final MockTime time = new MockTime(0, 0, 0);
    private final BlockingQueue<Runnable> callbacks = new LinkedBlockingQueue<>();
    private final List<CompletableFuture<ClientResponse>> requests = new ArrayList<>();
    private final Subscriber subscriber = mock(Subscriber.class);
    private final AsyncSender sender = mock(AsyncSender.class);
    private final EventLoop eventLoop = mock(EventLoop.class);
    private SubscriberRequester requester;
    private LogCaptureAppender appender;

    @BeforeEach
    public void setup() {
        appender = LogCaptureAppender.createAndRegister();
        appender.setClassLogger(SubscriberRequester.class, Level.INFO);
        doAnswer(invocation -> {
            callbacks.add(invocation.getArgument(0));
            return null;
        }).when(eventLoop).execute(any());
        when(sender.sendRequest(any(), any())).thenAnswer(invocation -> {
            CompletableFuture<ClientResponse> future = new CompletableFuture<>();
            requests.add(future);
            return future;
        });
        requester = newRequester(node);
    }

    @AfterEach
    public void cleanup() {
        requester.close();
        appender.close();
    }

    /** Suppressing logs must preserve both the minimum-delay path and the immediate retry path. */
    @Test
    public void testRequestSchedulingIsUnchanged() throws Exception {
        requester.start();
        runNextCallback();
        assertEquals(1, requests.size());
        // Complete before REQUEST_INTERVAL_MS: the existing scheduler queues request(), then request0().
        requests.get(0).completeExceptionally(new IOException("unavailable"));
        runNextCallback();
        assertEquals(1, requests.size());
        runNextCallback();
        assertEquals(2, requests.size());

        // Once the minimum interval has elapsed, each failure immediately queues the next request0().
        for (int i = 1; i < 6; i++) {
            time.sleep(SnapshotReadPartitionsManager.REQUEST_INTERVAL_MS);
            requests.get(i).completeExceptionally(new IOException("unavailable"));
            runNextCallback();
            assertEquals(i + 2, requests.size());
            assertTrue(callbacks.isEmpty());
        }
        assertEquals(1, messages("[SNAPSHOT_SUBSCRIBE_ERROR]").size());
        verify(sender, times(7)).sendRequest(any(), any());
    }

    /** A valid response flushes pending counts on the event loop, and the next failure starts a new episode. */
    @Test
    public void testSuccessfulResponseFlushesAndResets() throws Exception {
        requester.start();
        runNextCallback();
        failAndRetry(0);
        failAndRetry(1);
        time.sleep(1);
        requests.get(2).complete(response(Errors.NONE));
        assertTrue(messages("[SNAPSHOT_SUBSCRIBE_RECOVERED]").isEmpty());
        runNextCallback();

        List<String> recovered = messages("[SNAPSHOT_SUBSCRIBE_RECOVERED]");
        assertEquals(1, recovered.size());
        assertTrue(recovered.get(0).contains("unreportedFailures=1,totalFailures=2,"));
        verify(subscriber).onNewOperationBatch(any());
        verify(subscriber).unsafeRun();
        runNextCallback();
        time.sleep(1);
        requests.get(3).completeExceptionally(new IOException("new episode"));
        List<String> errors = messages("[SNAPSHOT_SUBSCRIBE_ERROR]");
        assertEquals(2, errors.size());
        assertTrue(errors.get(1).contains("totalFailures=1,failuresSinceLastReport=1,reportPeriodMs=0,"));
    }

    /** Normal timeout/disconnect/no-response values and protocol errors keep their existing logs and do not end an exception episode. */
    @Test
    public void testNormalErrorResponsesRemainOutOfScope() throws Exception {
        requester.start();
        runNextCallback();
        failAndRetry(0);
        failAndRetry(1);

        ClientResponse disconnected = mock(ClientResponse.class);
        when(disconnected.wasDisconnected()).thenReturn(true);
        ClientResponse timedOut = mock(ClientResponse.class);
        when(timedOut.wasTimedOut()).thenReturn(true);
        List<ClientResponse> responses = List.of(disconnected, timedOut, mock(ClientResponse.class), response(Errors.UNKNOWN_SERVER_ERROR));
        for (int i = 0; i < responses.size(); i++) {
            time.sleep(1);
            requests.get(i + 2).complete(responses.get(i));
            runNextCallback();
            runNextCallback();
            assertTrue(messages("[SNAPSHOT_SUBSCRIBE_RECOVERED]").isEmpty());
        }
        assertEquals(2, messages("[GET_SNAPSHOTS],[REQUEST_FAIL]").size());
        assertEquals(1, messages("[GET_SNAPSHOTS],[NO_RESPONSE]").size());
        assertEquals(1, messages("[GET_SNAPSHOTS],[ERROR]").size());
        time.sleep(1);
        requests.get(6).complete(response(Errors.NONE));
        runNextCallback();
        assertTrue(messages("[SNAPSHOT_SUBSCRIBE_RECOVERED]").get(0).contains("unreportedFailures=1,totalFailures=2,"));
    }

    /** Requesters own independent episodes even for the same destination, and different nodes report their own identity. */
    @Test
    public void testRequesterStateIsIndependent() throws Exception {
        SubscriberRequester sameDestination = newRequester(node);
        Node otherNode = new Node(2, "other-host", 9093);
        SubscriberRequester otherDestination = newRequester(otherNode);
        try {
            requester.start();
            sameDestination.start();
            otherDestination.start();
            runNextCallback();
            runNextCallback();
            runNextCallback();
            time.sleep(1);
            for (int i = 0; i < 3; i++) {
                requests.get(i).completeExceptionally(new IOException("unavailable"));
            }
            List<String> errors = messages("[SNAPSHOT_SUBSCRIBE_ERROR]");
            assertEquals(3, errors.size());
            assertTrue(errors.get(2).contains("node=" + otherNode));
            for (String error : errors) {
                assertTrue(error.contains("totalFailures=1,failuresSinceLastReport=1,"));
            }
        } finally {
            sameDestination.close();
            otherDestination.close();
        }
    }

    private SubscriberRequester newRequester(Node destination) {
        return new SubscriberRequester(subscriber, destination, AutoMQVersion.V2, sender, id -> "topic", eventLoop, time);
    }

    private void failAndRetry(int index) throws Exception {
        time.sleep(1);
        requests.get(index).completeExceptionally(new IOException("unavailable"));
        runNextCallback();
    }

    private void runNextCallback() throws Exception {
        Runnable callback = callbacks.poll(10, TimeUnit.SECONDS);
        assertNotNull(callback);
        callback.run();
    }

    private ClientResponse response(Errors error) {
        ClientResponse response = mock(ClientResponse.class);
        when(response.hasResponse()).thenReturn(true);
        when(response.requestHeader()).thenReturn(mock(RequestHeader.class));
        when(response.responseBody()).thenReturn(new AutomqGetPartitionSnapshotResponse(
            new AutomqGetPartitionSnapshotResponseData().setErrorCode(error.code())));
        return response;
    }

    private List<String> messages(String prefix) {
        return appender.getMessages().stream().filter(message -> message.startsWith(prefix)).collect(Collectors.toList());
    }
}
