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
package org.apache.kafka.server.util;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InterBrokerAsyncSender implements AsyncSender {

    private final SendThread sendThread;
    private final ConcurrentLinkedQueue<PendingRequest> pendingRequests = new ConcurrentLinkedQueue<>();
    private final Time time;

    // Package-private: tests use this to avoid starting the thread
    InterBrokerAsyncSender(String name, KafkaClient networkClient, int requestTimeoutMs, Time time) {
        this.time = time;
        this.sendThread = new SendThread(name, networkClient, requestTimeoutMs, time);
    }

    public static InterBrokerAsyncSender create(String name, KafkaClient networkClient,
                                                 int requestTimeoutMs, Time time) {
        InterBrokerAsyncSender sender = new InterBrokerAsyncSender(name, networkClient, requestTimeoutMs, time);
        sender.sendThread.start();
        return sender;
    }

    @Override
    public <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(
        Node node, AbstractRequest.Builder<T> requestBuilder
    ) {
        CompletableFuture<ClientResponse> future = new CompletableFuture<>();
        PendingRequest request = new PendingRequest(node, requestBuilder, future, time.milliseconds());
        pendingRequests.offer(request);
        sendThread.wakeup();
        return future;
    }

    @Override
    public void close() {
        try {
            sendThread.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void pollOnce(long maxTimeoutMs) {
        sendThread.pollOnce(maxTimeoutMs);
    }

    private static class PendingRequest {
        final Node node;
        final AbstractRequest.Builder<?> requestBuilder;
        final CompletableFuture<ClientResponse> future;
        final long creationTimeMs;

        PendingRequest(Node node, AbstractRequest.Builder<?> requestBuilder, 
                      CompletableFuture<ClientResponse> future, long creationTimeMs) {
            this.node = node;
            this.requestBuilder = requestBuilder;
            this.future = future;
            this.creationTimeMs = creationTimeMs;
        }
    }

    private class SendThread extends InterBrokerSendThread {

        SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time) {
            super(name, networkClient, requestTimeoutMs, time);
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            Collection<RequestAndCompletionHandler> requests = new ArrayList<>();
            PendingRequest pending;
            while ((pending = pendingRequests.poll()) != null) {
                final PendingRequest request = pending;
                RequestCompletionHandler handler = new RequestCompletionHandler() {
                    @Override
                    public void onComplete(ClientResponse response) {
                        if (response.authenticationException() != null) {
                            request.future.completeExceptionally(response.authenticationException());
                        } else if (response.versionMismatch() != null) {
                            request.future.completeExceptionally(response.versionMismatch());
                        } else if (response.wasDisconnected()) {
                            if (response.wasTimedOut()) {
                                request.future.completeExceptionally(new TimeoutException("Request timed out"));
                            } else {
                                request.future.completeExceptionally(new DisconnectException("Disconnected from " + request.node));
                            }
                        } else {
                            request.future.complete(response);
                        }
                    }
                };
                requests.add(new RequestAndCompletionHandler(
                    request.creationTimeMs,
                    request.node,
                    request.requestBuilder,
                    handler
                ));
            }
            return requests;
        }
    }
}