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

package kafka.automq.zerozone;

import kafka.server.KafkaConfig;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public interface AsyncSender {

    <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(
        Node node,
        AbstractRequest.Builder<T> requestBuilder
    );

    void initiateClose();

    void close();

    class BrokersAsyncSender implements AsyncSender {
        private static final Logger LOGGER = LoggerFactory.getLogger(BrokersAsyncSender.class);
        private final NetworkClient networkClient;
        private final Time time;
        private final ExecutorService executorService;
        private final AtomicBoolean shouldRun = new AtomicBoolean(true);

        public BrokersAsyncSender(
            KafkaConfig brokerConfig,
            Metrics metrics,
            String metricGroupPrefix,
            Time time,
            String clientId,
            LogContext logContext
        ) {

            ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
                brokerConfig.interBrokerSecurityProtocol(),
                JaasContext.Type.SERVER,
                brokerConfig,
                brokerConfig.interBrokerListenerName(),
                brokerConfig.saslMechanismInterBrokerProtocol(),
                time,
                brokerConfig.saslInterBrokerHandshakeRequestEnable(),
                logContext
            );
            Selector selector = new Selector(
                NetworkReceive.UNLIMITED,
                brokerConfig.connectionsMaxIdleMs(),
                metrics,
                time,
                metricGroupPrefix,
                Collections.emptyMap(),
                false,
                channelBuilder,
                logContext
            );
            this.networkClient = new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                clientId,
                64,
                0,
                0,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                brokerConfig.replicaSocketReceiveBufferBytes(),
                brokerConfig.requestTimeoutMs(),
                brokerConfig.connectionSetupTimeoutMs(),
                brokerConfig.connectionSetupTimeoutMaxMs(),
                time,
                true,
                new ApiVersions(),
                logContext,
                MetadataRecoveryStrategy.REBOOTSTRAP
            );
            this.time = time;
            executorService = Threads.newFixedThreadPoolWithMonitor(1, metricGroupPrefix, true, LOGGER);
            executorService.submit(this::run);
        }

        private final ConcurrentMap<Node, Queue<Request>> waitingSendRequests = new ConcurrentHashMap<>();

        @Override
        public <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(Node node,
            AbstractRequest.Builder<T> requestBuilder) {
            CompletableFuture<ClientResponse> cf = new CompletableFuture<>();
            waitingSendRequests.compute(node, (n, queue) -> {
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<>();
                }
                queue.add(new Request(requestBuilder, cf));
                return queue;
            });
            return cf;
        }

        private void run() {
            Map<Node, ConnectingState> connectingStates = new ConcurrentHashMap<>();
            while (shouldRun.get()) {
                // TODO: graceful shutdown
                try {
                    long now = time.milliseconds();
                    waitingSendRequests.forEach((node, queue) -> {
                        if (queue.isEmpty()) {
                            return;
                        }
                        if (NetworkClientUtils.isReady(networkClient, node, now)) {
                            connectingStates.remove(node);
                            Request request = queue.poll();
                            ClientRequest clientRequest = networkClient.newClientRequest(Integer.toString(node.id()), request.requestBuilder, now, true, 30000, new RequestCompletionHandler() {
                                @Override
                                public void onComplete(ClientResponse response) {
                                    request.cf.complete(response);
                                }
                            });
                            networkClient.send(clientRequest, now);
                        } else {
                            ConnectingState connectingState = connectingStates.get(node);
                            if (connectingState == null) {
                                networkClient.ready(node, now);
                                connectingStates.put(node, new ConnectingState(now));
                            } else {
                                if (now - connectingState.startConnectNanos > 3000) {
                                    for (; ; ) {
                                        Request request = queue.poll();
                                        if (request == null) {
                                            break;
                                        }
                                        request.cf.completeExceptionally(new SocketTimeoutException(String.format("Cannot connect to node=%s", node)));
                                    }
                                    connectingStates.remove(node);
                                } else if (now - connectingState.startConnectNanos > 1000 && connectingState.connectTimes < 2) {
                                    // The broker network maybe slightly ready after the broker become UNFENCED.
                                    // So we need to retry connect twice.
                                    networkClient.ready(node, now);
                                    connectingState.connectTimes = connectingState.connectTimes + 1;
                                }
                            }
                        }
                    });
                    networkClient.poll(1, now);
                } catch (Throwable e) {
                    LOGGER.error("Processor get uncaught exception", e);
                }
            }
        }

        @Override
        public void initiateClose() {
            networkClient.initiateClose();
        }

        @Override
        public void close() {
            networkClient.close();
            shouldRun.set(false);
        }
    }

    class Request {
        final AbstractRequest.Builder<?> requestBuilder;
        final CompletableFuture<ClientResponse> cf;

        public Request(AbstractRequest.Builder<?> requestBuilder, CompletableFuture<ClientResponse> cf) {
            this.requestBuilder = requestBuilder;
            this.cf = cf;
        }
    }

    class ConnectingState {
        final long startConnectNanos;
        int connectTimes;

        public ConnectingState(long startConnectNanos) {
            this.startConnectNanos = startConnectNanos;
            connectTimes = 1;
        }
    }

}
