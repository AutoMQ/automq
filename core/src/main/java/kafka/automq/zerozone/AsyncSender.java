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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
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
import org.apache.kafka.server.util.InterBrokerAsyncSender;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public interface AsyncSender {

    <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(
        Node node,
        AbstractRequest.Builder<T> requestBuilder
    );

    void initiateClose();

    void close();

    class BrokersAsyncSender implements AsyncSender {
        private static final int REQUEST_TIMEOUT_MS = 30_000;

        private final NetworkClient networkClient;
        private final org.apache.kafka.server.util.AsyncSender delegate;

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
            this.delegate = InterBrokerAsyncSender.create(
                metricGroupPrefix,
                networkClient,
                REQUEST_TIMEOUT_MS,
                time
            );
        }

        @Override
        public <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(Node node,
            AbstractRequest.Builder<T> requestBuilder) {
            return delegate.sendRequest(node, requestBuilder);
        }

        @Override
        public void initiateClose() {
            networkClient.initiateClose();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

}
