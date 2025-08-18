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

package kafka.automq.interceptor;

import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.AutomqZoneRouterRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;

import com.automq.stream.utils.FutureUtil;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NoopTrafficInterceptor implements TrafficInterceptor {
    private final ElasticKafkaApis kafkaApis;
    private final MetadataCache metadataCache;

    public NoopTrafficInterceptor(ElasticKafkaApis kafkaApis, MetadataCache metadataCache) {
        this.kafkaApis = kafkaApis;
        this.metadataCache = metadataCache;
    }

    @Override
    public void close() {

    }

    @Override
    public void handleProduceRequest(ProduceRequestArgs args) {
        kafkaApis.handleProduceAppendJavaCompatible(args);
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(AutomqZoneRouterRequestData request) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(ClientIdMetadata clientId,
        List<MetadataResponseData.MetadataResponseTopic> topics) {
        return topics;
    }

    @Override
    public Optional<Node> getLeaderNode(int leaderId, ClientIdMetadata clientId, String listenerName) {
        scala.Option<Node> opt = metadataCache.getAliveBrokerNode(leaderId, new ListenerName(listenerName));
        if (opt.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(opt.get());
        }
    }
}
