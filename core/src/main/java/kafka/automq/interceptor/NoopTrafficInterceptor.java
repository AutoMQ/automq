/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.interceptor;

import kafka.server.MetadataCache;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Node;
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
    public void handleProduceRequest(ProduceRequestArgs args) {
        kafkaApis.handleProduceAppendJavaCompatible(args);
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
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
