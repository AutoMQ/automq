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

package kafka.automq.zonerouter;

import kafka.server.MetadataCache;
import kafka.server.RequestLocal;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;

import com.automq.stream.utils.FutureUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class NoopProduceRouter implements ProduceRouter {
    private final ElasticKafkaApis kafkaApis;
    private final MetadataCache metadataCache;

    public NoopProduceRouter(ElasticKafkaApis kafkaApis, MetadataCache metadataCache) {
        this.kafkaApis = kafkaApis;
        this.metadataCache = metadataCache;
    }

    @Override
    public void handleProduceRequest(short apiVersion, ClientIdMetadata clientId, int timeout, short requiredAcks,
        boolean internalTopicsAllowed, String transactionId, Map<TopicPartition, MemoryRecords> entriesPerPartition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
        Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback,
        RequestLocal requestLocal
    ) {
        kafkaApis.handleProduceAppendJavaCompatible(
            timeout,
            requiredAcks,
            internalTopicsAllowed,
            transactionId,
            entriesPerPartition,
            rst -> {
                responseCallback.accept(rst);
                return null;
            },
            rst -> {
                recordValidationStatsCallback.accept(rst);
                return null;
            },
            apiVersion,
            requestLocal
        );
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(String clientId,
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
