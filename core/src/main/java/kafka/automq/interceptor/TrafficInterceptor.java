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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface TrafficInterceptor {

    void handleProduceRequest(ProduceRequestArgs args);

    CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata);

    List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(ClientIdMetadata clientId,
        List<MetadataResponseData.MetadataResponseTopic> topics);

    Optional<Node> getLeaderNode(int leaderId, ClientIdMetadata clientId, String listenerName);

}
