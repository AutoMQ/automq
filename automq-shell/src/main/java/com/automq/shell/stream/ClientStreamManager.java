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

package com.automq.shell.stream;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.CloseStreamsRequestData;
import org.apache.kafka.common.message.CloseStreamsResponseData;
import org.apache.kafka.common.message.DescribeStreamsRequestData;
import org.apache.kafka.common.message.DescribeStreamsResponseData;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.CloseStreamsRequest;
import org.apache.kafka.common.requests.s3.DescribeStreamsRequest;
import org.apache.kafka.common.requests.s3.GetOpeningStreamsRequest;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClientStreamManager {

    private final NetworkClient networkClient;
    private final Node bootstrapServer;

    public ClientStreamManager(NetworkClient networkClient, Node bootstrapServer) throws IOException {
        this.networkClient = networkClient;
        this.bootstrapServer = bootstrapServer;
        connect(bootstrapServer);
    }

    private void connect(Node node) throws IOException {
        boolean ready = networkClient.isReady(node, Time.SYSTEM.milliseconds());
        if (ready) {
            return;
        }

        ready = NetworkClientUtils.awaitReady(networkClient, node, Time.SYSTEM, 1000);
        if (!ready) {
            throw new NetworkException("Failed to connect to the node " + node.id());
        }
    }

    public List<DescribeStreamsResponseData.StreamMetadata> describeStreamsByStream(int streamId) throws IOException {
        return describeStreams(streamId, -1, Map.of());
    }

    public List<DescribeStreamsResponseData.StreamMetadata> describeStreamsByNode(int nodeId) throws IOException {
        return describeStreams(-1, nodeId, Map.of());
    }

    public List<DescribeStreamsResponseData.StreamMetadata> describeStreamsByTopicPartition(Map<String, List<Integer>> topicPartitionMap) throws IOException {
        return describeStreams(-1, -1, topicPartitionMap);
    }

    private List<DescribeStreamsResponseData.StreamMetadata> describeStreams(int streamId, int nodeId, Map<String, List<Integer>> topicPartitionMap) throws IOException {
        long now = Time.SYSTEM.milliseconds();

        DescribeStreamsRequestData data = new DescribeStreamsRequestData()
            .setStreamId(streamId)
            .setNodeId(nodeId)
            .setTopicPartitions(topicPartitionMap.entrySet().stream().map(e -> new DescribeStreamsRequestData.TopicPartitionData()
                .setTopicName(e.getKey())
                .setPartitions(e.getValue().stream().map(partition -> new DescribeStreamsRequestData.PartitionData().setPartitionIndex(partition)).collect(Collectors.toList())))
                .collect(Collectors.toList()));
        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(bootstrapServer.id()),
            new DescribeStreamsRequest.Builder(data), now, true, 3000, null);

        ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, Time.SYSTEM);
        DescribeStreamsResponseData responseData = (DescribeStreamsResponseData) response.responseBody().data();

        Errors code = Errors.forCode(responseData.errorCode());
        if (Objects.requireNonNull(code) == Errors.NONE) {
            return responseData.streamMetadataList();
        }

        throw code.exception();
    }

    public List<StreamMetadata> getOpeningStreams(Node node, long nodeEpoch, boolean failoverMode) throws IOException {
        long now = Time.SYSTEM.milliseconds();
        GetOpeningStreamsRequestData data = new GetOpeningStreamsRequestData()
            .setNodeId(node.id())
            .setNodeEpoch(nodeEpoch)
            .setFailoverMode(failoverMode);
        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(bootstrapServer.id()),
            new GetOpeningStreamsRequest.Builder(data), now, true, 3000, null);

        ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, Time.SYSTEM);
        GetOpeningStreamsResponseData responseData = (GetOpeningStreamsResponseData) response.responseBody().data();

        Errors code = Errors.forCode(responseData.errorCode());
        if (Objects.requireNonNull(code) == Errors.NONE) {
            return responseData.streamMetadataList().stream()
                .map(m -> new StreamMetadata(m.streamId(), m.epoch(), m.startOffset(), m.endOffset(), StreamState.OPENED))
                .collect(Collectors.toList());
        }

        throw code.exception();
    }

    public void closeStream(long streamId, long epoch, int nodeId) throws IOException {
        long now = Time.SYSTEM.milliseconds();
        CloseStreamsRequestData.CloseStreamRequest request = new CloseStreamsRequestData.CloseStreamRequest()
            .setStreamId(streamId)
            .setStreamEpoch(epoch);

        CloseStreamsRequestData data = new CloseStreamsRequestData()
            .setNodeId(nodeId)
            .setNodeEpoch(now)
            .setCloseStreamRequests(List.of(request));

        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(bootstrapServer.id()),
            new CloseStreamsRequest.Builder(data), now, true, 3000, null);

        ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, Time.SYSTEM);
        CloseStreamsResponseData responseData = (CloseStreamsResponseData) response.responseBody().data();

        Errors code = Errors.forCode(responseData.errorCode());
        if (Objects.requireNonNull(code) == Errors.NONE) {
            return;
        }

        throw code.exception();
    }
}
