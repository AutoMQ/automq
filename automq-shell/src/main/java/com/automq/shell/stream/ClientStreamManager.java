/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.shell.stream;

import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
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

public class ClientStreamManager {

    private final NetworkClient networkClient;
    private final Node bootstrapServer;

    public ClientStreamManager(NetworkClient networkClient, Node bootstrapServer) throws IOException {
        this.networkClient = networkClient;
        this.bootstrapServer = bootstrapServer;
        connect(bootstrapServer);
    }

    private void connect(Node node) throws IOException {
        node = new Node(node.id(), node.host(), 9093);
        boolean ready = networkClient.isReady(node, Time.SYSTEM.milliseconds());
        if (ready) {
            return;
        }

        ready = NetworkClientUtils.awaitReady(networkClient, node, Time.SYSTEM, 1000);
        if (!ready) {
            throw new NetworkException("Failed to connect to the broker.");
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
