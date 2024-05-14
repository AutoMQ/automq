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

package org.apache.kafka.cli.stream;

import com.automq.stream.s3.metadata.StreamMetadata;
import com.automq.stream.s3.metadata.StreamState;
import com.automq.stream.s3.streams.StreamManager;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.GetOpeningStreamsRequestData;
import org.apache.kafka.common.message.GetOpeningStreamsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.GetOpeningStreamsRequest;
import org.apache.kafka.common.utils.Time;

public class ClientStreamManager implements StreamManager {

    private final NetworkClient networkClient;

    public ClientStreamManager(NetworkClient networkClient) {
        this.networkClient = networkClient;
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        return null;
    }

    private void connect(Node node) throws IOException {
        boolean ready = networkClient.isReady(node, Time.SYSTEM.milliseconds());
        if (ready) {
            return;
        }

        ready = NetworkClientUtils.awaitReady(networkClient, new Node(1, "localhost", 9093), Time.SYSTEM, 1000);
        if (!ready) {
            throw new NetworkException("Failed to connect to the broker.");
        }
    }

    public List<StreamMetadata> getOpeningStreams(Node node, long nodeEpoch, boolean failoverMode) throws IOException {
        long now = System.currentTimeMillis();
        connect(node);

        GetOpeningStreamsRequestData data = new GetOpeningStreamsRequestData()
            .setNodeId(node.id())
            .setNodeEpoch(nodeEpoch)
            .setFailoverMode(failoverMode);
        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(node.id()),
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

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Long> createStream(Map<String, String> tags) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long epoch) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
