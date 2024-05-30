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

import com.automq.shell.metrics.S3MetricsExporter;
import com.automq.stream.api.KeyValue;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.DeleteKVsRequest;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientKVClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetricsExporter.class);

    private final NetworkClient networkClient;
    private final Node bootstrapServer;

    public ClientKVClient(NetworkClient networkClient, Node bootstrapServer) throws IOException {
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

    public KeyValue.Value delKV(KeyValue.Key key) throws IOException {
        long now = Time.SYSTEM.milliseconds();

        LOGGER.trace("[ClientKVClient]: Delete KV: {}", key);
        DeleteKVsRequestData data = new DeleteKVsRequestData()
            .setDeleteKVRequests(List.of(new DeleteKVsRequestData.DeleteKVRequest().setKey(key.get())));

        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(bootstrapServer.id()),
            new DeleteKVsRequest.Builder(data), now, true, 3000, null);

        ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, Time.SYSTEM);
        DeleteKVsResponseData responseData = (DeleteKVsResponseData) response.responseBody().data();

        Errors code = Errors.forCode(responseData.errorCode());
        if (Objects.requireNonNull(code) == Errors.NONE) {
            return KeyValue.Value.of(responseData.deleteKVResponses().get(0).value());
        }

        throw code.exception();
    }
}
