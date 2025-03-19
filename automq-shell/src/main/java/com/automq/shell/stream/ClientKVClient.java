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

package com.automq.shell.stream;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.DeleteKVsResponseData;
import org.apache.kafka.common.message.GetKVsRequestData;
import org.apache.kafka.common.message.GetKVsResponseData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.s3.DeleteKVsRequest;
import org.apache.kafka.common.requests.s3.GetKVsRequest;
import org.apache.kafka.common.requests.s3.PutKVsRequest;
import org.apache.kafka.common.utils.Time;

import com.automq.shell.metrics.S3MetricsExporter;
import com.automq.stream.api.KeyValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

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

    public KeyValue.Value getKV(String key) throws IOException {
        long now = Time.SYSTEM.milliseconds();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ClientKVClient]: Get KV: {}", key);
        }

        GetKVsRequestData data = new GetKVsRequestData()
            .setGetKeyRequests(List.of(new GetKVsRequestData.GetKVRequest().setKey(key)));

        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(bootstrapServer.id()),
            new GetKVsRequest.Builder(data), now, true, 3000, null);

        ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, Time.SYSTEM);
        GetKVsResponseData responseData = (GetKVsResponseData) response.responseBody().data();

        Errors code = Errors.forCode(responseData.errorCode());
        if (Objects.requireNonNull(code) == Errors.NONE) {
            return KeyValue.Value.of(responseData.getKVResponses().get(0).value());
        }

        throw code.exception();
    }

    public KeyValue.Value putKV(String key, byte[] value) throws IOException {
        long now = Time.SYSTEM.milliseconds();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ClientKVClient]: put KV: {}", key);
        }

        PutKVsRequestData data = new PutKVsRequestData()
            .setPutKVRequests(List.of(new PutKVsRequestData.PutKVRequest().setKey(key).setValue(value)));

        ClientRequest clientRequest = networkClient.newClientRequest(String.valueOf(bootstrapServer.id()),
            new PutKVsRequest.Builder(data), now, true, 3000, null);

        ClientResponse response = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, Time.SYSTEM);
        PutKVsResponseData responseData = (PutKVsResponseData) response.responseBody().data();

        Errors code = Errors.forCode(responseData.errorCode());
        if (Objects.requireNonNull(code) == Errors.NONE) {
            return KeyValue.Value.of(responseData.putKVResponses().get(0).value());
        }

        throw code.exception();
    }

    public KeyValue.Value deleteKV(String key) throws IOException {
        long now = Time.SYSTEM.milliseconds();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[ClientKVClient]: Delete KV: {}", key);
        }

        DeleteKVsRequestData data = new DeleteKVsRequestData()
            .setDeleteKVRequests(List.of(new DeleteKVsRequestData.DeleteKVRequest().setKey(key)));

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
