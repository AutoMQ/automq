package org.apache.kafka.server.util;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;

import java.util.concurrent.CompletableFuture;

public interface AsyncSender extends AutoCloseable {

    <T extends AbstractRequest> CompletableFuture<ClientResponse> sendRequest(
        Node node,
        AbstractRequest.Builder<T> requestBuilder
    );

    @Override
    void close();
}