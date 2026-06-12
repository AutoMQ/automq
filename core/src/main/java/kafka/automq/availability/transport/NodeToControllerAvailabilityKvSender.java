/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.availability.transport;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.s3.DeleteKVsRequest;
import org.apache.kafka.common.requests.s3.DeleteKVsResponse;
import org.apache.kafka.common.requests.s3.PutKVsRequest;
import org.apache.kafka.common.requests.s3.PutKVsResponse;
import org.apache.kafka.server.ControllerRequestCompletionHandler;
import org.apache.kafka.server.NodeToControllerChannelManager;

import java.util.concurrent.CompletableFuture;

/**
 * Node-to-Controller sender for availability KV writes issued from Broker runtime loops.
 */
public class NodeToControllerAvailabilityKvSender implements AvailabilityKvRequestSender {
    private final NodeToControllerChannelManager channelManager;

    public NodeToControllerAvailabilityKvSender(NodeToControllerChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @Override
    public CompletableFuture<Void> put(PutKVsRequestData request) {
        return send(new PutKVsRequest.Builder(request), response -> {
            if (!(response.responseBody() instanceof PutKVsResponse)) {
                return "unexpected response " + response.responseBody();
            }
            PutKVsResponse putResponse = (PutKVsResponse) response.responseBody();
            Errors topLevel = Errors.forCode(putResponse.data().errorCode());
            if (topLevel != Errors.NONE) {
                return "top-level error " + topLevel;
            }
            return putResponse.subResponses().stream()
                .map(subResponse -> Errors.forCode(subResponse.errorCode()))
                .filter(error -> error != Errors.NONE)
                .findFirst()
                .map(error -> "sub-response error " + error)
                .orElse(null);
        });
    }

    @Override
    public CompletableFuture<Void> delete(DeleteKVsRequestData request) {
        return send(new DeleteKVsRequest.Builder(request), response -> {
            if (!(response.responseBody() instanceof DeleteKVsResponse)) {
                return "unexpected response " + response.responseBody();
            }
            DeleteKVsResponse deleteResponse = (DeleteKVsResponse) response.responseBody();
            Errors topLevel = Errors.forCode(deleteResponse.data().errorCode());
            if (topLevel != Errors.NONE) {
                return "top-level error " + topLevel;
            }
            return deleteResponse.subResponses().stream()
                .map(subResponse -> Errors.forCode(subResponse.errorCode()))
                .filter(error -> error != Errors.NONE && error != Errors.KEY_NOT_EXIST)
                .findFirst()
                .map(error -> "sub-response error " + error)
                .orElse(null);
        });
    }

    private CompletableFuture<Void> send(AbstractRequest.Builder<?> builder, ResponseErrorExtractor errorExtractor) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        channelManager.sendRequest(builder, new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                result.completeExceptionally(new RuntimeException("availability KV request timed out"));
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.authenticationException() != null) {
                    result.completeExceptionally(response.authenticationException());
                    return;
                }
                if (response.versionMismatch() != null) {
                    result.completeExceptionally(response.versionMismatch());
                    return;
                }
                String error = errorExtractor.extract(response);
                if (error == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(new RuntimeException(error));
                }
            }
        });
        return result;
    }

    private interface ResponseErrorExtractor {
        String extract(ClientResponse response);
    }
}
