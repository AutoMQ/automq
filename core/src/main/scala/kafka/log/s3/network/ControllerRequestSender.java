/*
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

package kafka.log.s3.network;

import java.util.concurrent.CompletableFuture;
import kafka.server.BrokerServer;
import kafka.server.BrokerToControllerChannelManager;
import kafka.server.ControllerRequestCompletionHandler;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerRequestSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRequestSender.class);

    private final BrokerServer brokerServer;
    private BrokerToControllerChannelManager channelManager;

    public ControllerRequestSender(BrokerServer brokerServer) {
        this.brokerServer = brokerServer;
        this.channelManager = brokerServer.clientToControllerChannelManager();
    }

    public <T extends AbstractRequest, R extends ApiMessage> CompletableFuture<R> send(AbstractRequest.Builder<T> requestBuilder,
        Class<R> responseDataType) {
        CompletableFuture<R> cf = new CompletableFuture<>();
        LOGGER.debug("Sending request {}", requestBuilder);
        channelManager.sendRequest(requestBuilder, new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                // TODO: add timeout retry policy
                LOGGER.error("Timeout while creating stream");
                cf.completeExceptionally(new TimeoutException("Timeout while creating stream"));
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.authenticationException() != null) {
                    LOGGER.error("Authentication error while sending request: {}", requestBuilder, response.authenticationException());
                    cf.completeExceptionally(response.authenticationException());
                    return;
                }
                if (response.versionMismatch() != null) {
                    LOGGER.error("Version mismatch while sending request: {}", requestBuilder, response.versionMismatch());
                    cf.completeExceptionally(response.versionMismatch());
                    return;
                }
                if (!responseDataType.isInstance(response.responseBody().data())) {
                    LOGGER.error("Unexpected response type: {} while sending request: {}",
                        response.responseBody().data().getClass().getSimpleName(), requestBuilder);
                    cf.completeExceptionally(new RuntimeException("Unexpected response type while sending request"));
                }
                cf.complete((R) response.responseBody().data());
            }
        });
        return cf;
    }
}
