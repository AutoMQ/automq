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

package kafka.log.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kafka.log.s3.network.ControllerRequestSender;
import kafka.log.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.s3.network.ControllerRequestSender.ResponseHandleResult;
import kafka.log.s3.network.ControllerRequestSender.RetryPolicyContext;
import kafka.server.BrokerServer;
import kafka.server.BrokerToControllerChannelManager;
import kafka.server.ControllerRequestCompletionHandler;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.CreateStreamRequestData;
import org.apache.kafka.common.message.CreateStreamResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.s3.CreateStreamRequest;
import org.apache.kafka.common.requests.s3.CreateStreamResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

@Timeout(40)
@Tag("S3Unit")
public class ControllerRequestSenderTest {

    private BrokerServer brokerServer;
    private BrokerToControllerChannelManager channelManager;
    private ControllerRequestSender requestSender;
    private RetryPolicyContext retryPolicyContext;

    @BeforeEach
    public void setUp() {
        brokerServer = Mockito.mock(BrokerServer.class);
        channelManager = Mockito.mock(BrokerToControllerChannelManager.class);
        Mockito.when(brokerServer.clientToControllerChannelManager()).thenReturn(channelManager);
        retryPolicyContext = new RetryPolicyContext(2, 100L);
        requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
    }

    @Test
    public void testBasic() {
        // first time inner-error, second time success
        Mockito.doAnswer(ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamResponse response = new CreateStreamResponse(new CreateStreamResponseData()
                .setErrorCode(Errors.STREAM_INNER_ERROR.code()));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        }).doAnswer(ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamResponse response = new CreateStreamResponse(new CreateStreamResponseData()
                .setErrorCode(Errors.NONE.code())
                .setStreamId(13L));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        }).when(channelManager).sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));

        CreateStreamRequest.Builder request = new CreateStreamRequest.Builder(
            new CreateStreamRequestData()
        );
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponseData, Long> task = new RequestTask<>(future, request, CreateStreamResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamId());
                default:
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        assertDoesNotThrow(() -> {
            Long streamId = future.get(1, TimeUnit.SECONDS);
            assertEquals(13L, streamId);
        });
        Mockito.verify(channelManager, Mockito.times(2))
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
    }

    @Test
    public void testMaxRetry() {
        Answer failedAns = ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamResponse response = new CreateStreamResponse(new CreateStreamResponseData()
                .setErrorCode(Errors.STREAM_INNER_ERROR.code()));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        };
        Answer successAns = ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamResponse response = new CreateStreamResponse(new CreateStreamResponseData()
                .setErrorCode(Errors.NONE.code())
                .setStreamId(13L));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        };
        // failed 3 times, success 1 time
        Mockito.doAnswer(failedAns).doAnswer(failedAns).doAnswer(failedAns).doAnswer(successAns).when(channelManager)
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
        CreateStreamRequest.Builder request = new CreateStreamRequest.Builder(
            new CreateStreamRequestData()
        );
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponseData, Long> task = new RequestTask<>(future, request, CreateStreamResponseData.class, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamId());
                default:
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        assertThrows(ExecutionException.class, () -> {
            future.get(1, TimeUnit.SECONDS);
        });
        Mockito.verify(channelManager, Mockito.times(3))
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
    }
}
