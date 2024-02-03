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

package kafka.log.stream.s3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.network.ControllerRequestSender.RequestTask;
import kafka.log.stream.s3.network.ControllerRequestSender.RetryPolicyContext;
import kafka.log.stream.s3.network.request.BatchRequest;
import kafka.log.stream.s3.network.request.WrapRequest;
import kafka.server.BrokerServer;
import kafka.server.BrokerToControllerChannelManager;
import kafka.server.ControllerRequestCompletionHandler;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.CreateStreamsRequestData;
import org.apache.kafka.common.message.CreateStreamsRequestData.CreateStreamRequest;
import org.apache.kafka.common.message.CreateStreamsResponseData;
import org.apache.kafka.common.message.CreateStreamsResponseData.CreateStreamResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.s3.CreateStreamsRequest;
import org.apache.kafka.common.requests.s3.CreateStreamsResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static kafka.log.stream.s3.network.ControllerRequestSender.ResponseHandleResult;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        Mockito.when(brokerServer.newBrokerToControllerChannelManager(anyString(), anyInt())).thenReturn(channelManager);
        KafkaConfig config = Mockito.mock(KafkaConfig.class);
        Mockito.when(config.brokerId()).thenReturn(1);
        Mockito.when(config.nodeEpoch()).thenReturn(1L);
        Mockito.when(brokerServer.config()).thenReturn(config);
        retryPolicyContext = new RetryPolicyContext(2, 100L);
        requestSender = new ControllerRequestSender(brokerServer, retryPolicyContext);
    }

    @Test
    public void testBasic() {
        // first time inner-error, second time success
        Mockito.doAnswer(ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamsResponse response = new CreateStreamsResponse(new CreateStreamsResponseData()
                .setCreateStreamResponses(List.of(new CreateStreamResponse()
                    .setErrorCode(Errors.STREAM_INNER_ERROR.code()))));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        }).doAnswer(ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamsResponse response = new CreateStreamsResponse(new CreateStreamsResponseData()
                .setCreateStreamResponses(List.of(new CreateStreamResponse()
                    .setErrorCode(Errors.NONE.code())
                    .setStreamId(13L))));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        }).when(channelManager).sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));

        WrapRequest req = new BatchRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.CREATE_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new CreateStreamsRequest.Builder(
                    new CreateStreamsRequestData()
                        .setCreateStreamRequests(List.of(new CreateStreamRequest()))
                );
            }

            @Override
            public Builder addSubRequest(Builder builder) {
                CreateStreamsRequest.Builder realBuilder = (CreateStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(new CreateStreamRequest());
                return realBuilder;
            }
        };
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponse, Long> task = new RequestTask<CreateStreamResponse, Long>(req, future, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamId());
                default:
                    return ResponseHandleResult.withRetry();
            }
        });
        this.requestSender.send(task);
        Assertions.assertDoesNotThrow(() -> {
            Long streamId = future.get(1, TimeUnit.SECONDS);
            Assertions.assertEquals(13L, streamId);
        });
        verify(channelManager, times(2))
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
    }

    @Test
    public void testMaxRetry() {
        Answer failedAns = ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamsResponse response = new CreateStreamsResponse(new CreateStreamsResponseData()
                .setCreateStreamResponses(List.of(new CreateStreamResponse()
                    .setErrorCode(Errors.STREAM_INNER_ERROR.code()))));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        };
        Answer successAns = ink -> {
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            CreateStreamsResponse response = new CreateStreamsResponse(new CreateStreamsResponseData()
                .setCreateStreamResponses(List.of(new CreateStreamResponse()
                    .setErrorCode(Errors.NONE.code())
                    .setStreamId(13L))));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            handler.onComplete(clientResponse);
            return null;
        };
        // failed 3 times, success 1 time
        Mockito.doAnswer(failedAns).doAnswer(failedAns).doAnswer(failedAns).doAnswer(successAns).when(channelManager)
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
        WrapRequest req = new BatchRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.CREATE_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new CreateStreamsRequest.Builder(
                    new CreateStreamsRequestData()
                        .setCreateStreamRequests(List.of(new CreateStreamRequest()))
                );
            }

            @Override
            public Builder addSubRequest(Builder builder) {
                CreateStreamsRequest.Builder realBuilder = (CreateStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(new CreateStreamRequest());
                return realBuilder;
            }
        };
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponse, Long> task = new RequestTask<CreateStreamResponse, Long>(req, future, resp -> {
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
        verify(channelManager, times(3))
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
    }

    @Test
    public void testBatchSend() throws Exception {
        Mockito.doAnswer(ink -> {
            CreateStreamsRequest.Builder requestBuilder = ink.getArgument(0);
            CreateStreamsRequestData data = requestBuilder.build().data();
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            assertEquals(1, data.createStreamRequests().size());
            List<CreateStreamResponse> responses = new ArrayList<>();
            for (int i = 0; i < data.createStreamRequests().size(); i++) {
                responses.add(new CreateStreamResponse().setErrorCode(Errors.NONE.code()));
            }
            CreateStreamsResponse response = new CreateStreamsResponse(new CreateStreamsResponseData()
                .setCreateStreamResponses(responses));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            // first time sleep 3s
            CompletableFuture.delayedExecutor(3, TimeUnit.SECONDS).execute(() -> handler.onComplete(clientResponse));
            return null;
        }).doAnswer(ink -> {
            CreateStreamsRequest.Builder requestBuilder = ink.getArgument(0);
            CreateStreamsRequestData data = requestBuilder.build().data();
            ControllerRequestCompletionHandler handler = ink.getArgument(1);
            assertEquals(3, data.createStreamRequests().size());
            List<CreateStreamResponse> responses = new ArrayList<>();
            for (int i = 0; i < data.createStreamRequests().size(); i++) {
                responses.add(new CreateStreamResponse().setErrorCode(Errors.NONE.code()));
            }
            CreateStreamsResponse response = new CreateStreamsResponse(new CreateStreamsResponseData()
                .setCreateStreamResponses(responses));
            ClientResponse clientResponse = new ClientResponse(
                null, null, null, -1, -1, false, null, null, response);
            // first time we get first request with 1 sub-request
            handler.onComplete(clientResponse);
            return null;
        }).when(channelManager).sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));

        // first time send and mock that it takes 3s to get response
        CompletableFuture future0 = send();
        List<CompletableFuture> allFutures = new ArrayList<>();
        allFutures.add(future0);
        Thread.sleep(1000);
        for (int i = 0; i < 3; i++) {
            allFutures.add(send());
        }
        assertDoesNotThrow(() -> {
            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).get(4, TimeUnit.SECONDS);
        });
        verify(channelManager, times(2))
            .sendRequest(any(AbstractRequest.Builder.class), any(ControllerRequestCompletionHandler.class));
    }

    public static final ExecutorService EXECUTOR_SERVICE = java.util.concurrent.Executors.newFixedThreadPool(4);

    private CompletableFuture send() {
        WrapRequest req = new BatchRequest() {
            @Override
            public ApiKeys apiKey() {
                return ApiKeys.CREATE_STREAMS;
            }

            @Override
            public Builder toRequestBuilder() {
                return new CreateStreamsRequest.Builder(
                    new CreateStreamsRequestData()).addSubRequest(new CreateStreamRequest());
            }

            @Override
            public Builder addSubRequest(Builder builder) {
                CreateStreamsRequest.Builder realBuilder = (CreateStreamsRequest.Builder) builder;
                realBuilder.addSubRequest(new CreateStreamRequest());
                return realBuilder;
            }
        };
        CompletableFuture<Long> future = new CompletableFuture<>();
        RequestTask<CreateStreamResponse, Long> task = new RequestTask<CreateStreamResponse, Long>(req, future, resp -> {
            switch (Errors.forCode(resp.errorCode())) {
                case NONE:
                    return ResponseHandleResult.withSuccess(resp.streamId());
                default:
                    return ResponseHandleResult.withRetry();
            }
        });
        EXECUTOR_SERVICE.submit(() -> {
            this.requestSender.send(task);
        });
        return future;
    }
}
