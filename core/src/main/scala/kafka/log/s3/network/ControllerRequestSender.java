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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import kafka.server.BrokerServer;
import kafka.server.BrokerToControllerChannelManager;
import kafka.server.ControllerRequestCompletionHandler;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class ControllerRequestSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRequestSender.class);
    private final RetryPolicyContext retryPolicyContext;
    private final BrokerServer brokerServer;
    private final BrokerToControllerChannelManager channelManager;

    private final ScheduledExecutorService retryService;

    public ControllerRequestSender(BrokerServer brokerServer, RetryPolicyContext retryPolicyContext) {
        this.retryPolicyContext = retryPolicyContext;
        this.brokerServer = brokerServer;
        this.channelManager = brokerServer.clientToControllerChannelManager();
        this.retryService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("controller-request-retry-sender"));
    }

    public void send(RequestTask task) {
        Builder requestBuilder = task.requestBuilder;
        Class responseDataType = task.responseDataType;
        task.sendHit();
        LOGGER.trace("Sending task: {}", task);
        channelManager.sendRequest(requestBuilder, new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                // TODO: add timeout retry policy
                LOGGER.error("Timeout while creating stream");
                task.completeExceptionally(new TimeoutException("Timeout while creating stream"));
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.authenticationException() != null) {
                    LOGGER.error("Authentication error while sending request: {}", requestBuilder, response.authenticationException());
                    task.completeExceptionally(response.authenticationException());
                    return;
                }
                if (response.versionMismatch() != null) {
                    LOGGER.error("Version mismatch while sending request: {}", requestBuilder, response.versionMismatch());
                    task.completeExceptionally(response.versionMismatch());
                    return;
                }
                if (!responseDataType.isInstance(response.responseBody().data())) {
                    LOGGER.error("Unexpected response type: {} while sending request: {}",
                        response.responseBody().data().getClass().getSimpleName(), requestBuilder);
                    task.completeExceptionally(new RuntimeException("Unexpected response type while sending request"));
                }
                ApiMessage data = response.responseBody().data();
                try {
                    ResponseHandleResult result = (ResponseHandleResult) task.responseHandler.apply(data);
                    if (result.retry()) {
                        retryTask(task);
                        return;
                    }
                    task.complete(result.getResponse());
                } catch (Exception e) {
                    task.completeExceptionally(e);
                }
            }
        });
    }

    private void retryTask(RequestTask task) {
        if (task.sendCount() > retryPolicyContext.maxRetryCount) {
            LOGGER.error("Task: {}, retry count exceed max retry count: {}", task, retryPolicyContext.maxRetryCount);
            task.completeExceptionally(new RuntimeException("Retry count exceed max retry count: "));
            return;
        }
        long delay = retryPolicyContext.retryBaseDelayMs * (1 << (task.sendCount() - 1));
        LOGGER.warn("Retry task: {}, delay : {} ms", task, delay);
        retryService.schedule(() -> send(task), delay, TimeUnit.MILLISECONDS);
    }

    public static class RequestTask<T/*controller response data type*/, Z/*upper response data type*/> {

        private final CompletableFuture<Z> cf;

        private final AbstractRequest.Builder requestBuilder;

        private final Class<T> responseDataType;

        /**
         * The response handler is used to determine whether the response is valid or retryable.
         */
        private final Function<T, ResponseHandleResult<Z>> responseHandler;

        private int sendCount;

        public RequestTask(CompletableFuture<Z> future, AbstractRequest.Builder requestBuilder, Class<T> responseDataType,
            Function<T, ResponseHandleResult<Z>> responseHandler) {
            this.cf = future;
            this.requestBuilder = requestBuilder;
            this.responseDataType = responseDataType;
            this.responseHandler = responseHandler;
        }

        public CompletableFuture<Z> cf() {
            return cf;
        }

        public void sendHit() {
            sendCount++;
        }

        public int sendCount() {
            return sendCount;
        }

        public void complete(Z result) {
            cf.complete(result);
        }

        public void completeExceptionally(Throwable throwable) {
            cf.completeExceptionally(throwable);
        }

        @Override
        public String toString() {
            return "RequestTask{" +
                "requestBuilder=" + requestBuilder +
                ", responseDataType=" + responseDataType +
                ", sendCount=" + sendCount +
                '}';
        }
    }

    public static class ResponseHandleResult<R> {

        private final boolean retry;
        private final R response;

        private ResponseHandleResult(boolean retry, R response) {
            this.retry = retry;
            this.response = response;
        }

        public static <R> ResponseHandleResult<R> withRetry() {
            return new ResponseHandleResult<>(true, null);
        }

        public static <R> ResponseHandleResult<R> withSuccess(R response) {
            return new ResponseHandleResult<>(false, response);
        }

        public boolean retry() {
            return retry;
        }

        public R getResponse() {
            return response;
        }
    }

    public static class RetryPolicyContext {
        private int maxRetryCount;
        private long retryBaseDelayMs;

        public RetryPolicyContext(int maxRetryCount, long retryBaseDelayMs) {
            this.maxRetryCount = maxRetryCount;
            this.retryBaseDelayMs = retryBaseDelayMs;
        }

        public int maxRetryCount() {
            return maxRetryCount;
        }

        public long retryBaseDelayMs() {
            return retryBaseDelayMs;
        }

        public void setMaxRetryCount(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
        }

        public void setRetryBaseDelayMs(long retryBaseDelayMs) {
            this.retryBaseDelayMs = retryBaseDelayMs;
        }
    }
}
