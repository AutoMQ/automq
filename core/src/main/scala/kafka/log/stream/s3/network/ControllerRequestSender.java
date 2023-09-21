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

package kafka.log.stream.s3.network;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.log.stream.s3.network.request.BatchRequest;
import kafka.log.stream.s3.network.request.WrapRequest;
import kafka.server.BrokerServer;
import kafka.server.BrokerToControllerChannelManager;
import kafka.server.ControllerRequestCompletionHandler;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.s3.AbstractBatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ControllerRequestSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerRequestSender.class);
    private final RetryPolicyContext retryPolicyContext;
    private final BrokerServer brokerServer;
    private final BrokerToControllerChannelManager channelManager;

    private final ScheduledExecutorService retryService;

    private final ConcurrentHashMap<ApiKeys, RequestAccumulator> requestAccumulatorMap;

    private final int brokerId;

    private final long brokerEpoch;

    public ControllerRequestSender(BrokerServer brokerServer, RetryPolicyContext retryPolicyContext) {
        this.retryPolicyContext = retryPolicyContext;
        this.brokerServer = brokerServer;
        this.channelManager = brokerServer.clientToControllerChannelManager();
        this.retryService = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("controller-request-retry-sender"));
        this.requestAccumulatorMap = new ConcurrentHashMap<>();
        this.brokerId = brokerServer.config().brokerId();
        this.brokerEpoch = brokerServer.config().brokerEpoch();
    }

    public void send(RequestTask task) {
        task.sendHit();
        if (task.request instanceof BatchRequest) {
            batchSend(task);
            return;
        }
        singleSend(task);
    }

    private void batchSend(RequestTask task) {
        requestAccumulatorMap.computeIfAbsent(task.request.apiKey(),
            this::createRequestAccumulator).send(task);
    }

    private void singleSend(RequestTask task) {
        Builder builder = task.request.toRequestBuilder();
        RequestCtx requestCtx = new RequestCtx() {
            @Override
            void onSuccess(AbstractResponse response) {
                try {

                    ResponseHandleResult result = (ResponseHandleResult) task.responseHandler.apply(response);
                    if (result.retry()) {
                        retryTask(task);
                        return;
                    }
                    task.complete(result.getResponse());
                } catch (Exception e) {
                    task.completeExceptionally(e);
                }
            }

            @Override
            void onError(Throwable e) {
                task.completeExceptionally(e);
            }
        };
        sendRequest(builder, requestCtx);
    }

    private void sendRequest(Builder requestBuilder, RequestCtx ctx) {
        channelManager.sendRequest(requestBuilder, new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                // TODO: add timeout retry policy
                LOGGER.error("Timeout while creating stream");
                ctx.onError(new TimeoutException("Timeout while creating stream"));
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.authenticationException() != null) {
                    LOGGER.error("Authentication error while sending request: {}", requestBuilder, response.authenticationException());
                    ctx.onError(response.authenticationException());
                    return;
                }
                if (response.versionMismatch() != null) {
                    LOGGER.error("Version mismatch while sending request: {}", requestBuilder, response.versionMismatch());
                    ctx.onError(response.versionMismatch());
                    return;
                }
                AbstractResponse resp = response.responseBody();
                ctx.onSuccess(resp);
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

    public abstract class RequestCtx {

        abstract void onSuccess(AbstractResponse response);

        abstract void onError(Throwable ex);
    }

    public class RequestAccumulator {
        AtomicBoolean inflight = new AtomicBoolean(false);
        BlockingQueue<RequestTask> requestQueue = new LinkedBlockingQueue<>();

        public RequestAccumulator() {
        }

        void send(RequestTask task) {
            if (task != null) {
                requestQueue.add(task);
            }
            if (!requestQueue.isEmpty() && inflight.compareAndSet(false, true)) {
                send0();
            }
        }

        void send0() {
            if (requestQueue.isEmpty()) {
                return;
            }
            List<RequestTask> inflight = new ArrayList<>();
            requestQueue.drainTo(inflight);
            Builder builder = inflight.get(0).request.toRequestBuilder();
            inflight.stream().map(task -> (BatchRequest) task.request).skip(1).forEach(req -> req.addSubRequest(builder));
            RequestCtx requestCtx = new RequestCtx() {
                @Override
                void onSuccess(AbstractResponse response) {
                    if (!(response instanceof AbstractBatchResponse)) {
                        LOGGER.error("Unexpected response type: {} while sending request: {}",
                            response.getClass().getSimpleName(), builder);
                        onError(new RuntimeException("Unexpected response type while sending request"));
                        return;
                    }
                    AbstractBatchResponse resp = (AbstractBatchResponse) response;
                    List subResponses = resp.subResponses();
                    if (subResponses.size() != inflight.size()) {
                        LOGGER.error("Response size: {} not match request size: {}", subResponses.size(), inflight.size());
                        onError(new RuntimeException("Response size not match request size"));
                        return;
                    }
                    for (int index = 0; index < subResponses.size(); index++) {
                        RequestTask task = inflight.get(index);
                        try {
                            ResponseHandleResult result = (ResponseHandleResult) task.responseHandler.apply(subResponses.get(index));
                            if (result.retry()) {
                                retryTask(task);
                                continue;
                            }
                            task.complete(result.getResponse());
                        } catch (Exception e) {
                            task.completeExceptionally(e);
                        }
                    }
                    if (RequestAccumulator.this.inflight.compareAndSet(true, false)) {
                        send(null);
                    }
                }

                @Override
                void onError(Throwable e) {
                    inflight.forEach(t -> t.future.completeExceptionally(e));
                    if (RequestAccumulator.this.inflight.compareAndSet(true, false)) {
                        send(null);
                    }
                }

            };
            ControllerRequestSender.this.sendRequest(builder, requestCtx);
        }

    }

    private RequestAccumulator createRequestAccumulator(ApiKeys type) {
        return new RequestAccumulator();
    }

    public static class RequestTask<T, R> {

        private final WrapRequest request;
        private final CompletableFuture<R> future;
        private final Function<T, ResponseHandleResult<R>> responseHandler;
        private int sendCount;

        public RequestTask(WrapRequest request, CompletableFuture<R> future,
            Function<T, ResponseHandleResult<R>> responseHandler) {
            this.request = request;
            this.future = future;
            this.responseHandler = responseHandler;
        }

        public WrapRequest request() {
            return request;
        }

        public void sendHit() {
            sendCount++;
        }

        public int sendCount() {
            return sendCount;
        }

        public void complete(R result) {
            future.complete(result);
        }

        public void completeExceptionally(Throwable throwable) {
            future.completeExceptionally(throwable);
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
