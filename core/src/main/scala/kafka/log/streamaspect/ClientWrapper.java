/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.log.streamaspect;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.s3.NodeEpochExpiredException;
import org.apache.kafka.common.errors.s3.NodeEpochNotExistException;
import org.apache.kafka.common.errors.s3.NodeFencedException;
import org.apache.kafka.common.utils.ThreadUtils;

import com.automq.stream.api.AppendResult;
import com.automq.stream.api.Client;
import com.automq.stream.api.CreateStreamOptions;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.api.exceptions.ErrorCode;
import com.automq.stream.api.exceptions.FastReadFailFastException;
import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import static com.automq.stream.utils.FutureUtil.cause;

public class ClientWrapper implements Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientWrapper.class);
    public static final Set<Short> HALT_ERROR_CODES = Set.of(
        ErrorCode.EXPIRED_STREAM_EPOCH,
        ErrorCode.STREAM_ALREADY_CLOSED,
        ErrorCode.OFFSET_OUT_OF_RANGE_BOUNDS
    );
    private final ScheduledExecutorService streamManagerRetryScheduler = Executors.newScheduledThreadPool(1,
        ThreadUtils.createThreadFactory("stream-manager-retry-%d", true));
    private final ExecutorService streamManagerCallbackExecutors = Executors.newFixedThreadPool(1,
        ThreadUtils.createThreadFactory("stream-manager-callback-executor-%d", true));
    private final ScheduledExecutorService generalRetryScheduler = Executors.newScheduledThreadPool(1,
        ThreadUtils.createThreadFactory("general-retry-scheduler-%d", true));
    private final ExecutorService generalCallbackExecutors = Executors.newFixedThreadPool(4,
        ThreadUtils.createThreadFactory("general-callback-scheduler-%d", true));
    private final Client innerClient;
    private volatile StreamClient streamClient;
    private final HashedWheelTimer fetchTimeout = new HashedWheelTimer(
        ThreadUtils.createThreadFactory("fetch-timeout-%d", true),
        1, TimeUnit.SECONDS, 512);

    public ClientWrapper(Client client) {
        this.innerClient = client;
    }

    @Override
    public StreamClient streamClient() {
        if (streamClient != null) {
            return streamClient;
        }
        synchronized (this) {
            if (streamClient != null) {
                return streamClient;
            }
            streamClient = new StreamClientImpl(innerClient.streamClient());
        }
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return innerClient.kvClient();
    }

    @Override
    public CompletableFuture<FailoverResponse> failover(FailoverRequest request) {
        return innerClient.failover(request);
    }

    @Override
    public void start() {
        innerClient.start();
    }

    @Override
    public void shutdown() {
        innerClient.shutdown();
        streamManagerRetryScheduler.shutdownNow();
        streamManagerCallbackExecutors.shutdownNow();
        generalRetryScheduler.shutdownNow();
        generalCallbackExecutors.shutdownNow();
    }

    /**
     * Check if the exception is a ElasticStreamClientException with a halt error code.
     *
     * @param t the exception
     * @return true if the exception is a ElasticStreamClientException with a halt error code, otherwise false
     */
    private static boolean shouldHalt(Throwable t) {
        // s3stream will retry all retryable exceptions, so we should halt all exceptions.
        // AlwaysSuccessClient could be removed later.
        if (t instanceof KafkaException) {
            return true;
        }
        if (!(t instanceof StreamClientException)) {
            return false;
        }
        StreamClientException e = (StreamClientException) t;
        return HALT_ERROR_CODES.contains((short) e.getCode());
    }

    /**
     * Maybe halt and complete the waiting future if the exception is a ElasticStreamClientException with a halt error code.
     *
     * @param t             the exception
     * @param waitingFuture the waiting future
     * @return true if the waiting future is completed, otherwise false
     */
    private static boolean maybeHaltAndCompleteWaitingFuture(Throwable t, CompletableFuture<?> waitingFuture) {
        t = cause(t);
        if (!shouldHalt(t)) {
            return false;
        }
        waitingFuture.completeExceptionally(new IOException("No operations allowed on stream", t));
        return true;
    }

    private static <T> CompletableFuture<T> failureHandle(CompletableFuture<T> cf) {
        return cf.whenComplete((rst, ex) -> {
            if (ex != null) {
                ex = FutureUtil.cause(ex);
                if (ex instanceof NodeEpochExpiredException
                    || ex instanceof NodeEpochNotExistException
                    || ex instanceof NodeFencedException) {
                    LOGGER.error("The node is fenced, force shutdown the node", ex);
                    //noinspection CallToPrintStackTrace
                    ex.printStackTrace();
                    Runtime.getRuntime().halt(1);
                }
            }
        });
    }

    private class StreamClientImpl implements StreamClient {

        private final StreamClient streamClient;

        public StreamClientImpl(StreamClient streamClient) {
            this.streamClient = streamClient;
        }

        @Override
        public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
            return failureHandle(streamClient.createAndOpenStream(options).thenApplyAsync(rst -> rst, streamManagerCallbackExecutors));
        }

        @Override
        public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions options) {
            return failureHandle(streamClient.openStream(streamId, options).thenApplyAsync(rst -> rst, streamManagerCallbackExecutors));
        }

        @Override
        public Optional<Stream> getStream(long streamId) {
            return streamClient.getStream(streamId);
        }

        public void shutdown() {
            streamClient.shutdown();
        }
    }

    private class StreamImpl implements Stream {
        private final Stream stream;

        public StreamImpl(Stream stream) {
            this.stream = stream;
        }

        @Override
        public long streamId() {
            return stream.streamId();
        }

        @Override
        public long streamEpoch() {
            return stream.streamEpoch();
        }

        @Override
        public long startOffset() {
            return stream.startOffset();
        }

        @Override
        public long confirmOffset() {
            return stream.confirmOffset();
        }

        @Override
        public long nextOffset() {
            return stream.nextOffset();
        }

        @Override
        public CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch) {
            return stream.append(context, recordBatch).whenComplete((rst, ex) -> {
                if (ex != null) {
                    LOGGER.error("Appending to stream[{}] failed, retry later", streamId(), ex);
                }
            });
        }

        @Override
        public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset,
            int maxBytesHint) {
            CompletableFuture<FetchResult> cf = new CompletableFuture<>();
            Timeout timeout = fetchTimeout.newTimeout(t -> LOGGER.warn("fetch timeout, stream[{}] [{}, {})", streamId(), startOffset, endOffset), 1, TimeUnit.MINUTES);
            stream.fetch(context, startOffset, endOffset, maxBytesHint).whenComplete((rst, e) -> FutureUtil.suppress(() -> {
                timeout.cancel();
                Throwable ex = FutureUtil.cause(e);
                if (ex != null) {
                    if (!(ex instanceof FastReadFailFastException)) {
                        LOGGER.error("Fetch stream[{}] [{},{}) fail", streamId(), startOffset, endOffset, ex);
                    }
                    cf.completeExceptionally(ex);
                } else {
                    cf.complete(rst);
                }
            }, LOGGER));
            return cf;
        }

        @Override
        public CompletableFuture<Void> trim(long newStartOffset) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            trim0(newStartOffset, cf);
            return cf;
        }

        private void trim0(long newStartOffset, CompletableFuture<Void> cf) {
            stream.trim(newStartOffset).whenCompleteAsync((rst, ex) -> {
                FutureUtil.suppress(() -> {
                    if (ex != null) {
                        if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                            LOGGER.error("Trim stream[{}] (new offset = {}) failed, retry later", streamId(), newStartOffset, ex);
                            generalRetryScheduler.schedule(() -> trim0(newStartOffset, cf), 3, TimeUnit.SECONDS);
                        }
                    } else {
                        cf.complete(rst);
                    }
                }, LOGGER);
            }, generalCallbackExecutors);
        }

        @Override
        public CompletableFuture<Void> close() {
            return failureHandle(stream.close().thenApplyAsync(nil -> nil, streamManagerCallbackExecutors));
        }

        @Override
        public CompletableFuture<Void> destroy() {
            return failureHandle(stream.destroy().thenApplyAsync(nil -> nil, streamManagerCallbackExecutors));
        }
    }
}
