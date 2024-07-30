/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

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
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.ThreadUtils;
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

import static com.automq.stream.utils.FutureUtil.cause;

public class AlwaysSuccessClient implements Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlwaysSuccessClient.class);
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

    public AlwaysSuccessClient(Client client) {
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

    private class StreamClientImpl implements StreamClient {

        private final StreamClient streamClient;

        public StreamClientImpl(StreamClient streamClient) {
            this.streamClient = streamClient;
        }

        @Override
        public CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options) {
            CompletableFuture<Stream> cf = new CompletableFuture<>();
            createAndOpenStream0(options, cf);
            return cf;
        }

        private void createAndOpenStream0(CreateStreamOptions options, CompletableFuture<Stream> cf) {
            streamClient.createAndOpenStream(options).whenCompleteAsync((stream, ex) -> {
                FutureUtil.suppress(() -> {
                    if (ex != null) {
                        LOGGER.error("Create and open stream fail, retry later", ex);
                        streamManagerRetryScheduler.schedule(() -> createAndOpenStream0(options, cf), 3, TimeUnit.SECONDS);
                    } else {
                        cf.complete(new StreamImpl(stream));
                    }
                }, LOGGER);
            }, streamManagerCallbackExecutors);
        }

        @Override
        public CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions options) {
            CompletableFuture<Stream> cf = new CompletableFuture<>();
            openStream0(streamId, options, cf);
            return cf;
        }

        @Override
        public Optional<Stream> getStream(long streamId) {
            return streamClient.getStream(streamId);
        }

        public void shutdown() {
            streamClient.shutdown();
        }

        private void openStream0(long streamId, OpenStreamOptions options, CompletableFuture<Stream> cf) {
            streamClient.openStream(streamId, options).whenCompleteAsync((stream, ex) -> {
                FutureUtil.suppress(() -> {
                    if (ex != null) {
                        if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                            LOGGER.error("Open stream[{}]({}) fail, retry later", streamId, options.epoch(), ex);
                            streamManagerRetryScheduler.schedule(() -> openStream0(streamId, options, cf), 3, TimeUnit.SECONDS);
                        }
                    } else {
                        cf.complete(new StreamImpl(stream));
                    }
                }, LOGGER);
            }, generalCallbackExecutors);
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
        public CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint) {
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
            CompletableFuture<Void> cf = new CompletableFuture<>();
            close0(cf);
            return cf;
        }

        private void close0(CompletableFuture<Void> cf) {
            stream.close().whenCompleteAsync((rst, ex) -> FutureUtil.suppress(() -> {
                if (ex != null) {
                    if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                        LOGGER.error("Close stream[{}] failed, retry later", streamId(), ex);
                        generalRetryScheduler.schedule(() -> close0(cf), 3, TimeUnit.SECONDS);
                    }
                } else {
                    cf.complete(rst);
                }
            }, LOGGER), generalCallbackExecutors);
        }

        @Override
        public CompletableFuture<Void> destroy() {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            destroy0(cf);
            return cf;
        }

        private void destroy0(CompletableFuture<Void> cf) {
            stream.destroy().whenCompleteAsync((rst, ex) -> FutureUtil.suppress(() -> {
                if (ex != null) {
                    if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                        LOGGER.error("Destroy stream[{}] failed, retry later", streamId(), ex);
                        generalRetryScheduler.schedule(() -> destroy0(cf), 3, TimeUnit.SECONDS);
                    }
                } else {
                    cf.complete(rst);
                }
            }, LOGGER), generalCallbackExecutors);
        }
    }
}
