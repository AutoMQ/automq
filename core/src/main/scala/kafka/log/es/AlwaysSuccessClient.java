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

package kafka.log.es;

import kafka.log.es.api.AppendResult;
import kafka.log.es.api.Client;
import kafka.log.es.api.CreateStreamOptions;
import kafka.log.es.api.ElasticStreamClientException;
import kafka.log.es.api.ErrorCode;
import kafka.log.es.api.FetchResult;
import kafka.log.es.api.KVClient;
import kafka.log.es.api.OpenStreamOptions;
import kafka.log.es.api.RecordBatch;
import kafka.log.es.api.Stream;
import kafka.log.es.api.StreamClient;
import org.apache.kafka.common.errors.es.SlowFetchHintException;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class AlwaysSuccessClient implements Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlwaysSuccessClient.class);
    public static final Set<Short> HALT_ERROR_CODES = Set.of(ErrorCode.EXPIRED_STREAM_EPOCH, ErrorCode.STREAM_ALREADY_CLOSED);
    public static final long DEFAULT_SLOW_FETCH_TIMEOUT_MILLIS = 10;
    private final ScheduledExecutorService streamManagerRetryScheduler = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("stream-manager-retry-%d", true));
    private final ExecutorService streamManagerCallbackExecutors = Executors.newFixedThreadPool(1,
            ThreadUtils.createThreadFactory("stream-manager-callback-executor-%d", true));
    private final ScheduledExecutorService appendRetryScheduler = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("append-retry-scheduler-%d", true));
    private final ScheduledExecutorService fetchRetryScheduler = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("fetch-retry-scheduler-%d", true));
    private final ScheduledExecutorService generalRetryScheduler = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("general-retry-scheduler-%d", true));
    private final ExecutorService generalCallbackExecutors = Executors.newFixedThreadPool(4,
            ThreadUtils.createThreadFactory("general-callback-scheduler-%d", true));
    private final ExecutorService appendCallbackExecutors = Executors.newFixedThreadPool(4,
            ThreadUtils.createThreadFactory("append-callback-scheduler-%d", true));
    private final ExecutorService fetchCallbackExecutors = Executors.newFixedThreadPool(4,
            ThreadUtils.createThreadFactory("fetch-callback-scheduler-%d", true));
    private final ScheduledExecutorService delayFetchScheduler = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("fetch-delayer-%d", true));
    private final StreamClient streamClient;
    private final KVClient kvClient;
    private final Delayer delayer;
    /**
     * The flag to indicate if the callback of append is async.
     * It is generally true, but for test cases, it is set to false. In test cases, we aim to ensure that
     * the committed offset is promptly updated right after appending. Otherwise, the subsequent fetch request may fail
     * due to the delay in updating the committed offset.
     */
    private final boolean appendCallbackAsync;
    private final long slowFetchTimeoutMillis;

    public AlwaysSuccessClient(Client client) {
        this(client, true, DEFAULT_SLOW_FETCH_TIMEOUT_MILLIS);
    }

    public AlwaysSuccessClient(Client client, boolean appendCallbackAsync) {
        this(client, appendCallbackAsync, DEFAULT_SLOW_FETCH_TIMEOUT_MILLIS);
    }

    public AlwaysSuccessClient(Client client, boolean appendCallbackAsync, long slowFetchTimeoutMillis) {
        this.streamClient = new StreamClientImpl(client.streamClient());
        this.kvClient = client.kvClient();
        this.delayer = new Delayer(delayFetchScheduler);
        this.appendCallbackAsync = appendCallbackAsync;
        this.slowFetchTimeoutMillis = slowFetchTimeoutMillis;
    }

    @Override
    public StreamClient streamClient() {
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return kvClient;
    }

    public void shutdownNow() {
        streamClient.shutdown();
        streamManagerRetryScheduler.shutdownNow();
        streamManagerCallbackExecutors.shutdownNow();
        appendRetryScheduler.shutdownNow();
        fetchRetryScheduler.shutdownNow();
        generalRetryScheduler.shutdownNow();
        generalCallbackExecutors.shutdownNow();
        appendCallbackExecutors.shutdownNow();
        fetchCallbackExecutors.shutdownNow();
        delayFetchScheduler.shutdownNow();
    }

    /**
     * Check if the exception is a ElasticStreamClientException with a halt error code.
     *
     * @param t the exception
     * @return true if the exception is a ElasticStreamClientException with a halt error code, otherwise false
     */
    private static boolean shouldHalt(Throwable t) {
        if (!(t instanceof ElasticStreamClientException)) {
            return false;
        }
        ElasticStreamClientException e = (ElasticStreamClientException) t;
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
        if (!shouldHalt(t)) {
            return false;
        }
        waitingFuture.completeExceptionally(new IOException("No operations allowed on stream"));
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
        private final Map<String, CompletableFuture<FetchResult>> holdUpFetchingFutureMap = new ConcurrentHashMap<>();

        public StreamImpl(Stream stream) {
            this.stream = stream;
        }

        @Override
        public long streamId() {
            return stream.streamId();
        }

        @Override
        public long startOffset() {
            return stream.startOffset();
        }

        @Override
        public long nextOffset() {
            return stream.nextOffset();
        }

        @Override
        public CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
            CompletableFuture<AppendResult> cf = new CompletableFuture<>();
            if (appendCallbackAsync) {
                append0(recordBatch, cf);
            } else {
                append0WithSyncCallback(recordBatch, cf);
            }
            return cf;
        }

        private void append0(RecordBatch recordBatch, CompletableFuture<AppendResult> cf) {
            stream.append(recordBatch).whenCompleteAsync((rst, ex) -> FutureUtil.suppress(() -> {
                if (ex != null) {
                    if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                        LOGGER.error("Appending to stream[{}] failed, retry later", streamId(), ex);
                        appendRetryScheduler.schedule(() -> append0(recordBatch, cf), 3, TimeUnit.SECONDS);
                    }
                } else {
                    cf.complete(rst);
                }
            }, LOGGER), appendCallbackExecutors);
        }

        /**
         * Append to stream without using async callback threadPools.
         * <strong> Used for tests only.</strong>
         *
         * @param recordBatch
         * @param cf
         */
        private void append0WithSyncCallback(RecordBatch recordBatch, CompletableFuture<AppendResult> cf) {
            stream.append(recordBatch).whenComplete((rst, ex) -> FutureUtil.suppress(() -> {
                if (ex != null) {
                    if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                        LOGGER.error("Appending to stream[{}] failed, retry later", streamId(), ex);
                        appendRetryScheduler.schedule(() -> append0(recordBatch, cf), 3, TimeUnit.SECONDS);
                    }
                } else {
                    cf.complete(rst);
                }
            }, LOGGER));
        }

        /**
         * Get a new CompletableFuture with a {@link SlowFetchHintException} if not otherwise completed before the given timeout.
         *
         * @param id        the id of rawFuture in holdUpFetchingFutureMap
         * @param rawFuture the raw future
         * @param timeout   how long to wait before completing exceptionally with a SlowFetchHintException, in units of {@code unit}
         * @param unit      a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
         * @return a new CompletableFuture with completed results of the rawFuture if the raw future is done before timeout, otherwise a new
         * CompletableFuture with a {@link SlowFetchHintException}
         */
        private CompletableFuture<FetchResult> timeoutAndStoreFuture(String id,
                                                                     CompletableFuture<FetchResult> rawFuture, long timeout,
                                                                     TimeUnit unit) {
            if (unit == null) {
                throw new NullPointerException();
            }

            if (rawFuture.isDone()) {
                return rawFuture;
            }

            final CompletableFuture<FetchResult> cf = new CompletableFuture<>();
            rawFuture.whenComplete(new CompleteFetchingFutureAndCancelTimeoutCheck(delayer.delay(() -> {
                if (rawFuture == null) {
                    return;
                }

                // If rawFuture is done, then complete the cf with the result of rawFuture.
                if (rawFuture.isDone()) {
                    rawFuture.whenComplete((result, exception) -> {
                        if (exception != null) {
                            cf.completeExceptionally(exception);
                        } else {
                            cf.complete(result);
                        }
                    });
                } else { // else, complete the cf with a SlowFetchHintException and store the rawFuture for slow fetching.
                    holdUpFetchingFutureMap.putIfAbsent(id, rawFuture);
                    cf.completeExceptionally(new SlowFetchHintException());
                }
            }, timeout, unit), cf));
            return cf;
        }

        @Override
        public CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint) {
            String holdUpKey = startOffset + "-" + endOffset + "-" + maxBytesHint;
            CompletableFuture<FetchResult> cf = new CompletableFuture<>();
            // If this thread is not marked, then just fetch data.
            if (!SeparateSlowAndQuickFetchHint.isMarked()) {
                if (holdUpFetchingFutureMap.containsKey(holdUpKey)) {
                    holdUpFetchingFutureMap.remove(holdUpKey).thenAccept(cf::complete);
                } else {
                    fetch0(startOffset, endOffset, maxBytesHint, cf);
                }
            } else {
                CompletableFuture<FetchResult> firstFetchFuture = new CompletableFuture<>();
                fetch0(startOffset, endOffset, maxBytesHint, firstFetchFuture);
                // Try to have a quick fetch. If the first fetching is timeout, then complete with SlowFetchHintException.
                timeoutAndStoreFuture(holdUpKey, firstFetchFuture, slowFetchTimeoutMillis, TimeUnit.MILLISECONDS)
                        .whenComplete((rst, ex) -> FutureUtil.suppress(() -> {
                            if (ex != null) {
                                if (ex instanceof SlowFetchHintException) {
                                    LOGGER.debug("Fetch stream[{}] [{},{}) timeout for {} ms, retry later with slow fetching", streamId(), startOffset, endOffset, DEFAULT_SLOW_FETCH_TIMEOUT_MILLIS);
                                    cf.completeExceptionally(ex);
                                } else if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                                    cf.completeExceptionally(ex);
                                }
                            } else {
                                cf.complete(rst);
                            }
                        }, LOGGER));
            }
            return cf;
        }

        private void fetch0(long startOffset, long endOffset, int maxBytesHint, CompletableFuture<FetchResult> cf) {
            stream.fetch(startOffset, endOffset, maxBytesHint).whenCompleteAsync((rst, ex) -> {
                FutureUtil.suppress(() -> {
                    if (ex != null) {
                        if (!maybeHaltAndCompleteWaitingFuture(ex, cf)) {
                            LOGGER.error("Fetch stream[{}] [{},{}) fail, retry later", streamId(), startOffset, endOffset, ex);
                            fetchRetryScheduler.schedule(() -> fetch0(startOffset, endOffset, maxBytesHint, cf), 3, TimeUnit.SECONDS);
                        }
                    } else {
                        cf.complete(rst);
                    }
                }, LOGGER);
            }, fetchCallbackExecutors);
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


    static final class Delayer {
        private final ScheduledExecutorService delayFetchScheduler;

        public Delayer(ScheduledExecutorService delayFetchScheduler) {
            this.delayFetchScheduler = delayFetchScheduler;
        }

        public ScheduledFuture<?> delay(Runnable command, long delay,
                                        TimeUnit unit) {
            return delayFetchScheduler.schedule(command, delay, unit);
        }
    }

    /**
     * A BiConsumer that completes the FetchResult future and cancels the timeout check task.
     */
    static final class CompleteFetchingFutureAndCancelTimeoutCheck implements BiConsumer<FetchResult, Throwable> {
        /**
         * A ScheduledFuture that represents the timeout check task.
         */
        final ScheduledFuture<?> f;
        /**
         * A CompletableFuture waiting for the fetching result.
         */
        final CompletableFuture<FetchResult> waitingFuture;

        CompleteFetchingFutureAndCancelTimeoutCheck(ScheduledFuture<?> f, CompletableFuture<FetchResult> waitingFuture) {
            this.f = f;
            this.waitingFuture = waitingFuture;
        }

        public void accept(FetchResult result, Throwable ex) {
            // cancels the timeout check task.
            if (ex == null && f != null && !f.isDone())
                f.cancel(false);

            // completes the waiting future right now.
            if (ex == null) {
                waitingFuture.complete(result);
            } else {
                waitingFuture.completeExceptionally(ex);
            }
        }
    }
}
