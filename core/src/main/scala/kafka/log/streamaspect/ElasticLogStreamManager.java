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

import com.automq.stream.api.OpenStreamOptions;
import com.automq.stream.api.Stream;
import com.automq.stream.api.StreamClient;
import com.automq.stream.utils.AsyncLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.automq.stream.utils.LockUtils.runInLock;

/**
 * Owns the streams used by one elastic log, including existing streams opened by {@link #create} and streams that
 * remain lazy until first use. Creation opens all existing streams concurrently and compensating-closes every
 * successful open before propagating a peer open failure.
 */
public class ElasticLogStreamManager {
    private static final Logger LOGGER = AsyncLogger.wrap(LoggerFactory.getLogger(ElasticLogStreamManager.class));
    private final Map<String, Stream> streamMap = new ConcurrentHashMap<>();
    private final StreamClient streamClient;
    private final int replicaCount;
    private final long epoch;
    private final Map<String, String> tags;
    private final boolean snapshotRead;
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * inner listener for created LazyStream
     */
    private final LazyStreamStreamEventListener innerListener = new LazyStreamStreamEventListener();
    /**
     * outer register listener
     */
    private ElasticStreamEventListener outerListener;

    private ElasticLogStreamManager(StreamClient streamClient, int replicaCount, long epoch,
        Map<String, String> tags, boolean snapshotRead) {
        this.streamClient = streamClient;
        this.replicaCount = replicaCount;
        this.epoch = epoch;
        this.tags = tags;
        this.snapshotRead = snapshotRead;
    }

    /**
     * Creates a manager and asynchronously opens all existing streams, compensating partial opens before failure.
     *
     * @return a future containing the initialized manager
     */
    public static CompletableFuture<ElasticLogStreamManager> create(Map<String, Long> streams,
        StreamClient streamClient, int replicaCount, long epoch, Map<String, String> tags, boolean snapshotRead) {
        ElasticLogStreamManager manager = new ElasticLogStreamManager(
            streamClient, replicaCount, epoch, tags, snapshotRead);
        CompletableFuture<CompletableFuture<ElasticLogStreamManager>> openFuture =
            manager.openExistingStreams(streams).handle((nil, exception) -> {
                if (exception == null) {
                    return CompletableFuture.completedFuture(manager);
                }
                Throwable openFailure = unwrap(exception);
                return manager.close().<ElasticLogStreamManager>handle((ignored, cleanupException) -> {
                    if (cleanupException != null) {
                        addCleanupFailure(openFailure, cleanupException);
                    }
                    throw new CompletionException(openFailure);
                });
            });
        return openFuture.thenCompose(future -> future);
    }

    private CompletableFuture<Void> openExistingStreams(Map<String, Long> streams) {
        List<CompletableFuture<Void>> openFutures = new ArrayList<>(streams.size());
        for (Map.Entry<String, Long> entry : streams.entrySet()) {
            String name = entry.getKey();
            long streamId = entry.getValue();
            if (streamId == LazyStream.NOOP_STREAM_ID) {
                streamMap.put(name, newLazyStream(name));
            } else {
                openFutures.add(openExistingStream(name, streamId).thenAccept(stream -> streamMap.put(name, stream)));
            }
        }
        return CompletableFuture.allOf(openFutures.toArray(new CompletableFuture[0]));
    }

    private static void addCleanupFailure(Throwable openFailure, Throwable cleanupException) {
        Throwable cleanupFailure = unwrap(cleanupException);
        if (cleanupFailure != openFailure) {
            openFailure.addSuppressed(cleanupFailure);
        }
    }

    public Stream getStream(String name) throws IOException {
        if (streamMap.containsKey(name)) {
            return streamMap.get(name);
        }
        if (snapshotRead) {
            throw new IllegalStateException("snapshotRead mode can not create stream");
        }
        LazyStream lazyStream = newLazyStream(name);
        // pre-create log and tim stream cause of their high frequency of use.
        boolean warmUp = "log".equals(name) || "tim".equals(name);
        if (warmUp) {
            lazyStream.warmUp();
        }
        streamMap.put(name, lazyStream);
        return lazyStream;
    }

    public Map<String, Stream> streams() {
        return Collections.unmodifiableMap(streamMap);
    }

    /**
     * Opens and registers an existing stream unless the same stream is already registered under the name. This method
     * serializes callers and blocks with {@link CompletableFuture#join()} until the open completes. An exceptional open
     * is propagated as an unchecked completion failure.
     *
     * @param name stream name
     * @param streamId existing stream ID, or {@link LazyStream#NOOP_STREAM_ID} if it has not been created
     */
    public void openIfNotExist(String name, long streamId) {
        runInLock(lock, () -> {
            Stream current = streamMap.get(name);
            if (current != null && current.streamId() == streamId) {
                return;
            }
            Stream stream = streamId == LazyStream.NOOP_STREAM_ID
                ? newLazyStream(name)
                : openExistingStream(name, streamId).join();
            streamMap.put(name, stream);
        });
    }

    public void setListener(ElasticStreamEventListener listener) {
        this.outerListener = listener;
    }

    /**
     * Starts closing all registered streams concurrently.
     *
     * @return a future that completes when every stream close completes, or exceptionally if any close fails
     */
    public CompletableFuture<Void> close() {
        return CompletableFuture.allOf(streamMap.values().stream().map(Stream::close).toArray(CompletableFuture[]::new));
    }

    private LazyStream newLazyStream(String name) {
        LazyStream lazyStream = new LazyStream(name, streamClient, replicaCount, epoch, tags);
        lazyStream.setListener(innerListener);
        return lazyStream;
    }

    private CompletableFuture<Stream> openExistingStream(String name, long streamId) {
        OpenStreamOptions.Builder options = OpenStreamOptions.builder().epoch(epoch).tags(tags);
        if (snapshotRead) {
            options.readWriteMode(OpenStreamOptions.ReadWriteMode.SNAPSHOT_READ);
        }
        return streamClient.openStream(streamId, options.build()).thenApply(stream -> {
            LOGGER.info("opened existing stream: streamId={}, epoch={}, name={}", streamId, epoch, name);
            return stream;
        });
    }

    private static Throwable unwrap(Throwable throwable) {
        Throwable cause = throwable;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    class LazyStreamStreamEventListener implements ElasticStreamEventListener {
        @Override
        public void onEvent(long streamId, ElasticStreamMetaEvent event) {
            Optional.ofNullable(outerListener).ifPresent(listener -> listener.onEvent(streamId, event));
        }
    }
}
