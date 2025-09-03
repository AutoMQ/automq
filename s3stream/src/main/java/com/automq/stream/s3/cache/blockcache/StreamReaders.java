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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class StreamReaders implements S3BlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReaders.class);
    private static final long STREAM_READER_EXPIRED_MILLS = TimeUnit.MINUTES.toMillis(1);
    private static final long STREAM_READER_EXPIRED_CHECK_INTERVAL_MILLS = TimeUnit.MINUTES.toMillis(1);
    private final Cache[] caches;
    private final Time time;
    private final DataBlockCache dataBlockCache;
    private final ObjectReaderFactory objectReaderFactory;

    private final ObjectManager objectManager;
    private final ObjectStorage objectStorage;

    public StreamReaders(long size, ObjectManager objectManager, ObjectStorage objectStorage,
        ObjectReaderFactory objectReaderFactory) {
        this(size, objectManager, objectStorage, objectReaderFactory, Systems.CPU_CORES, Time.SYSTEM);
    }

    public StreamReaders(long size, ObjectManager objectManager, ObjectStorage objectStorage,
        ObjectReaderFactory objectReaderFactory, int concurrency) {
        this(size, objectManager, objectStorage, objectReaderFactory, concurrency, Time.SYSTEM);
    }

    @SuppressWarnings("this-escape")
    public StreamReaders(long size, ObjectManager objectManager, ObjectStorage objectStorage,
        ObjectReaderFactory objectReaderFactory, int concurrency, Time time) {
        this.time = time;
        EventLoop[] eventLoops = new EventLoop[concurrency];
        for (int i = 0; i < concurrency; i++) {
            eventLoops[i] = new EventLoop("stream-reader-" + i);
        }
        this.caches = new Cache[concurrency];
        for (int i = 0; i < concurrency; i++) {
            caches[i] = new Cache(eventLoops[i]);
        }
        this.dataBlockCache = new DataBlockCache(size, eventLoops);

        this.objectReaderFactory = objectReaderFactory;
        this.objectManager = objectManager;
        this.objectStorage = objectStorage;

        Threads.COMMON_SCHEDULER.scheduleAtFixedRate(this::triggerExpiredStreamReaderCleanup,
            STREAM_READER_EXPIRED_CHECK_INTERVAL_MILLS,
            STREAM_READER_EXPIRED_CHECK_INTERVAL_MILLS,
            TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(TraceContext context, long streamId, long startOffset, long endOffset,
        int maxBytes) {
        Cache cache = caches[Math.abs((int) (streamId % caches.length))];
        return cache.read(streamId, startOffset, endOffset, maxBytes);
    }

    /**
     * Get the total number of active StreamReaders across all caches.
     * This method is intended for testing purposes only.
     */
    @VisibleForTesting
    int getActiveStreamReaderCount() {
        int total = 0;
        for (Cache cache : caches) {
            total += cache.getStreamReaderCount();
        }
        return total;
    }

    @VisibleForTesting
    void triggerExpiredStreamReaderCleanup() {
        for (Cache cache : caches) {
            cache.submitCleanupExpiredStreamReader();
        }
    }

    static class StreamReaderKey {
        final long streamId;
        final long startOffset;

        public StreamReaderKey(long streamId, long startOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            StreamReaderKey key = (StreamReaderKey) o;
            return streamId == key.streamId && startOffset == key.startOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamId, startOffset);
        }

        @Override
        public String toString() {
            return "StreamReaderKey{" +
                "streamId=" + streamId +
                ", startOffset=" + startOffset +
                '}';
        }
    }

    class Cache {
        private final EventLoop eventLoop;
        private final Map<StreamReaderKey, StreamReader> streamReaders = new HashMap<>();
        private long lastStreamReaderExpiredCheckTime;

        public Cache(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
            this.lastStreamReaderExpiredCheckTime = time.milliseconds();
        }

        public CompletableFuture<ReadDataBlock> read(long streamId, long startOffset,
            long endOffset,
            int maxBytes) {
            CompletableFuture<ReadDataBlock> cf = new CompletableFuture<>();
            eventLoop.execute(() -> {
                cleanupExpiredStreamReader();
                StreamReaderKey key = new StreamReaderKey(streamId, startOffset);
                StreamReader streamReader = streamReaders.remove(key);
                if (streamReader == null) {
                    streamReader = new StreamReader(streamId, startOffset, eventLoop, objectManager, objectReaderFactory, dataBlockCache, time);
                }
                StreamReader finalStreamReader = streamReader;
                CompletableFuture<ReadDataBlock> streamReadCf = streamReader.read(startOffset, endOffset, maxBytes)
                    .whenComplete((rst, ex) -> {
                        if (ex != null) {
                            LOGGER.error("read {} [{}, {}), maxBytes: {} from block cache fail", streamId, startOffset, endOffset, maxBytes, ex);
                            finalStreamReader.close();
                        } else {
                            // when two stream read progress is the same, only one stream reader can be retained
                            StreamReader oldStreamReader = streamReaders.put(new StreamReaderKey(streamId, finalStreamReader.nextReadOffset()), finalStreamReader);
                            if (oldStreamReader != null) {
                                oldStreamReader.close();
                            }
                        }
                    });
                FutureUtil.propagate(streamReadCf, cf);
            });
            return cf;
        }

        private void submitCleanupExpiredStreamReader() {
            eventLoop.execute(this::cleanupExpiredStreamReader);
        }

        /**
         * Get the number of StreamReaders in this cache.
         * This method is intended for testing purposes only.
         */
        @VisibleForTesting
        int getStreamReaderCount() {
            return streamReaders.size();
        }

        private void cleanupExpiredStreamReader() {
            long now = time.milliseconds();
            if (now > lastStreamReaderExpiredCheckTime + STREAM_READER_EXPIRED_CHECK_INTERVAL_MILLS) {
                lastStreamReaderExpiredCheckTime = now;
                Iterator<Map.Entry<StreamReaderKey, StreamReader>> it = streamReaders.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<StreamReaderKey, StreamReader> entry = it.next();
                    StreamReader streamReader = entry.getValue();
                    if (now > streamReader.lastAccessTimestamp() + STREAM_READER_EXPIRED_MILLS) {
                        streamReader.close();
                        it.remove();
                    }
                }
            }
        }
    }
}
