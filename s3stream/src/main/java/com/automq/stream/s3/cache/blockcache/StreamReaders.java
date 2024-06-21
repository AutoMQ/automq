/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.ObjectReaderLRUCache;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.threads.EventLoop;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamReaders implements S3BlockCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReaders.class);
    private static final int MAX_OBJECT_READER_SIZE = 100 * 1024 * 1024; // 100MB;
    private static final long STREAM_READER_EXPIRED_MILLS = TimeUnit.MINUTES.toMillis(1);
    private static final long STREAM_READER_EXPIRED_CHECK_INTERVAL_MILLS = TimeUnit.MINUTES.toMillis(1);
    private final Cache[] caches;
    private final DataBlockCache dataBlockCache;
    private final ObjectReaderLRUCache objectReaders;
    private final ObjectReaderFactory objectReaderFactory;

    private final ObjectManager objectManager;
    private final ObjectStorage objectStorage;

    public StreamReaders(long size, ObjectManager objectManager, ObjectStorage objectStorage) {
        this(size, objectManager, objectStorage, Systems.CPU_CORES);
    }

    public StreamReaders(long size, ObjectManager objectManager, ObjectStorage objectStorage, int concurrency) {
        EventLoop[] eventLoops = new EventLoop[concurrency];
        for (int i = 0; i < concurrency; i++) {
            eventLoops[i] = new EventLoop("stream-reader-" + i);
        }
        this.caches = new Cache[concurrency];
        for (int i = 0; i < concurrency; i++) {
            caches[i] = new Cache(eventLoops[i]);
        }
        this.dataBlockCache = new DataBlockCache(size, eventLoops);
        this.objectReaders = new ObjectReaderLRUCache(MAX_OBJECT_READER_SIZE);
        this.objectReaderFactory = new ObjectReaderFactory();

        this.objectManager = objectManager;
        this.objectStorage = objectStorage;
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(TraceContext context, long streamId, long startOffset, long endOffset,
        int maxBytes) {
        Cache cache = caches[Math.abs((int) (streamId % caches.length))];
        return cache.read(streamId, startOffset, endOffset, maxBytes);
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
        private long lastStreamReaderExpiredCheckTime = System.currentTimeMillis();

        public Cache(EventLoop eventLoop) {
            this.eventLoop = eventLoop;
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
                    streamReader = new StreamReader(streamId, startOffset, eventLoop, objectManager, objectReaderFactory, dataBlockCache);
                }
                StreamReader finalStreamReader = streamReader;
                CompletableFuture<ReadDataBlock> streamReadCf = streamReader.read(startOffset, endOffset, maxBytes)
                    .whenComplete((rst, ex) -> {
                        if (ex != null) {
                            LOGGER.error("read {} [{}, {}), maxBytes: {} from block cache fail", streamId, startOffset, endOffset, maxBytes, ex);
                        } else {
                            // when two stream read progress is the same, only one stream reader can be retained
                            streamReaders.put(new StreamReaderKey(streamId, finalStreamReader.nextReadOffset()), finalStreamReader);
                        }
                    });
                FutureUtil.propagate(streamReadCf, cf);
            });
            return cf;
        }

        private void cleanupExpiredStreamReader() {
            long now = System.currentTimeMillis();
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

    class ObjectReaderFactory implements Function<S3ObjectMetadata, ObjectReader> {
        @Override
        public synchronized ObjectReader apply(S3ObjectMetadata metadata) {
            ObjectReader objectReader = objectReaders.get(metadata.objectId());
            if (objectReader == null) {
                objectReader = ObjectReader.reader(metadata, objectStorage);
                objectReaders.put(metadata.objectId(), objectReader);
            }
            return objectReader.retain();
        }
    }
}
