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
package kafka.log.stream.s3.metadata;

import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.S3StreamsMetadataImage;
import org.apache.kafka.image.S3StreamsMetadataImage.RangeGetter;
import org.apache.kafka.metadata.stream.S3Object;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.LRUCache;
import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.index.lazy.StreamSetObjectRangeIndex;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.utils.FutureUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

/**
 * Extracted from {@link StreamMetadataManager} (#2731 review — the class had grown large
 * enough to warrant its own file).
 */
public class DefaultRangeGetter implements RangeGetter {
    private final S3ObjectsImage objectsImage;
    private final ObjectReaderFactory objectReaderFactory;
    public static final StreamIdBloomFilter STREAM_ID_BLOOM_FILTER = new StreamIdBloomFilter(20 * 1024 * 1024);
    private S3StreamsMetadataImage.GetObjectsContext getObjectsContext;

    public DefaultRangeGetter(S3ObjectsImage objectsImage,
        ObjectReaderFactory objectReaderFactory) {
        this.objectsImage = objectsImage;
        this.objectReaderFactory = objectReaderFactory;
    }

    @Override
    public void attachGetObjectsContext(S3StreamsMetadataImage.GetObjectsContext ctx) {
        this.getObjectsContext = ctx;
    }

    public static void updateIndex(ObjectReader reader, Long nodeId, Long streamId) {
        reader.basicObjectInfo().thenAccept(info -> {
            Long objectId = reader.metadata().objectId();
            List<StreamOffsetRange> streamOffsetRanges = info.indexBlock().streamOffsetRanges();

            STREAM_ID_BLOOM_FILTER.update(objectId, streamOffsetRanges);

            StreamSetObjectRangeIndex.getInstance().updateIndex(objectId, nodeId, streamId, streamOffsetRanges);
        }).whenComplete((v, e) -> reader.release());
    }

    @Override
    public CompletableFuture<Optional<StreamOffsetRange>> find(long objectId, long streamId, long nodeId, long orderId) {
        S3Object s3Object = objectsImage.getObjectMetadata(objectId);
        if (s3Object == null) {
            return FutureUtil.failedFuture(new IllegalArgumentException("Cannot find object metadata for object: " + objectId));
        }

        boolean mightContain = STREAM_ID_BLOOM_FILTER.mightContain(objectId, streamId);
        if (!mightContain) {
            getObjectsContext.bloomFilterSkipSSOCount++;
            return CompletableFuture.completedFuture(Optional.empty());
        }

        getObjectsContext.searchSSOStreamOffsetRangeCount++;
        // The reader will be release after the find operation
        @SuppressWarnings("resource")
        ObjectReader reader = objectReaderFactory.get(new S3ObjectMetadata(objectId, s3Object.getObjectSize(), s3Object.getAttributes()));
        CompletableFuture<Optional<StreamOffsetRange>> cf = reader.basicObjectInfo()
            .thenApply(info -> info.indexBlock().findStreamOffsetRange(streamId));
        cf.whenCompleteAsync((rst, ex) -> {
            if (rst.isEmpty()) {
                getObjectsContext.searchSSORangeEmpty.add(1);
            }
            updateIndex(reader, nodeId, streamId);
        }, StreamSetObjectRangeIndex.UPDATE_INDEX_THREAD_POOL);
        return cf;
    }

    @Override
    public CompletableFuture<ByteBuf> readNodeRangeIndex(long nodeId) {
        ObjectStorage storage = objectReaderFactory.getObjectStorage();
        return storage.read(new ReadOptions().bucket(ObjectAttributes.MATCH_ALL_BUCKET), ObjectUtils.genIndexKey(0, nodeId));
    }

    public static class StreamIdBloomFilter {
        public static final double DEFAULT_FPP = 0.01;
        private final LRUCache<Long/*objectId*/, SizedBloomFilter> cache = new LRUCache<>();
        private final long maxBloomFilterSize;
        private long cacheSize = 0;

        public StreamIdBloomFilter(long maxBloomFilterCacheSize) {
            this.maxBloomFilterSize = maxBloomFilterCacheSize;
        }

        public synchronized void maintainCacheSize() {
            while (cacheSize > maxBloomFilterSize) {
                Map.Entry<Long, SizedBloomFilter> entry = cache.pop();
                if (entry != null) {
                    cacheSize -= Long.BYTES + entry.getValue().sizeInBytes;
                }
            }
        }

        public synchronized boolean mightContain(long objectId, long streamId) {
            SizedBloomFilter bloomFilter = cache.get(objectId);
            if (bloomFilter == null) {
                return true; // treat as exist
            }

            cache.touchIfExist(objectId);
            return bloomFilter.filter.mightContain(streamId);
        }

        public synchronized void removeObject(long objectId) {
            SizedBloomFilter filter = cache.get(objectId);
            if (cache.remove(objectId)) {
                cacheSize -= Long.BYTES + filter.sizeInBytes;
            }
        }

        public synchronized void update(long objectId, List<StreamOffsetRange> streamOffsetRanges) {
            if (cache.containsKey(objectId)) {
                return;
            }

            SizedBloomFilter bloomFilter = SizedBloomFilter.create(streamOffsetRanges.size(), DEFAULT_FPP);

            streamOffsetRanges.forEach(range -> bloomFilter.filter.put(range.streamId()));
            cache.put(objectId, bloomFilter);
            cacheSize += Long.BYTES + bloomFilter.sizeInBytes;

            maintainCacheSize();
        }

        public synchronized long sizeInBytes() {
            return this.cacheSize;
        }

        public synchronized int objectNum() {
            return this.cache.size();
        }

        public synchronized void clear() {
            cache.clear();
        }
    }

    /**
     * Pairs a Guava {@code BloomFilter<Long>} with its approximate bit-array size.
     *
     * Guava's BloomFilter has no public byte-size accessor (unlike the ORC BloomFilter this
     * replaces, which exposed {@code sizeInBytes()} directly) — this repo declares no ORC
     * dependency anywhere, so pulling one in just for this one class was not worth it. The size
     * is computed with the same formula Guava's own {@code BloomFilter.create} uses internally
     * to size its bit array ({@code BloomFilter.optimalNumOfBits(n, p)} — package-private,
     * verified against Guava 32.0.1-jre's actual source, the version this repo pins in
     * gradle/dependencies.gradle; {@link DefaultRangeGetterTest} asserts this reimplementation
     * against that real method via reflection). It covers the dominant, n-scaling term (the bit
     * array itself) but not the small constant overhead of the wrapping objects (hash function
     * count, funnel/strategy references, array padding) — close enough for a soft LRU eviction
     * budget (maintainCacheSize above), not used for correctness.
     */
    private static final class SizedBloomFilter {
        final BloomFilter<Long> filter;
        final long sizeInBytes;

        private SizedBloomFilter(BloomFilter<Long> filter, long sizeInBytes) {
            this.filter = filter;
            this.sizeInBytes = sizeInBytes;
        }

        static SizedBloomFilter create(int expectedInsertions, double fpp) {
            BloomFilter<Long> filter = BloomFilter.create(Funnels.longFunnel(), Math.max(expectedInsertions, 1), fpp);
            long numBits = optimalNumOfBits(Math.max(expectedInsertions, 1), fpp);
            long sizeInBytes = (numBits + 7) / 8;
            return new SizedBloomFilter(filter, sizeInBytes);
        }

        // Same formula Guava's BloomFilter uses internally (BloomFilter.optimalNumOfBits),
        // which is package-private there — reimplemented here since we only need the size.
        private static long optimalNumOfBits(long n, double p) {
            if (p == 0) {
                p = Double.MIN_VALUE;
            }
            return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
        }
    }
}
