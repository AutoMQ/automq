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

package com.automq.stream.s3;

import com.automq.stream.WrappedByteBuf;
import com.automq.stream.utils.Systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.UnpooledByteBufAllocator;

public class ByteBufAlloc {

    public static final boolean MEMORY_USAGE_DETECT = Boolean.parseBoolean(System.getenv("AUTOMQ_MEMORY_USAGE_DETECT"));
    public static final int MEMORY_USAGE_DETECT_INTERVAL = Systems.getEnvInt("AUTOMQ_MEMORY_USAGE_DETECT_TIME_INTERVAL", 60000);

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufAlloc.class);

    public static final int DEFAULT = 0;
    public static final int ENCODE_RECORD = 1;
    public static final int DECODE_RECORD = 2;
    public static final int WRITE_INDEX_BLOCK = 3;
    public static final int READ_INDEX_BLOCK = 4;
    public static final int WRITE_DATA_BLOCK_HEADER = 5;
    public static final int WRITE_FOOTER = 6;
    public static final int STREAM_OBJECT_COMPACTION_READ = 7;
    public static final int STREAM_OBJECT_COMPACTION_WRITE = 8;
    public static final int STREAM_SET_OBJECT_COMPACTION_READ = 9;
    public static final int STREAM_SET_OBJECT_COMPACTION_WRITE = 10;
    public static final int BLOCK_CACHE = 11;
    public static final int S3_WAL = 12;
    public static final int POOLED_MEMORY_RECORDS = 13;
    public static final int SNAPSHOT_READ_CACHE = 14;

    // the MAX_TYPE_NUMBER may change when new type added.
    public static final int MAX_TYPE_NUMBER = 20;

    private static final LongAdder[] USAGE_STATS = new LongAdder[MAX_TYPE_NUMBER];
    private static final LongAdder UNKNOWN_USAGE_STATS = new LongAdder();

    private static long lastMetricLogTime = System.currentTimeMillis();
    private static final Map<Integer, String> ALLOC_TYPE = new HashMap<>();

    public static ByteBufAllocMetric byteBufAllocMetric = null;

    /**
     * The policy used to allocate memory.
     */
    private static ByteBufAllocPolicy policy = ByteBufAllocPolicy.POOLED_HEAP;
    /**
     * The allocator used to allocate memory. It should be updated when {@link #policy} is updated.
     */
    private static AbstractByteBufAllocator allocator = getAllocatorByPolicy(policy);
    /**
     * The metric of the allocator. It should be updated when {@link #policy} is updated.
     */
    private static ByteBufAllocatorMetric metric = getMetricByAllocator(allocator);

    static {
        for (int i = 0; i < MAX_TYPE_NUMBER; i++) {
            USAGE_STATS[i] = new LongAdder();
        }

        registerAllocType(DEFAULT, "default");
        registerAllocType(ENCODE_RECORD, "write_record");
        registerAllocType(DECODE_RECORD, "read_record");
        registerAllocType(WRITE_INDEX_BLOCK, "write_index_block");
        registerAllocType(READ_INDEX_BLOCK, "read_index_block");
        registerAllocType(WRITE_DATA_BLOCK_HEADER, "write_data_block_header");
        registerAllocType(WRITE_FOOTER, "write_footer");
        registerAllocType(STREAM_OBJECT_COMPACTION_READ, "stream_object_compaction_read");
        registerAllocType(STREAM_OBJECT_COMPACTION_WRITE, "stream_object_compaction_write");
        registerAllocType(STREAM_SET_OBJECT_COMPACTION_READ, "stream_set_object_compaction_read");
        registerAllocType(STREAM_SET_OBJECT_COMPACTION_WRITE, "stream_set_object_compaction_write");
        registerAllocType(BLOCK_CACHE, "block_cache");
        registerAllocType(S3_WAL, "s3_wal");
        registerAllocType(POOLED_MEMORY_RECORDS, "pooled_memory_records");
        registerAllocType(SNAPSHOT_READ_CACHE, "snapshot_read_cache");

    }

    /**
     * Set the policy used to allocate memory.
     */
    public static void setPolicy(ByteBufAllocPolicy policy) {
        LOGGER.info("Set alloc policy to {}", policy);
        ByteBufAlloc.policy = policy;
        ByteBufAlloc.allocator = getAllocatorByPolicy(policy);
        ByteBufAlloc.metric = getMetricByAllocator(allocator);
    }

    public static ByteBufAllocPolicy getPolicy() {
        return policy;
    }

    /**
     * Get the chunk size of the allocator.
     * It returns {@link Optional#empty} if the {@link #policy} is not {@link ByteBufAllocPolicy#isPooled}.
     */
    public static Optional<Integer> getChunkSize() {
        if (policy.isPooled()) {
            return Optional.of(((PooledByteBufAllocatorMetric) metric).chunkSize());
        }
        return Optional.empty();
    }

    public static CompositeByteBuf compositeByteBuffer() {
        return allocator.compositeDirectBuffer(Integer.MAX_VALUE);
    }

    public static ByteBuf byteBuffer(int initCapacity) {
        return byteBuffer(initCapacity, DEFAULT);
    }

    public static ByteBuf byteBuffer(int initCapacity, int type) {
        try {
            if (MEMORY_USAGE_DETECT) {
                LongAdder counter;

                if (type > MAX_TYPE_NUMBER) {
                    counter = UNKNOWN_USAGE_STATS;
                } else {
                    counter = USAGE_STATS[type];
                }

                counter.add(initCapacity);

                long now = System.currentTimeMillis();
                if (now - lastMetricLogTime > MEMORY_USAGE_DETECT_INTERVAL) {
                    // it's ok to be not thread safe
                    lastMetricLogTime = now;
                    ByteBufAlloc.byteBufAllocMetric = new ByteBufAllocMetric();
                    LOGGER.info("Buffer usage: {}", ByteBufAlloc.byteBufAllocMetric);
                }
                return new WrappedByteBuf(policy.isDirect() ? allocator.directBuffer(initCapacity) : allocator.heapBuffer(initCapacity), () -> counter.add(-initCapacity));
            } else {
                return policy.isDirect() ? allocator.directBuffer(initCapacity) : allocator.heapBuffer(initCapacity);
            }
        } catch (OutOfMemoryError e) {
            if (MEMORY_USAGE_DETECT) {
                ByteBufAlloc.byteBufAllocMetric = new ByteBufAllocMetric();
                LOGGER.error("alloc buffer OOM, {}", ByteBufAlloc.byteBufAllocMetric, e);
            } else {
                LOGGER.error("alloc buffer OOM", e);
            }
            System.err.println("alloc buffer OOM");
            Runtime.getRuntime().halt(1);
            throw e;
        }
    }

    private static void registerAllocType(int type, String name) {
        if (ALLOC_TYPE.containsKey(type)) {
            throw new IllegalArgumentException("type already registered: " + type + "=" + ALLOC_TYPE.get(type));
        }

        if (type > MAX_TYPE_NUMBER) {
            throw new IllegalArgumentException("type: " + type + ", name: " + name + "." +
                "exceed MAX_TYPE_NUMBER please change the relate code");
        }

        ALLOC_TYPE.put(type, name);
    }

    private static AbstractByteBufAllocator getAllocatorByPolicy(ByteBufAllocPolicy policy) {
        if (policy.isPooled()) {
            return PooledByteBufAllocator.DEFAULT;
        }
        return UnpooledByteBufAllocator.DEFAULT;
    }

    private static ByteBufAllocatorMetric getMetricByAllocator(AbstractByteBufAllocator allocator) {
        if (allocator instanceof ByteBufAllocatorMetricProvider) {
            return ((ByteBufAllocatorMetricProvider) allocator).metric();
        }
        return null;
    }

    public static class ByteBufAllocMetric {
        private final long usedMemory;
        private final long allocatedMemory;
        private final Map<String, Long> detail = new HashMap<>();

        public ByteBufAllocMetric() {
            ALLOC_TYPE.forEach((type, name) -> {
                detail.put(type + "/" + name, USAGE_STATS[type].longValue());
            });

            detail.put("-1/unknown", UNKNOWN_USAGE_STATS.longValue());
            this.usedMemory = policy.isDirect() ? metric.usedDirectMemory() : metric.usedHeapMemory();
            this.allocatedMemory = this.detail.values().stream().mapToLong(Long::longValue).sum();
        }

        public long getUsedMemory() {
            return usedMemory;
        }

        public Map<String, Long> getDetailedMap() {
            return detail;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ByteBufAllocMetric{allocatorMetric=");
            sb.append(metric);
            sb.append(", allocatedMemory=");
            sb.append(allocatedMemory);
            sb.append(", detail=");
            for (Map.Entry<String, Long> entry : detail.entrySet()) {
                sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
            }
            sb.append("pooled=");
            sb.append(policy.isPooled());
            sb.append(", direct=");
            sb.append(policy.isDirect());
            sb.append("}");
            return sb.toString();
        }
    }

    public interface OOMHandler {
        /**
         * Try handle OOM exception.
         *
         * @param memoryRequired the memory required
         * @return freed memory.
         */
        int handle(int memoryRequired);
    }
}
