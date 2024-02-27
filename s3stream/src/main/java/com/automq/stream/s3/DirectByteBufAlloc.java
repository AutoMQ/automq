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

package com.automq.stream.s3;

import com.automq.stream.WrappedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectByteBufAlloc {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufAlloc.class);
    private static final PooledByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;
    private static final Map<Integer, LongAdder> USAGE_STATS = new ConcurrentHashMap<>();
    private static long lastMetricLogTime = System.currentTimeMillis();
    private static final Map<Integer, String> ALLOC_TYPE = new HashMap<>();

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

    static {
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

    }

    public static CompositeByteBuf compositeByteBuffer() {
        return ALLOC.compositeDirectBuffer(Integer.MAX_VALUE);
    }

    public static ByteBuf byteBuffer(int initCapacity) {
        return byteBuffer(initCapacity, DEFAULT);
    }

    public static ByteBuf byteBuffer(int initCapacity, int type) {
        try {
            LongAdder usage = USAGE_STATS.compute(type, (k, v) -> {
                if (v == null) {
                    v = new LongAdder();
                }
                v.add(initCapacity);
                return v;
            });
            long now = System.currentTimeMillis();
            if (now - lastMetricLogTime > 60000) {
                // it's ok to be not thread safe
                lastMetricLogTime = now;
                LOGGER.info("Direct Memory usage: netty.usedDirectMemory={}, DirectByteBufAlloc={}", ALLOC.metric().usedDirectMemory(), metric());
            }
            return new WrappedByteBuf(ALLOC.directBuffer(initCapacity), () -> usage.add(-initCapacity));
        } catch (OutOfMemoryError e) {
            LOGGER.error("alloc direct buffer OOM, netty.usedDirectMemory={}, DirectByteBufAlloc={}", ALLOC.metric().usedDirectMemory(), metric(), e);
            System.err.println("alloc direct buffer OOM");
            Runtime.getRuntime().halt(1);
            throw e;
        }
    }

    public static void registerAllocType(int type, String name) {
        if (ALLOC_TYPE.containsKey(type)) {
            throw new IllegalArgumentException("type already registered: " + type + "=" + ALLOC_TYPE.get(type));
        }
        ALLOC_TYPE.put(type, name);
    }

    public static Metric metric() {
        return new Metric();
    }

    public static class Metric {
        private final long usage;
        private final Map<Integer, Long> detail;

        public Metric() {
            Map<Integer, Long> detail = new HashMap<>();
            USAGE_STATS.forEach((k, v) -> detail.put(k, v.longValue()));
            this.detail = detail;
            this.usage = this.detail.values().stream().mapToLong(Long::longValue).sum();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("DirectByteBufAllocMetric{usage=");
            sb.append(usage);
            sb.append(", detail=");
            for (Map.Entry<Integer, Long> entry : detail.entrySet()) {
                sb.append(entry.getKey()).append("/").append(ALLOC_TYPE.get(entry.getKey())).append("=").append(entry.getValue()).append(",");
            }
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
