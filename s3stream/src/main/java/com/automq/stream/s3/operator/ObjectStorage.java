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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectStorage {

    void close();

    /**
     * Get {@link Writer} for the object.
     */
    Writer writer(WriteOptions options, String objectPath);

    /**
     * Range read object from the object.
     */
    CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end);


    // Low level API
    CompletableFuture<Void> write(WriteOptions options, String objectPath, ByteBuf buf);

    CompletableFuture<List<ObjectInfo>> list(String prefix);

    CompletableFuture<Void> delete(List<ObjectPath> objectPaths);

    class ObjectPath {
        private final short bucket;
        private final String key;

        public ObjectPath(short bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }

        public short bucket() {
            return bucket;
        }

        public String key() {
            return key;
        }
    }

    class ObjectInfo extends ObjectPath {
        private final long timestamp;

        public ObjectInfo(short bucket, String key, long timestamp) {
            super(bucket, key);
            this.timestamp = timestamp;
        }

        public long timestamp() {
            return timestamp;
        }
    }

    class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private int allocType = ByteBufAlloc.DEFAULT;

        public WriteOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public WriteOptions allocType(int allocType) {
            this.allocType = allocType;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }

        public int allocType() {
            return allocType;
        }

    }

    class ReadOptions {
        public static final ReadOptions DEFAULT = new ReadOptions();

        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;
        private short bucket = (short) 0;

        public ReadOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public ReadOptions bucket(short bucket) {
            this.bucket = bucket;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }

        public short bucket() {
            return bucket;
        }
    }
}
