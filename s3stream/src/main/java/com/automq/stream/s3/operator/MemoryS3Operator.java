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
package com.automq.stream.s3.operator;

import com.automq.stream.s3.compact.AsyncTokenBucketThrottle;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryS3Operator implements S3Operator {
    private final Map<String, ByteBuf> storage = new ConcurrentHashMap<>();

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ByteBufAllocator alloc) {
        ByteBuf value = storage.get(path);
        if (value == null) {
            return FutureUtil.failedFuture(new IllegalArgumentException("object not exist"));
        }
        int length = (int) (end - start);
        return CompletableFuture.completedFuture(value.retainedSlice(value.readerIndex() + (int) start, length));
    }

    @Override
    public CompletableFuture<Void> write(String path, ByteBuf data) {
        ByteBuf buf = Unpooled.buffer(data.readableBytes());
        buf.writeBytes(data.duplicate());
        storage.put(path, buf);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Writer writer(String path, String logIdent, AsyncTokenBucketThrottle readThrottle) {
        ByteBuf buf = Unpooled.buffer();
        storage.put(path, buf);
        return new Writer() {
            @Override
            public CompletableFuture<Void> write(ByteBuf part) {
                buf.writeBytes(part);
                // Keep the same behavior as a real S3Operator
                // Release the part after write
                part.release();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean hashBatchingPart() {
                return false;
            }

            @Override
            public void copyWrite(String sourcePath, long start, long end) {
                ByteBuf source = storage.get(sourcePath);
                if (source == null) {
                    throw new IllegalArgumentException("object not exist");
                }
                buf.writeBytes(source.slice(source.readerIndex() + (int) start, (int) (end - start)));
            }

            @Override
            public CompletableFuture<Void> close() {
                return CompletableFuture.completedFuture(null);
            }
        };
    }

    @Override
    public CompletableFuture<Void> delete(String path) {
        storage.remove(path);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<String>> delete(List<String> objectKeys) {
        objectKeys.forEach(storage::remove);
        return CompletableFuture.completedFuture(null);
    }
}
