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
package kafka.log.s3.operator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import kafka.log.es.FutureUtil;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MemoryS3Operator implements S3Operator {
    private final Map<String, ByteBuf> storage = new HashMap<>();

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
        return CompletableFuture.completedFuture(value.slice(value.readerIndex() + (int) start, length));
    }

    @Override
    public CompletableFuture<Void> write(String path, ByteBuf data) {
        storage.put(path, data.duplicate());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Writer writer(String path) {
        ByteBuf buf = Unpooled.buffer();
        storage.put(path, buf);
        return new Writer() {
            @Override
            public CompletableFuture<CompletedPart> write(ByteBuf part) {
                buf.writeBytes(part);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void copyWrite(String sourcePath, long start, long end) {
                ByteBuf source = storage.get(sourcePath);
                if (source == null) {
                    throw new IllegalArgumentException("object not exist");
                }
                buf.writeBytes(source.slice(source.readerIndex() + (int) start, source.readerIndex() + (int) end));
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
}
