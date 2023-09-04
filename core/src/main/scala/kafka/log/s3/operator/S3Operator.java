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
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.concurrent.CompletableFuture;

public interface S3Operator {

    void close();

    /**
     * Range read from object.
     *
     * @param path  object path.
     * @param start range start.
     * @param end   range end.
     * @return data.
     */
    CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ByteBufAllocator alloc);

    default CompletableFuture<ByteBuf> rangeRead(String path, long start, long end) {
        return rangeRead(path, start, end, UnpooledByteBufAllocator.DEFAULT);
    }

    /**
     * Write data to object.
     *
     * @param path object path.
     * @param data data.
     */
    CompletableFuture<Void> write(String path, ByteBuf data);

    /**
     * New multi-part object writer.
     *
     * @param path object path
     * @return {@link Writer}
     */
    Writer writer(String path);

    CompletableFuture<Void> delete(String path);
}
