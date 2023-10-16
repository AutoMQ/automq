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
import io.netty.buffer.ByteBuf;

import java.util.List;
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
    CompletableFuture<ByteBuf> rangeRead(String path, long start, long end);

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
     * @param path         object path
     * @param logIdent     log identifier
     * @param readThrottle read throttle. null means no throttle.
     *                     It is used to throttle reading in copy-write.
     * @return {@link Writer}
     */
    Writer writer(String path, String logIdent, AsyncTokenBucketThrottle readThrottle);

    default Writer writer(String path, String logIdent) {
        return writer(path, logIdent, null);
    }

    CompletableFuture<Void> delete(String path);

    /**
     * Delete a list of objects.
     * @param objectKeys object keys to delete.
     * @return deleted object keys.
     */
    CompletableFuture<List<String>> delete(List<String> objectKeys);
}
