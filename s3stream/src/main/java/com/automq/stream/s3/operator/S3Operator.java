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

import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.services.s3.model.CompletedPart;

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
     * @param throttleStrategy throttle strategy.
     * @return data.
     */
    CompletableFuture<ByteBuf> rangeRead(String path, long start, long end, ThrottleStrategy throttleStrategy);

    default CompletableFuture<ByteBuf> rangeRead(String path, long start, long end) {
        return rangeRead(path, start, end, ThrottleStrategy.BYPASS);
    }

    /**
     * Write data to object.
     *
     * @param path object path. The path should not start with '/' since Aliyun OSS does not support it.
     * @param data data.
     * @param throttleStrategy throttle strategy.
     */
    CompletableFuture<Void> write(String path, ByteBuf data, ThrottleStrategy throttleStrategy);

    default CompletableFuture<Void> write(String path, ByteBuf data) {
        return write(path, data, ThrottleStrategy.BYPASS);
    }

    /**
     * New multipart object writer.
     *
     * @param path         object path
     * @param throttleStrategy throttle strategy.
     * @return {@link Writer}
     */
    Writer writer(String path, ThrottleStrategy throttleStrategy);

    default Writer writer(String path) {
        return writer(path,  ThrottleStrategy.BYPASS);
    }

    CompletableFuture<Void> delete(String path);

    /**
     * Delete a list of objects.
     * @param objectKeys object keys to delete.
     * @return deleted object keys.
     */
    CompletableFuture<List<String>> delete(List<String> objectKeys);

    // low level API

    /**
     * Create mutlipart upload
     * @param path object path.
     * @return upload id.
     */
    CompletableFuture<String> createMultipartUpload(String path);

    /**
     * Upload part.
     * @return {@link CompletedPart}
     */
    CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data, ThrottleStrategy throttleStrategy);

    default CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data) {
        return uploadPart(path, uploadId, partNumber, data, ThrottleStrategy.BYPASS);
    }

    /**
     * Upload part copy
     * @return {@link CompletedPart}
     */
    CompletableFuture<CompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end, String uploadId, int partNumber);

    CompletableFuture<Void> completeMultipartUpload(String path, String uploadId, List<CompletedPart> parts);
}
