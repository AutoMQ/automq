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

import com.automq.stream.s3.network.ThrottleStrategy;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.model.CompletedPart;

public interface S3Operator {

    void close();

    /**
     * Range read from object.
     *
     * @param path             object path.
     * @param start            range start.
     * @param end              range end.
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
     * @param path             object path. The path should not start with '/' since Aliyun OSS does not support it.
     * @param data             data.
     * @param throttleStrategy throttle strategy.
     */
    CompletableFuture<Void> write(String path, ByteBuf data, ThrottleStrategy throttleStrategy);

    default CompletableFuture<Void> write(String path, ByteBuf data) {
        return write(path, data, ThrottleStrategy.BYPASS);
    }

    /**
     * New multipart object writer.
     *
     * @param path             object path
     * @param throttleStrategy throttle strategy.
     * @return {@link Writer}
     */
    Writer writer(Writer.Context ctx, String path, ThrottleStrategy throttleStrategy);

    default Writer writer(String path) {
        return writer(Writer.Context.DEFAULT, path, ThrottleStrategy.BYPASS);
    }

    CompletableFuture<List<Pair<String, Long>>> list(String prefix);

    CompletableFuture<Void> delete(String path);

    /**
     * Delete a list of objects.
     *
     * @param objectKeys object keys to delete.
     * @return deleted object keys.
     */
    CompletableFuture<List<String>> delete(List<String> objectKeys);

    // low level API

    /**
     * Create mutlipart upload
     *
     * @param path object path.
     * @return upload id.
     */
    CompletableFuture<String> createMultipartUpload(String path);

    /**
     * Upload part.
     *
     * @return {@link CompletedPart}
     */
    CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data,
        ThrottleStrategy throttleStrategy);

    default CompletableFuture<CompletedPart> uploadPart(String path, String uploadId, int partNumber, ByteBuf data) {
        return uploadPart(path, uploadId, partNumber, data, ThrottleStrategy.BYPASS);
    }

    /**
     * Upload part copy
     *
     * @return {@link CompletedPart}
     */
    CompletableFuture<CompletedPart> uploadPartCopy(String sourcePath, String path, long start, long end,
        String uploadId, int partNumber);

    CompletableFuture<Void> completeMultipartUpload(String path, String uploadId, List<CompletedPart> parts);
}
