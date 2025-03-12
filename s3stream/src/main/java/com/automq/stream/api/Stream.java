/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.api;

import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;

import java.util.concurrent.CompletableFuture;

/**
 * Record stream.
 */
public interface Stream {

    /**
     * Get stream id
     */
    long streamId();

    /**
     * Get stream epoch.
     */
    long streamEpoch();

    /**
     * Get stream start offset.
     */
    long startOffset();

    /**
     * Get stream confirm record offset.
     */
    long confirmOffset();

    /**
     * Get stream next append record offset.
     */
    long nextOffset();

    /**
     * Append recordBatch to stream.
     *
     * @param recordBatch {@link RecordBatch}.
     * @return - complete success with async {@link AppendResult}, when append success.
     * - complete exception with {@link StreamClientException}, when append fail. TODO: specify the exception.
     */
    CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch);

    default CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        return append(AppendContext.DEFAULT, recordBatch);
    }

    /**
     * Fetch recordBatch list from stream. Note the startOffset may be in the middle in the first recordBatch.
     * It is strongly recommended to handle the completion of the returned CompletableFuture in a separate thread.
     *
     * @param context     fetch context, {@link FetchContext}.
     * @param startOffset  start offset, if the startOffset in middle of a recordBatch, the recordBatch will be returned.
     * @param endOffset    exclusive end offset, if the endOffset in middle of a recordBatch, the recordBatch will be returned.
     * @param maxBytesHint max fetch data size hint, the real return data size may be larger than maxBytesHint.
     * @return - complete success with {@link FetchResult}, when fetch success.
     * - complete exception with {@link StreamClientException}, when startOffset is bigger than stream end offset.
     */
    CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint);

    default CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint) {
        return fetch(FetchContext.DEFAULT, startOffset, endOffset, maxBytesHint);
    }

    /**
     * Trim stream.
     *
     * @param newStartOffset new start offset.
     * @return - complete success with async {@link Void}, when trim success.
     * - complete exception with {@link StreamClientException}, when trim fail.
     */
    CompletableFuture<Void> trim(long newStartOffset);

    /**
     * Close the stream.
     */
    CompletableFuture<Void> close();

    /**
     * Destroy stream.
     */
    CompletableFuture<Void> destroy();

}
