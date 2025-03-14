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

package kafka.log.streamaspect;

import com.automq.stream.api.AppendResult;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatch;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;

import java.util.concurrent.CompletableFuture;

/**
 * Elastic stream slice is a slice from elastic stream, <strong> the startOffset of a slice is 0. </strong>
 * In the same time, there is only one writable slice in a stream, and the writable slice is always the last slice.
 */
public interface ElasticStreamSlice {

    /**
     * Append record batch to stream slice.
     *
     * @param recordBatch {@link RecordBatch}
     * @return {@link AppendResult}
     */
    CompletableFuture<AppendResult> append(AppendContext context, RecordBatch recordBatch);

    default CompletableFuture<AppendResult> append(RecordBatch recordBatch) {
        return append(AppendContext.DEFAULT, recordBatch);
    }

    /**
     * Fetch record batch from stream slice.
     *
     * @param startOffset  start offset.
     * @param endOffset    end offset.
     * @param maxBytesHint max fetch data size hint, the real return data size may be larger than maxBytesHint.
     * @return {@link FetchResult}
     */
    CompletableFuture<FetchResult> fetch(FetchContext context, long startOffset, long endOffset, int maxBytesHint);

    default CompletableFuture<FetchResult> fetch(long startOffset, long endOffset, int maxBytesHint) {
        return fetch(FetchContext.DEFAULT, startOffset, endOffset, maxBytesHint);
    }

    default CompletableFuture<FetchResult> fetch(long startOffset, long endOffset) {
        return fetch(startOffset, endOffset, (int) (endOffset - startOffset));
    }

    /**
     * Get stream slice next offset.
     */
    long nextOffset();

    /**
     * Get stream slice confirm offset. All data before this offset is confirmed.
     */
    long confirmOffset();


    /**
     * Get slice range which is the relative offset range in stream.
     *
     * @return {@link SliceRange}
     */
    SliceRange sliceRange();

    /**
     * Seal slice, forbid future append.
     */
    void seal();

    Stream stream();
}
