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

package com.automq.stream.s3.streams;

import com.automq.stream.s3.metadata.StreamMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface StreamManager {

    /**
     * Get current server opening streams.
     * When server is starting or recovering, WAL in EBS need streams offset to determine the recover point.
     *
     * @return list of {@link StreamMetadata}
     */
    CompletableFuture<List<StreamMetadata>> getOpeningStreams();

    /**
     * Get streams metadata by stream id.
     *
     * @param streamIds stream ids.
     * @return list of {@link StreamMetadata}
     */
    CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds);

    /**
     * Add a stream metadata listener.
     * @param streamId the stream id to listen
     * @param listener {@link StreamMetadataListener}
     * @return listener handle to remove listener
     */
    StreamMetadataListener.Handle addMetadataListener(long streamId, StreamMetadataListener listener);

    default CompletableFuture<Long> createStream() {
        return createStream(Collections.emptyMap());
    }

    /**
     * Create stream with tags
     * @param tags key value tags
     * @return stream id.
     */
    CompletableFuture<Long> createStream(Map<String, String> tags);

    /**
     * Open stream with newer epoch. The controller will:
     * 1. update stream epoch to fence old stream writer to commit object.
     * 2. calculate the last range endOffset.
     * 2. create a new range with serverId = current serverId, startOffset = last range endOffset.
     *
     * @param streamId stream id.
     * @param epoch    stream epoch.
     * @return {@link StreamMetadata}
     */
    default CompletableFuture<StreamMetadata> openStream(long streamId, long epoch) {
        return openStream(streamId, epoch, Collections.emptyMap());
    }

    CompletableFuture<StreamMetadata> openStream(long streamId, long epoch, Map<String, String> tags);

    /**
     * Trim stream to new start offset.
     *
     * @param streamId       stream id.
     * @param epoch          stream epoch.
     * @param newStartOffset new start offset.
     */
    CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset);

    /**
     * Close stream. Other server can open stream with newer epoch.
     *
     * @param streamId stream id.
     * @param epoch    stream epoch.
     */
    CompletableFuture<Void> closeStream(long streamId, long epoch);

    /**
     * Delete stream.
     *
     * @param streamId stream id.
     * @param epoch    stream epoch.
     */
    CompletableFuture<Void> deleteStream(long streamId, long epoch);

    void setStreamCloseHook(StreamCloseHook hook);
}
