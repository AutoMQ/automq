/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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
