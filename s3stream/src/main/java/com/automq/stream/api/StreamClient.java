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

package com.automq.stream.api;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Stream client, support stream create and open operation.
 */
public interface StreamClient {
    /**
     * Create and open stream.
     *
     * @param options create stream options.
     * @return {@link Stream}.
     */
    CompletableFuture<Stream> createAndOpenStream(CreateStreamOptions options);

    /**
     * Open stream.
     *
     * @param streamId stream id.
     * @param options  open stream options.
     * @return {@link Stream}.
     */
    CompletableFuture<Stream> openStream(long streamId, OpenStreamOptions options);

    /**
     * Retrieve an opened stream.
     *
     * @param streamId stream id.
     * @return {@link Optional<Stream>}.
     */
    Optional<Stream> getStream(long streamId);

    void shutdown();
}
