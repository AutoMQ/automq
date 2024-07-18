/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.api;

import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import java.util.concurrent.CompletableFuture;

/**
 * Elastic Stream client.
 */
public interface Client {
    void start();

    void shutdown();

    /**
     * Get stream client.
     *
     * @return {@link StreamClient}
     */
    StreamClient streamClient();

    /**
     * Get KV client.
     *
     * @return {@link KVClient}
     */
    KVClient kvClient();

    /**
     * Failover the another node volume
     */
    CompletableFuture<FailoverResponse> failover(FailoverRequest request);
}
