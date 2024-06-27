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

package com.automq.stream.s3.wal.impl;

import com.automq.stream.s3.wal.AcknowledgmentService;
import java.util.concurrent.CompletableFuture;

public class NopAcknowledgmentService implements AcknowledgmentService {
    @Override
    public CompletableFuture<Boolean> acknowledgeFailover(long targetNodeId, long currentNodeId, long epoch) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> acknowledgeCommit(long nodeId, long epoch, long firstOffset, long length) {
        return CompletableFuture.completedFuture(true);
    }
}
