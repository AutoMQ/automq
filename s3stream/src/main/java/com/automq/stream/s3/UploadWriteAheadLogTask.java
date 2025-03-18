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

package com.automq.stream.s3;

import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;

import java.util.concurrent.CompletableFuture;

public interface UploadWriteAheadLogTask {
    /**
     * Prepare the upload task and get the uploadId(objectId).
     * @return the uploadId(objectId)
     */
    CompletableFuture<Long> prepare();

    /**
     * Upload the delta wal data to the main storage
     */
    CompletableFuture<CommitStreamSetObjectRequest> upload();

    /**
     * Commit the upload result to controller.
     */
    CompletableFuture<Void> commit();

    /**
     * Burst the upload task.
     */
    void burst();

}
