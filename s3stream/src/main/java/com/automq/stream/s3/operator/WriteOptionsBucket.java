/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed by
 * the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

/**
 * Internal extension point for ObjectStorage/Writer implementations outside this package.
 * Application code should not bind write options to a bucket. Writers set it after selecting the concrete target bucket.
 */
public final class WriteOptionsBucket {
    private WriteOptionsBucket() {
    }

    public static ObjectStorage.WriteOptions bucketId(ObjectStorage.WriteOptions options, short bucketId) {
        return options.bucketId(bucketId);
    }
}
