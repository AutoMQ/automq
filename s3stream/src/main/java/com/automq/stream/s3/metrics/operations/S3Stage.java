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

package com.automq.stream.s3.metrics.operations;

public enum S3Stage {

    /* Append WAL stages start */
    APPEND_WAL_BEFORE(S3Operation.APPEND_STORAGE_WAL, "before"),
    APPEND_WAL_BLOCK_POLLED(S3Operation.APPEND_STORAGE_WAL, "block_polled"),
    APPEND_WAL_AWAIT(S3Operation.APPEND_STORAGE_WAL, "await"),
    APPEND_WAL_WRITE(S3Operation.APPEND_STORAGE_WAL, "write"),
    APPEND_WAL_AFTER(S3Operation.APPEND_STORAGE_WAL, "after"),
    APPEND_WAL_COMPLETE(S3Operation.APPEND_STORAGE_WAL, "complete"),
    /* Append WAL stages end */

    /* Force upload WAL start */
    FORCE_UPLOAD_WAL_AWAIT(S3Operation.FORCE_UPLOAD_STORAGE_WAL, "await"),
    FORCE_UPLOAD_WAL_COMPLETE(S3Operation.FORCE_UPLOAD_STORAGE_WAL, "complete"),
    /* Force upload WAL end */

    /* Upload WAL start */
    UPLOAD_WAL_PREPARE(S3Operation.UPLOAD_STORAGE_WAL, "prepare"),
    UPLOAD_WAL_UPLOAD(S3Operation.UPLOAD_STORAGE_WAL, "upload"),
    UPLOAD_WAL_COMMIT(S3Operation.UPLOAD_STORAGE_WAL, "commit"),
    UPLOAD_WAL_COMPLETE(S3Operation.UPLOAD_STORAGE_WAL, "complete");
    /* Upload WAL end */

    private final S3Operation operation;
    private final String name;

    S3Stage(S3Operation operation, String name) {
        this.operation = operation;
        this.name = name;
    }

    public S3Operation getOperation() {
        return operation;
    }

    public String getName() {
        return name;
    }

    public String getUniqueKey() {
        return operation.getUniqueKey() + "-" + name;
    }

    @Override
    public String toString() {
        return "S3Stage{" +
            "operation=" + operation.getName() +
            ", name='" + name + '\'' +
            '}';
    }
}
