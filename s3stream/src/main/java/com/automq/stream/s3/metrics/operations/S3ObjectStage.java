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

package com.automq.stream.s3.metrics.operations;

/**
 * TODO: Maybe merge into {@link S3Stage}
 */
public enum S3ObjectStage {
    UPLOAD_PART("upload_part"),
    READY_CLOSE("ready_close"),
    TOTAL("total");

    private final String name;

    S3ObjectStage(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getUniqueKey() {
        return "s3_object_stage_" + name;
    }
}
