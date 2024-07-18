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

import java.util.Map;

public enum S3MetricsType {
    S3Stream("S3Stream"),
    S3Storage("S3Storage"),
    S3Request("S3Request"),
    S3Object("S3Object"),
    S3Network("S3Network");

    private static final Map<String, S3MetricsType> MAP = Map.of(
        "S3Stream", S3Stream,
        "S3Storage", S3Storage,
        "S3Request", S3Request,
        "S3Object", S3Object
    );

    private final String name;

    S3MetricsType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public S3MetricsType of(String name) {
        return MAP.get(name);
    }

    @Override
    public String toString() {
        return "S3MetricsType{" +
            "name='" + name + '\'' +
            '}';
    }
}
