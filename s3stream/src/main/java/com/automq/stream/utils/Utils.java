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

package com.automq.stream.utils;

public class Utils {
    public static final String MAX_MERGE_READ_SPARSITY_RATE_NAME = "MERGE_READ_SPARSITY_RATE";

    public static float getMaxMergeReadSparsityRate() {
        float rate;
        try {
            rate = Float.parseFloat(System.getenv(MAX_MERGE_READ_SPARSITY_RATE_NAME));
        } catch (Exception e) {
            rate = 0.5f;
        }
        return rate;
    }
}
