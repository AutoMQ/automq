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
