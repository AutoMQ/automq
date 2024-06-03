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

import com.automq.stream.utils.Arguments;
import java.util.HashMap;
import java.util.Map;

public class CreateStreamOptions {
    private int replicaCount;
    private long epoch;
    private final Map<String, String> tags = new HashMap<>();

    private CreateStreamOptions() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public int replicaCount() {
        return replicaCount;
    }

    public long epoch() {
        return epoch;
    }

    public Map<String, String> tags() {
        return tags;
    }

    public static class Builder {
        private final CreateStreamOptions options = new CreateStreamOptions();

        public Builder replicaCount(int replicaCount) {
            Arguments.check(replicaCount > 0, "replica count should larger than 0");
            options.replicaCount = replicaCount;
            return this;
        }

        public Builder epoch(long epoch) {
            options.epoch = epoch;
            return this;
        }

        public Builder tag(String key, String value) {
            options.tags.put(key, value);
            return this;
        }

        public CreateStreamOptions build() {
            return options;
        }

    }
}
