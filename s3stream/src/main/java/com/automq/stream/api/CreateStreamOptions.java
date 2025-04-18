/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
