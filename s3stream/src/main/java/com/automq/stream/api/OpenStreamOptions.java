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

public class OpenStreamOptions {
    public static final OpenStreamOptions DEFAULT = new OpenStreamOptions();

    private ReadWriteMode readWriteMode = ReadWriteMode.READ_WRITE;
    private long epoch;
    private final Map<String, String> tags = new HashMap<>();

    private OpenStreamOptions() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public ReadWriteMode readWriteMode() {
        return readWriteMode;
    }

    public long epoch() {
        return epoch;
    }

    public Map<String, String> tags() {
        return tags;
    }

    public enum ReadWriteMode {
        READ_WRITE(0), SNAPSHOT_READ(1);

        final int code;

        ReadWriteMode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public static class Builder {
        private final OpenStreamOptions options = new OpenStreamOptions();

        public Builder readWriteMode(ReadWriteMode readWriteMode) {
            Arguments.isNotNull(readWriteMode, "readWriteMode should be set with READ_WRITE or SNAPSHOT_READ");

            options.readWriteMode = readWriteMode;
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

        public Builder tags(Map<String, String> tags) {
            options.tags.putAll(tags);
            return this;
        }

        public OpenStreamOptions build() {
            return options;
        }
    }
}
