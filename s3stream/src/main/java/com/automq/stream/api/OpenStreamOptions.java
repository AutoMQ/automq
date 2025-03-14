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
