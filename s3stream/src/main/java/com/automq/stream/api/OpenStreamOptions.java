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

package com.automq.stream.api;

import com.automq.stream.utils.Arguments;
import java.util.HashMap;
import java.util.Map;

public class OpenStreamOptions {
    private WriteMode writeMode = WriteMode.SINGLE;
    private ReadMode readMode = ReadMode.MULTIPLE;
    private long epoch;
    private final Map<String, String> tags = new HashMap<>();

    private OpenStreamOptions() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public WriteMode writeMode() {
        return writeMode;
    }

    public ReadMode readMode() {
        return readMode;
    }

    public long epoch() {
        return epoch;
    }

    public Map<String, String> tags() {
        return tags;
    }

    public enum WriteMode {
        SINGLE(0), MULTIPLE(1);

        final int code;

        WriteMode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public enum ReadMode {
        SINGLE(0), MULTIPLE(1);

        final int code;

        ReadMode(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public static class Builder {
        private final OpenStreamOptions options = new OpenStreamOptions();

        public Builder writeMode(WriteMode writeMode) {
            Arguments.isNotNull(writeMode, "WriteMode should be set with SINGLE or MULTIPLE");
            options.writeMode = writeMode;
            return this;
        }

        public Builder readMode(ReadMode readMode) {
            Arguments.isNotNull(readMode, "ReadMode should be set with SINGLE or MULTIPLE");
            options.readMode = readMode;
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
