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

package com.automq.shell.log;

import org.apache.commons.lang3.StringUtils;

public interface LogRecorder {
    boolean append(LogEvent event);

    class LogEvent {
        private final long timestampMillis;
        private final String level;
        private final String logger;
        private final String message;
        private final String[] stackTrace;

        public LogEvent(long timestampMillis, String level, String logger, String message, String[] stackTrace) {
            this.timestampMillis = timestampMillis;
            this.level = level;
            this.logger = logger;
            this.message = message;
            this.stackTrace = stackTrace;
        }

        public void validate() {
            if (timestampMillis <= 0) {
                throw new IllegalArgumentException("Timestamp must be greater than 0");
            }
            if (StringUtils.isBlank(level)) {
                throw new IllegalArgumentException("Level cannot be blank");
            }
            if (StringUtils.isBlank(logger)) {
                throw new IllegalArgumentException("Level cannot be blank");
            }
            if (StringUtils.isBlank(message)) {
                throw new IllegalArgumentException("Level cannot be blank");
            }
        }

        public long timestampMillis() {
            return timestampMillis;
        }

        public String level() {
            return level;
        }

        public String logger() {
            return logger;
        }

        public String message() {
            return message;
        }

        public String[] stackTrace() {
            return stackTrace;
        }
    }
}
