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

package com.automq.log.uploader;

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
                throw new IllegalArgumentException("Logger cannot be blank");
            }
            if (StringUtils.isBlank(message)) {
                throw new IllegalArgumentException("Message cannot be blank");
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
