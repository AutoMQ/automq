/*
 * Copyright 2026, AutoMQ HK Limited.
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

package org.apache.kafka.common.requests.s3;

import java.util.HashMap;
import java.util.Map;

/**
 * Discriminator for one typed Stream Archive operation payload.
 */
public enum StreamArchiveOperationType {
    ARCHIVE_PREPARE((byte) 0),
    ARCHIVE_PUBLISH((byte) 1),
    CLEANUP_PREPARE((byte) 2),
    CLEANUP_COMMIT((byte) 3),
    ADVANCE_EMPTY_CURSOR((byte) 4);

    private static final Map<Byte, StreamArchiveOperationType> TYPES = new HashMap<>();

    static {
        for (StreamArchiveOperationType type : values()) {
            TYPES.put(type.value, type);
        }
    }

    private final byte value;

    StreamArchiveOperationType(byte value) {
        this.value = value;
    }

    /**
     * Return the protocol discriminator.
     */
    public byte value() {
        return value;
    }

    /**
     * Resolve a protocol discriminator, or {@code null} when it is unknown.
     */
    public static StreamArchiveOperationType fromValue(byte value) {
        return TYPES.get(value);
    }
}
