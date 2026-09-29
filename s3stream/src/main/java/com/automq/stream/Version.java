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

package com.automq.stream;

public enum Version {
    V0((short) 1),
    // Support StreamObjectCompactV1 (based on composite object)
    // Support wal registration
    V1((short) 2),
    // Support Infinite Storage Stream Archive lifecycle
    V2((short) 3);

    public static final Version LATEST = V2;
    private final short level;

    Version(short level) {
        this.level = level;
    }

    public static Version from(short level) {
        switch (level) {
            case 1:
                return V0;
            case 2:
                return V1;
            case 3:
                return V2;
            default:
                throw new IllegalArgumentException("Unknown Version level: " + level);
        }
    }

    public short featureLevel() {
        return level;
    }

    public boolean isStreamObjectCompactV1Supported() {
        return isAtLeast(V1);
    }

    public boolean isWalRegistrationSupported() {
        return isAtLeast(V1);
    }

    /**
     * Returns whether finalized Stream Archive layout and lifecycle behavior is enabled.
     */
    public boolean isStreamArchiveSupported() {
        return isAtLeast(V2);
    }

    public boolean isAtLeast(Version otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }
}
