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

package com.automq.stream;

public enum Version {
    V0((short) 1),
    // Support StreamObjectCompactV1 (based on composite object)
    // Support wal registration
    V1((short) 2);

    public static final Version LATEST = V1;
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

    public boolean isAtLeast(Version otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }
}
