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

package org.apache.kafka.server.common.automq;

public enum AutoMQVersion {

    V0((short) 1),
    // Support reassignment v1: elect leader after partition open in the new broker
    V1((short) 2);

    public static final String FEATURE_NAME = "automq.version";
    public static final AutoMQVersion LATEST = V1;

    private final short level;

    AutoMQVersion(short level) {
        this.level = level;
    }

    public static AutoMQVersion from(short level) {
        for (AutoMQVersion version : AutoMQVersion.values()) {
            if (version.level == level) {
                return version;
            }
        }
        if (level == 0) {
            // when the version is not set, we assume it is V0
            return V0;
        }
        throw new IllegalArgumentException("Unknown AutoMQVersion level: " + level);
    }

    public short featureLevel() {
        return level;
    }

    public boolean isReassignmentV1Supported() {
        return isAtLeast(V1);
    }

    public boolean isStreamTagsSupported() {
        return isAtLeast(V1);
    }

    public short streamRecordVersion() {
        if (isReassignmentV1Supported()) {
            return 1;
        } else {
            return 0;
        }
    }

    public boolean isAtLeast(AutoMQVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

}
