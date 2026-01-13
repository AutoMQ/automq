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

package org.apache.kafka.server.common.automq;

import com.automq.stream.Version;

public enum AutoMQVersion {

    V0((short) 1),
    // Support reassignment v1: elect leader after partition open in the new broker
    // Support stream tags
    V1((short) 2),
    // Support composite object
    // Support object bucket index
    // Support huge cluster
    // Support node registration
    V2((short) 3),
    // Support zero zone v2
    V3((short) 4),
    // Support proxy-main dual mapping
    V4((short) 5);

    public static final String FEATURE_NAME = "automq.version";
    public static final AutoMQVersion LATEST = V4;

    private final short level;
    private final Version s3streamVersion;

    AutoMQVersion(short level) {
        this.level = level;
        s3streamVersion = mapS3StreamVersion(level);
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

    public boolean isTopicCleanupByControllerSupported() {
        return isAtLeast(V1);
    }

    public boolean isCompositeObjectSupported() {
        return isAtLeast(V2);
    }

    public boolean isObjectBucketsSupported() {
        return isAtLeast(V2);
    }

    public boolean isObjectAttributesSupported() {
        return isAtLeast(V2);
    }

    public boolean isHugeClusterSupported() {
        return isAtLeast(V2);
    }

    public boolean isNodeRegistrationSupported() {
        return isAtLeast(V2);
    }

    public boolean isZeroZoneV2Supported() {
        return isAtLeast(V3);
    }

    public boolean isDualMappingSupported() {
        return isAtLeast(V4);
    }

    public short streamRecordVersion() {
        if (isReassignmentV1Supported()) {
            return 1;
        } else {
            return 0;
        }
    }

    public short objectRecordVersion() {
        if (isObjectAttributesSupported()) {
            return 1;
        } else {
            return 0;
        }
    }

    public short streamSetObjectRecordVersion() {
        if (isAtLeast(V2)) {
            return 1;
        } else {
            return 0;
        }
    }

    public short streamObjectRecordVersion() {
        if (isAtLeast(V2)) {
            return 1;
        } else {
            return 0;
        }
    }

    public Version s3streamVersion() {
        return s3streamVersion;
    }

    public boolean isAtLeast(AutoMQVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    private Version mapS3StreamVersion(short automqVersion) {
        return switch (automqVersion) {
            case 1, 2 -> Version.V0;
            case 3, 4, 5 -> Version.V1;
            default -> throw new IllegalArgumentException("Unknown AutoMQVersion level: " + automqVersion);
        };
    }


}
