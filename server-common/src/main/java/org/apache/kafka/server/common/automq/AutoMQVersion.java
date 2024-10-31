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

package org.apache.kafka.server.common.automq;

import com.automq.stream.Version;
import java.util.Map;
import org.apache.kafka.server.common.FeatureVersion;
import org.apache.kafka.server.common.MetadataVersion;

public enum AutoMQVersion implements FeatureVersion {

    V0((short) 1, MetadataVersion.IBP_3_4_IV0),
    // Support reassignment v1: elect leader after partition open in the new broker
    // Support stream tags
    V1((short) 2, MetadataVersion.IBP_3_7_IV0),
    // Support composite object
    // Support object bucket index
    // Support huge cluster
    // Support node registration
    V2((short) 3, MetadataVersion.IBP_3_8_IV0);

    public static final String FEATURE_NAME = "automq.version";
    public static final AutoMQVersion LATEST = V2;

    private final short level;
    private final Version s3streamVersion;
    private final MetadataVersion metadataVersion;

    AutoMQVersion(short level, MetadataVersion metadataVersion) {
        this.level = level;
        s3streamVersion = mapS3StreamVersion(level);
        this.metadataVersion = metadataVersion;
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

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return metadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return Map.of();
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
        switch (automqVersion) {
            case 1:
            case 2:
                return Version.V0;
            case 3:
                return Version.V1;
            default:
                throw new IllegalArgumentException("Unknown AutoMQVersion level: " + automqVersion);
        }
    }


}
