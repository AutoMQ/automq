/*
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

package org.apache.kafka.controller;

import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A holder class of the local node's supported feature flags as well as the quorum node IDs.
 */
public final class QuorumFeatures {
    public static final VersionRange DISABLED = VersionRange.of(0, 0);

    private final int nodeId;
    private final Map<String, VersionRange> localSupportedFeatures;
    private final List<Integer> quorumNodeIds;

    static public Optional<String> reasonNotSupported(
        short newVersion,
        String what,
        VersionRange range
    ) {
        if (!range.contains(newVersion)) {
            if (range.max() == (short) 0) {
                return Optional.of(what + " does not support this feature.");
            } else {
                return Optional.of(what + " only supports versions " + range);
            }
        }
        return Optional.empty();
    }

    public static Map<String, VersionRange> defaultFeatureMap() {
        Map<String, VersionRange> features = new HashMap<>(1);
        features.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
                MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
                MetadataVersion.latest().featureLevel()));
        return features;
    }

    public QuorumFeatures(
        int nodeId,
        Map<String, VersionRange> localSupportedFeatures,
        List<Integer> quorumNodeIds
    ) {
        this.nodeId = nodeId;
        this.localSupportedFeatures = Collections.unmodifiableMap(localSupportedFeatures);
        this.quorumNodeIds = Collections.unmodifiableList(quorumNodeIds);
    }

    public int nodeId() {
        return nodeId;
    }

    public Map<String, VersionRange> localSupportedFeatures() {
        return localSupportedFeatures;
    }

    public List<Integer> quorumNodeIds() {
        return quorumNodeIds;
    }

    public VersionRange localSupportedFeature(String name) {
        return localSupportedFeatures.getOrDefault(name, DISABLED);
    }

    public boolean isControllerId(int nodeId) {
        return quorumNodeIds.contains(nodeId);
    }

    public Optional<String> reasonNotLocallySupported(
        String featureName,
        short newVersion
    ) {
        return reasonNotSupported(newVersion,
            "Local controller " + nodeId,
            localSupportedFeature(featureName));
    }

    public Optional<String> reasonAllControllersZkMigrationNotReady(
        MetadataVersion metadataVersion,
        Map<Integer, ControllerRegistration> controllers
    ) {
        if (!metadataVersion.isMigrationSupported()) {
            return Optional.of("Metadata version too low at " + metadataVersion);
        } else if (!metadataVersion.isControllerRegistrationSupported()) {
            return Optional.empty();
        }
        for (int quorumNodeId : quorumNodeIds) {
            ControllerRegistration registration = controllers.get(quorumNodeId);
            if (registration == null) {
                return Optional.of("No registration found for controller " + quorumNodeId);
            } else if (!registration.zkMigrationReady()) {
                return Optional.of("Controller " + quorumNodeId + " has not enabled " +
                        "zookeeper.metadata.migration.enable");
            }
        }
        return Optional.empty();
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, localSupportedFeatures, quorumNodeIds);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(QuorumFeatures.class))) return false;
        QuorumFeatures other = (QuorumFeatures) o;
        return nodeId == other.nodeId &&
            localSupportedFeatures.equals(other.localSupportedFeatures) &&
            quorumNodeIds.equals(other.quorumNodeIds);
    }

    @Override
    public String toString() {
        List<String> features = new ArrayList<>();
        localSupportedFeatures.entrySet().forEach(f -> features.add(f.getKey() + ": " + f.getValue()));
        features.sort(String::compareTo);
        List<String> nodeIds = new ArrayList<>();
        quorumNodeIds.forEach(id -> nodeIds.add("" + id));
        nodeIds.sort(String::compareTo);
        return "QuorumFeatures" +
            "(nodeId=" + nodeId +
            ", localSupportedFeatures={" + features + "}" +
            ", quorumNodeIds=[" + nodeIds + "]" +
            ")";
    }
}
