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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.controller.FeatureControlManagerTest.createFakeClusterFeatureSupportDescriber;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protects the cluster capability boundary for enabling sharded WAL writes and admitting readers. */
@Tag("S3Unit")
public class WALShardingFeatureTest {
    /** Finalizing sharding support requires both registered brokers and controllers to support the new level. */
    @Test
    public void testUpgradeRequiresCompatibleNodes() {
        for (AutoMQVersion broker : List.of(AutoMQVersion.V5, AutoMQVersion.V6)) {
            for (AutoMQVersion controller : List.of(AutoMQVersion.V5, AutoMQVersion.V6)) {
                FeatureControlManager manager = manager(broker, controller);
                manager.replay(record(AutoMQVersion.V5));
                ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
                    Map.of(AutoMQVersion.FEATURE_NAME, AutoMQVersion.V6.featureLevel()), Map.of(), false);
                boolean compatible = broker.isWALShardingSupported() && controller.isWALShardingSupported();
                assertEquals(compatible ? Errors.NONE : Errors.INVALID_UPDATE_VERSION,
                    result.response().get(AutoMQVersion.FEATURE_NAME).error());
                assertEquals(compatible ? 1 : 0, result.records().size());
            }
        }
    }

    /** Even an unsafe downgrade cannot remove reader compatibility while sharded objects may remain. */
    @ParameterizedTest
    @EnumSource(value = FeatureUpdate.UpgradeType.class, names = {"SAFE_DOWNGRADE", "UNSAFE_DOWNGRADE"})
    public void testDowngradeRejected(FeatureUpdate.UpgradeType type) {
        FeatureControlManager manager = manager(AutoMQVersion.V6, AutoMQVersion.V6);
        manager.replay(record(AutoMQVersion.V6));
        ControllerResult<Map<String, ApiError>> result = manager.updateFeatures(
            Map.of(AutoMQVersion.FEATURE_NAME, AutoMQVersion.V5.featureLevel()),
            Map.of(AutoMQVersion.FEATURE_NAME, type), false);
        ApiError error = result.response().get(AutoMQVersion.FEATURE_NAME);
        assertEquals(Errors.INVALID_UPDATE_VERSION, error.error());
        assertTrue(error.message().contains("retained sharded objects"));
        assertTrue(result.records().isEmpty());
        assertEquals(AutoMQVersion.V6, manager.autoMQVersion());
    }

    /** An older broker cannot rejoin after reader support has been finalized. */
    @Test
    public void testOldBrokerRegistrationRejected() {
        ClusterControlManager cluster = new ClusterControlManager.Builder()
            .setQuorumVoters(List.of())
            .setFeatureControlManager(manager(AutoMQVersion.V6, AutoMQVersion.V6))
            .setBrokerUncleanShutdownHandler((id, records) -> { }).build();
        FinalizedControllerFeatures features = new FinalizedControllerFeatures(
            Map.of(AutoMQVersion.FEATURE_NAME, AutoMQVersion.V6.featureLevel()), 0);
        BrokerRegistrationRequestData.Feature old = new BrokerRegistrationRequestData.Feature()
            .setName(AutoMQVersion.FEATURE_NAME).setMinSupportedVersion(AutoMQVersion.V0.featureLevel())
            .setMaxSupportedVersion(AutoMQVersion.V5.featureLevel());
        assertThrows(UnsupportedVersionException.class, () -> cluster.processRegistrationFeature(1, features, old));
    }

    private static FeatureControlManager manager(AutoMQVersion broker, AutoMQVersion controller) {
        return new FeatureControlManager.Builder()
            .setMetadataVersion(MetadataVersion.IBP_3_9_IV0)
            .setQuorumFeatures(new QuorumFeatures(0, QuorumFeatures.defaultFeatureMap(false), List.of(0, 1)))
            .setClusterFeatureSupportDescriber(createFakeClusterFeatureSupportDescriber(
                List.of(Map.entry(2, supported(broker))), List.of(Map.entry(1, supported(controller)))))
            .build();
    }

    private static Map<String, VersionRange> supported(AutoMQVersion version) {
        return Map.of(AutoMQVersion.FEATURE_NAME, VersionRange.of(AutoMQVersion.V0.featureLevel(), version.featureLevel()));
    }

    private static FeatureLevelRecord record(AutoMQVersion version) {
        return new FeatureLevelRecord().setName(AutoMQVersion.FEATURE_NAME).setFeatureLevel(version.featureLevel());
    }
}
