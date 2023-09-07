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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumFeaturesTest {
    private final static Map<String, VersionRange> LOCAL;

    private final static QuorumFeatures QUORUM_FEATURES;

    static {
        Map<String, VersionRange> local = new HashMap<>();
        local.put("foo", VersionRange.of(0, 3));
        local.put("bar", VersionRange.of(0, 4));
        local.put("baz", VersionRange.of(2, 2));
        LOCAL = Collections.unmodifiableMap(local);
        QUORUM_FEATURES = new QuorumFeatures(0, LOCAL, Arrays.asList(0, 1, 2));
    }

    @Test
    public void testLocalSupportedFeature() {
        assertEquals(VersionRange.of(0, 3), QUORUM_FEATURES.localSupportedFeature("foo"));
        assertEquals(VersionRange.of(0, 4), QUORUM_FEATURES.localSupportedFeature("bar"));
        assertEquals(VersionRange.of(2, 2), QUORUM_FEATURES.localSupportedFeature("baz"));
        assertEquals(VersionRange.of(0, 0), QUORUM_FEATURES.localSupportedFeature("quux"));
    }

    @Test
    public void testReasonNotSupported() {
        assertEquals(Optional.of("Local controller 0 only supports versions 0-3"),
            QuorumFeatures.reasonNotSupported((short) 10,
                "Local controller 0", VersionRange.of(0, 3)));
        assertEquals(Optional.empty(),
            QuorumFeatures.reasonNotSupported((short) 3,
                "Local controller 0", VersionRange.of(0, 3)));
    }

    @Test
    public void testIsControllerId() {
        assertTrue(QUORUM_FEATURES.isControllerId(0));
        assertTrue(QUORUM_FEATURES.isControllerId(1));
        assertTrue(QUORUM_FEATURES.isControllerId(2));
        assertFalse(QUORUM_FEATURES.isControllerId(3));
    }

    @Test
    public void testZkMigrationNotReadyIfMetadataVersionTooLow() {
        assertEquals(Optional.of("Metadata version too low at 3.0-IV1"),
            QUORUM_FEATURES.reasonAllControllersZkMigrationNotReady(
                MetadataVersion.IBP_3_0_IV1, Collections.emptyMap()));
    }

    @Test
    public void testZkMigrationReadyIfControllerRegistrationNotSupported() {
        assertEquals(Optional.empty(),
            QUORUM_FEATURES.reasonAllControllersZkMigrationNotReady(
                MetadataVersion.IBP_3_4_IV0, Collections.emptyMap()));
    }

    @Test
    public void testZkMigrationNotReadyIfNotAllControllersRegistered() {
        assertEquals(Optional.of("No registration found for controller 0"),
            QUORUM_FEATURES.reasonAllControllersZkMigrationNotReady(
                MetadataVersion.IBP_3_7_IV0, Collections.emptyMap()));
    }

    @Test
    public void testZkMigrationNotReadyIfControllerNotReady() {
        assertEquals(Optional.of("Controller 0 has not enabled zookeeper.metadata.migration.enable"),
            QUORUM_FEATURES.reasonAllControllersZkMigrationNotReady(
                MetadataVersion.IBP_3_7_IV0, Collections.singletonMap(0,
                    new ControllerRegistration.Builder().
                        setId(0).
                        setZkMigrationReady(false).
                        setIncarnationId(Uuid.fromString("kCBJaDGNQk6x3y5xbtQOpg")).
                        setListeners(Collections.singletonMap("CONTROLLER",
                                new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
                        build())));
    }

    @Test
    public void testZkMigrationReadyIfAllControllersReady() {
        Map<Integer, ControllerRegistration> controllers = new HashMap<>();
        QUORUM_FEATURES.quorumNodeIds().forEach(id -> {
            controllers.put(id,
                new ControllerRegistration.Builder().
                    setId(id).
                    setZkMigrationReady(true).
                    setIncarnationId(Uuid.fromString("kCBJaDGNQk6x3y5xbtQOpg")).
                    setListeners(Collections.singletonMap("CONTROLLER",
                        new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "localhost", 9093))).
                    build());
        });
        assertEquals(Optional.empty(), QUORUM_FEATURES.reasonAllControllersZkMigrationNotReady(
            MetadataVersion.IBP_3_7_IV0, controllers));
    }
}
