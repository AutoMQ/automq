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

package kafka.automq.zerozone;

import kafka.automq.interceptor.ClientIdMetadata;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link DefaultZeroZoneConfig} through the real {@link kafka.server.DynamicBrokerConfig} path, where the
 * persisted KRaft configs of a broker are replayed as one batch and validation runs before every apply.
 */
@Timeout(60)
@Tag("S3Unit")
public class DefaultZeroZoneConfigDynamicConfigTest {
    private static final String ZONE_CIDR_BLOCKS_CONFIG_KEY = "automq.zone.cidr.blocks";
    /**
     * Holds a block that only the previous IPv4-only parser accepted, an IPv4 block and an IPv6 block.
     */
    private static final String MIXED_BLOCKS = "az-a@10.0.0/24,10.0.1.0/24<>az-b@2001:db8:1:a01::/64";

    private KafkaConfig kafkaConfig;
    private DefaultZeroZoneConfig zeroZoneConfig;

    @BeforeEach
    public void setup(@TempDir Path logDir) {
        kafkaConfig = KafkaConfig.fromProps(brokerProps(logDir));
        kafkaConfig.dynamicConfig().initialize(Option.empty(), Option.empty());
        zeroZoneConfig = new DefaultZeroZoneConfig(kafkaConfig);
        kafkaConfig.addReconfigurable(zeroZoneConfig);
    }

    /**
     * Given per-broker configs holding a legacy block next to IPv4 and IPv6 blocks, when they are replayed, then the
     * whole batch is applied instead of being dropped.
     */
    @Test
    public void testPerBrokerConfigsAreReplayed() throws Exception {
        Properties props = new Properties();
        props.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, MIXED_BLOCKS);
        props.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, "az-c");
        kafkaConfig.dynamicConfig().updateBrokerConfig(kafkaConfig.brokerId(), props, false);

        assertAppliedMixedBlocks();
    }

    /**
     * Given cluster default configs holding a legacy block next to IPv4 and IPv6 blocks, when they are replayed,
     * then the whole batch is applied instead of being dropped.
     */
    @Test
    public void testClusterDefaultConfigsAreReplayed() throws Exception {
        Properties props = new Properties();
        props.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, MIXED_BLOCKS);
        props.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, "az-c");
        kafkaConfig.dynamicConfig().updateDefaultConfig(props, false);

        assertAppliedMixedBlocks();
    }

    /**
     * Given an already applied legacy block, when an unrelated excluded zone is updated, then validation of the
     * unchanged block does not reject the update.
     */
    @Test
    public void testUnrelatedUpdateIsNotBlockedByLegacyBlocks() throws Exception {
        Properties applied = new Properties();
        applied.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, MIXED_BLOCKS);
        kafkaConfig.dynamicConfig().updateDefaultConfig(applied, false);
        assertEquals("az-a", zeroZoneConfig.rack(client("10.0.1.5")));

        Properties update = new Properties();
        update.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, MIXED_BLOCKS);
        update.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, "az-c");
        assertDoesNotThrow(() -> kafkaConfig.dynamicConfig().validate(update, false));
        kafkaConfig.dynamicConfig().updateDefaultConfig(update, false);

        assertAppliedMixedBlocks();
    }

    /**
     * Given a legacy cluster default value shadowed by a valid per-broker override, when the override is deleted,
     * then the fallback to the cluster default still passes validation and applies its usable blocks.
     */
    @Test
    public void testFallbackToLegacyClusterDefaultIsStillApplied() throws Exception {
        Properties clusterDefault = new Properties();
        clusterDefault.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, MIXED_BLOCKS);
        kafkaConfig.dynamicConfig().updateDefaultConfig(clusterDefault, false);
        assertEquals("az-a", zeroZoneConfig.rack(client("10.0.1.5")));

        Properties perBroker = new Properties();
        perBroker.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, "az-1@192.0.2.0/24");
        assertDoesNotThrow(() -> kafkaConfig.dynamicConfig().validate(perBroker, true));
        kafkaConfig.dynamicConfig().updateBrokerConfig(kafkaConfig.brokerId(), perBroker, false);
        assertEquals("az-1", zeroZoneConfig.rack(client("192.0.2.5")));
        assertNull(zeroZoneConfig.rack(client("10.0.1.5")));

        // Deleting the override brings the legacy cluster default back as the effective value.
        Properties deletion = new Properties();
        assertDoesNotThrow(() -> kafkaConfig.dynamicConfig().validate(deletion, true));
        kafkaConfig.dynamicConfig().updateBrokerConfig(kafkaConfig.brokerId(), deletion, false);

        KafkaConfig currentConfig = kafkaConfig.dynamicConfig().currentKafkaConfig();
        assertEquals(MIXED_BLOCKS, currentConfig.originals().get(ZONE_CIDR_BLOCKS_CONFIG_KEY));
        assertEquals("az-a", zeroZoneConfig.rack(client("10.0.1.5")));
        assertEquals("az-b", zeroZoneConfig.rack(client("2001:db8:1:a01::5")));
        assertNull(zeroZoneConfig.rack(client("10.0.0.5")));
        assertNull(zeroZoneConfig.rack(client("192.0.2.5")));
    }

    /**
     * Given a block that the previous IPv4-only parser rejected as well, when it is validated, then the update is
     * rejected and the active mapping is left untouched.
     */
    @Test
    public void testMalformedBlocksAreRejectedByValidation() throws Exception {
        Properties applied = new Properties();
        applied.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, MIXED_BLOCKS);
        kafkaConfig.dynamicConfig().updateDefaultConfig(applied, false);

        Properties update = new Properties();
        update.put(ZONE_CIDR_BLOCKS_CONFIG_KEY, "az-d@10.9.0.0");
        update.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, "az-c");
        assertThrows(ConfigException.class, () -> kafkaConfig.dynamicConfig().validate(update, false));

        kafkaConfig.dynamicConfig().updateDefaultConfig(update, false);
        assertEquals("az-a", zeroZoneConfig.rack(client("10.0.1.5")));
        assertEquals(Set.of(), zeroZoneConfig.excludeZones());
    }

    private void assertAppliedMixedBlocks() throws UnknownHostException {
        // Asserted first because a dropped batch is only logged, and the effective config shows it right away.
        KafkaConfig currentConfig = kafkaConfig.dynamicConfig().currentKafkaConfig();
        assertEquals(MIXED_BLOCKS, currentConfig.originals().get(ZONE_CIDR_BLOCKS_CONFIG_KEY));
        assertEquals(List.of("az-c"), currentConfig.getList(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY));

        assertEquals(Set.of("az-c"), zeroZoneConfig.excludeZones());
        assertEquals("az-a", zeroZoneConfig.rack(client("10.0.1.5")));
        assertEquals("az-b", zeroZoneConfig.rack(client("2001:db8:1:a01::5")));
        assertNull(zeroZoneConfig.rack(client("10.0.0.5")));
    }

    private static ClientIdMetadata client(String address) throws UnknownHostException {
        return ClientIdMetadata.of("c", InetAddress.getByName(address), null);
    }

    private static Properties brokerProps(Path logDir) {
        Properties props = new Properties();
        props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker");
        props.put(KRaftConfigs.NODE_ID_CONFIG, "1");
        props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1000@localhost:9093");
        props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:9092");
        props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:9092");
        props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT");
        props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT");
        props.put(ServerLogConfigs.LOG_DIRS_CONFIG, logDir.toString());
        return props;
    }
}
