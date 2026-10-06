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
import kafka.automq.zerozone.DefaultZeroZoneConfig.CIDRMatcher;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(60)
@Tag("S3Unit")
public class DefaultZeroZoneConfigTest {

    /**
     * Given configured zones, deleting their effective overrides clears CIDR matching and exclusions.
     */
    @Test
    public void testDeleteDynamicConfigs() throws Exception {
        KafkaConfig kafkaConfig = mock(KafkaConfig.class);
        when(kafkaConfig.originals()).thenReturn(Map.of());
        when(kafkaConfig.getList(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY)).thenReturn(List.of());
        DefaultZeroZoneConfig config = new DefaultZeroZoneConfig(kafkaConfig);
        AtomicInteger notifications = new AtomicInteger();
        config.registerListener(zones -> notifications.incrementAndGet());
        ClientIdMetadata client = ClientIdMetadata.of("", InetAddress.getByName("10.0.0.1"), null);
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", "az-a@10.0.0.0/24");
        effective.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, "az-c");
        config.reconfigure(effective);
        assertEquals("az-a", config.rack(client));
        assertEquals(Set.of("az-c"), config.excludeZones());

        effective.put("automq.zone.cidr.blocks", null);
        effective.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, List.of());
        config.validateReconfiguration(effective);
        assertEquals("az-a", config.rack(client));
        config.reconfigure(effective);
        assertNull(config.rack(client));
        assertEquals(Set.of(), config.excludeZones());
        assertEquals(2, notifications.get());
        config.reconfigure(effective);
        assertEquals(2, notifications.get());
    }

    @Test
    public void testCIDRFind() {
        CIDRMatcher matcher = new CIDRMatcher("us-east-1a@10.0.0.0/19,10.0.32.0/19<>us-east-1b@10.0.64.0/19<>us-east-1c@10.0.96.0/19");
        assertEquals("us-east-1a", matcher.find("10.0.31.233").zone());
        assertEquals("10.0.0.0/19", matcher.find("10.0.31.233").cidr());

        assertEquals("us-east-1a", matcher.find("10.0.32.233").zone());
        assertEquals("10.0.32.0/19", matcher.find("10.0.32.233").cidr());

        assertEquals("us-east-1b", matcher.find("10.0.65.0").zone());

        assertEquals("us-east-1c", matcher.find("10.0.97.0").zone());

        assertNull(matcher.find("10.0.128.0"));
    }

    /**
     * Given no CIDR block is configured, when an IPv6 client sends no zone hint, then it has no rack and no failure.
     */
    @Test
    public void testIPv6ClientWithoutCidrBlocks() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of());
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("::6"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:1:a01::5"), null)));
    }

    /**
     * Given a client without a resolved address, when its rack is looked up, then no rack is returned.
     */
    @Test
    public void testClientWithoutAddress() {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@10.0.0.0/24"));
        assertNull(config.rack(ClientIdMetadata.of("c")));
    }

    /**
     * Given a client with a zone hint, when its rack is looked up, then the hint wins over CIDR matching.
     */
    @Test
    public void testZoneHintWinsOverCidrBlocks() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@2001:db8::/32"));
        ClientIdMetadata client = ClientIdMetadata.of("automq_az=az-b&c", InetAddress.getByName("2001:db8::1"), null);
        assertEquals("az-b", config.rack(client));
    }

    /**
     * Given IPv4 and IPv6 blocks mapped to the same zones, when clients connect, then each address family is
     * matched against the blocks of its own family and the longest matching prefix wins.
     */
    @Test
    public void testMixedIPv4AndIPv6CidrBlocks() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks",
            "az-1@2001:db8:1:a00::/64,10.0.0.0/19<>az-2@2001:db8:1:a01::/64,10.0.32.0/19<>wide@2001:db8::/32"));
        assertEquals("az-1", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:1:a00:aea4::5"), null)));
        assertEquals("az-1", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.1.7"), null)));
        assertEquals("az-2", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:1:a01:19b0::5"), null)));
        assertEquals("az-2", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.40.1"), null)));
        assertEquals("wide", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:ffff::1"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db9::1"), null)));
    }

    /**
     * Given blocks of a single address family, when a client of the other family connects, then it never matches.
     */
    @Test
    public void testAddressFamiliesDoNotCrossMatch() throws Exception {
        DefaultZeroZoneConfig ipv4Only = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@0.0.0.0/0"));
        assertNull(ipv4Only.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8::1"), null)));
        assertEquals("az-a", ipv4Only.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.7"), null)));

        DefaultZeroZoneConfig ipv6Only = newConfig(Map.of("automq.zone.cidr.blocks", "az-b@::/0"));
        assertNull(ipv6Only.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.7"), null)));
        assertEquals("az-b", ipv6Only.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8::1"), null)));
    }

    /**
     * Given blocks at the prefix length boundaries of both families, when addresses are matched, then the longest
     * matching prefix wins.
     */
    @Test
    public void testLongestPrefixWinsAtBoundaries() {
        CIDRMatcher matcher = new CIDRMatcher(
            "z0@::/0<>z1@8000::/1<>z63@2001:db8:1:a00::/63<>z127@2001:db8:1:a01::10/127<>v4@0.0.0.0/0<>v4host@10.0.0.1/32");
        assertEquals("z127", matcher.find("2001:db8:1:a01::11").zone());
        assertEquals("z63", matcher.find("2001:db8:1:a01::12").zone());
        assertEquals("z63", matcher.find("2001:db8:1:a00::1").zone());
        assertEquals("z0", matcher.find("2001:db8:1:a02::1").zone());
        assertEquals("z1", matcher.find("ffff::1").zone());
        assertEquals("v4host", matcher.find("10.0.0.1").zone());
        assertEquals("v4", matcher.find("10.0.0.2").zone());
    }

    /**
     * Given an IPv4-mapped IPv6 client address, when its rack is looked up, then the IPv4 block of its zone matches.
     */
    @Test
    public void testIPv4MappedClientAddress() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@10.0.0.0/24"));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("::ffff:10.0.0.9"), null)));
        assertEquals("az-a", new CIDRMatcher("az-a@10.0.0.0/24").find("::ffff:10.0.0.9").zone());
    }

    /**
     * Given a block with zero-padded octets, when addresses are matched, then the octets keep their decimal meaning.
     */
    @Test
    public void testZeroPaddedDottedQuadKeepsDecimalMeaning() {
        CIDRMatcher matcher = new CIDRMatcher("az-a@010.000.008.000/21");
        assertEquals("az-a", matcher.find("10.0.8.1").zone());
        assertEquals("az-a", matcher.find("10.0.15.255").zone());
        assertNull(matcher.find("10.0.7.255"));
        assertNull(matcher.find("8.0.0.1"));
    }

    /**
     * Given octets padded beyond three characters, when addresses are matched, then they keep their decimal meaning.
     */
    @Test
    public void testLongZeroPaddedDottedQuadKeepsDecimalMeaning() {
        CIDRMatcher matcher = new CIDRMatcher("az-a@0010.0.0.0/8");
        assertEquals("az-a", matcher.find("10.1.2.3").zone());
        assertNull(matcher.find("11.1.2.3"));
    }

    /**
     * Given a block holding a single host, when addresses are matched, then only that address matches it.
     */
    @Test
    public void testHostBlocksMatchASingleAddress() {
        CIDRMatcher matcher = new CIDRMatcher("v6@2001:db8:1:a01::5/128<>v4@10.0.0.1/32");
        assertEquals("v6", matcher.find("2001:db8:1:a01::5").zone());
        assertNull(matcher.find("2001:db8:1:a01::6"));
        assertEquals("v4", matcher.find("10.0.0.1").zone());
        assertNull(matcher.find("10.0.0.2"));
    }

    /**
     * Given a block with more than one prefix length component, when it is parsed, then only the first component is
     * used, as in previous releases, and the block is reported.
     */
    @Test
    public void testExtraPrefixComponentsKeepTheFirstPrefix() {
        CIDRMatcher matcher = new CIDRMatcher("az-a@10.0.0.0/8/9");
        assertEquals("az-a", matcher.find("10.1.2.3").zone());
        assertNull(matcher.find("11.1.2.3"));
        assertEquals(List.of("Zone CIDR block az-a@10.0.0.0/8/9 has extra '/' components, only 10.0.0.0/8 is used"),
            matcher.warnings());
    }

    /**
     * Given an IPv6 block carrying a zone index, when it is parsed, then the index is dropped and the block matches
     * as if it had not been written.
     */
    @Test
    public void testScopedIPv6BlockDropsTheZoneIndex() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@fe80::1%eth0/64"));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("fe80::5"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("fe81::5"), null)));
    }

    /**
     * Given a zone segment holding more than one separator, when it is parsed, then it is skipped without notice and
     * the other segments still apply, which is the behavior of previous releases.
     */
    @Test
    public void testZoneSegmentWithSeveralSeparatorsIsSkipped() {
        CIDRMatcher matcher = new CIDRMatcher("az-a@10.0.0.0/24@extra<>az-b@10.0.1.0/24");
        assertNull(matcher.find("10.0.0.5"));
        assertEquals("az-b", matcher.find("10.0.1.5").zone());
        assertEquals(List.of(), matcher.warnings());
    }

    /**
     * Given IPv6 blocks applied dynamically, when they are later deleted, then the mapping is applied and cleared.
     */
    @Test
    public void testReconfigureIPv6Blocks() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of());
        ClientIdMetadata client = ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:1:a01::5"), null);
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", "az-2@2001:db8:1:a01::/64");
        config.validateReconfiguration(effective);
        config.reconfigure(effective);
        assertEquals("az-2", config.rack(client));

        effective.put("automq.zone.cidr.blocks", null);
        config.reconfigure(effective);
        assertNull(config.rack(client));
    }

    /**
     * Given a value holding blocks that only the previous IPv4-only parser accepted, when it is validated and
     * applied, then validation passes, the unusable blocks are skipped and the valid blocks of the same value apply.
     */
    @Test
    public void testLegacyBlocksAreSkippedButKeepTheValueApplicable() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of());
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", "az-a@10.0.0/24,10.0.1.0/24<>az-b@300.0.0.0/8,2001:db8::/64");
        config.validateReconfiguration(effective);
        config.reconfigure(effective);
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.1.5"), null)));
        assertEquals("az-b", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8::5"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.5"), null)));

        CIDRMatcher matcher = new CIDRMatcher((String) effective.get("automq.zone.cidr.blocks"));
        assertEquals(2, matcher.warnings().size());
        assertTrue(matcher.warnings().get(0).startsWith("Ignoring zone CIDR block az-a@10.0.0/24 "));
        assertTrue(matcher.warnings().get(1).startsWith("Ignoring zone CIDR block az-b@300.0.0.0/8 "));
    }

    /**
     * Given each shape the previous IPv4-only parser accepted, when it is validated, then it is still accepted.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "az@10.0.0/24",
        "az@1.2.3.4.5/8",
        "az@300.0.0.0/8",
        "az@10.0.0.0/33",
        "az@10.0.0.0/-1",
        "az@10.0.0.0/33/9",
    })
    public void testLegacyBlocksPassValidation(String blocks) throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of());
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", blocks);
        config.validateReconfiguration(effective);
        config.reconfigure(effective);
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.1"), null)));
    }

    /**
     * Given a block the previous IPv4-only parser rejected as well, when it is validated, then validation fails and
     * the active mapping is left untouched.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "az@10.0.0.0",
        "az@10.0.0.0/x",
        "az@not-an-ip/24",
        "az@feed:not-an-ip/64",
        "az@2001:db8::/129",
        "az@::ffff:10.0.0.0/104",
        "az@,10.0.0.0/24",
    })
    public void testInvalidBlocksAreRejected(String blocks) throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@10.0.0.0/24"));
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", blocks);
        effective.put(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY, "az-x");
        assertThrows(ConfigException.class, () -> config.validateReconfiguration(effective));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.1"), null)));
        assertEquals(Set.of(), config.excludeZones());
    }

    /**
     * Given a static configuration with IPv6 and legacy blocks, when the broker starts, then it does not fail and
     * the usable blocks are active.
     */
    @Test
    public void testStaticConfigWithIPv6AndLegacyBlocks() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks",
            "az-a@2001:db8:1:a00::/64,10.0.0/24<>az-b@10.0.1.0/24"));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:1:a00::9"), null)));
        assertEquals("az-b", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.1.9"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.9"), null)));
    }

    /**
     * Given a string that is not an address literal, when it is looked up, then no block matches and no name is resolved.
     */
    @Test
    public void testFindIgnoresNonLiterals() {
        CIDRMatcher matcher = new CIDRMatcher("z0@::/0<>v4@0.0.0.0/0");
        assertNull(matcher.find("not-an-ip"));
        assertNull(matcher.find("feed:not-an-ip"));
        assertNull(matcher.find("1::2::3"));
        assertNull(matcher.find("10.0.0"));
        assertNull(matcher.find(""));
        assertNull(matcher.find((String) null));
    }

    /**
     * Given no configured block, when the matcher is asked, then it reports itself as empty.
     */
    @Test
    public void testEmptyMatcher() {
        assertTrue(new CIDRMatcher("").isEmpty());
        assertTrue(new CIDRMatcher("az-a@10.0.0/24").isEmpty());
        assertFalse(new CIDRMatcher("az-a@10.0.0.0/24").isEmpty());
    }

    private static DefaultZeroZoneConfig newConfig(Map<String, Object> originals) {
        KafkaConfig kafkaConfig = mock(KafkaConfig.class);
        when(kafkaConfig.originals()).thenReturn(originals);
        when(kafkaConfig.getList(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY)).thenReturn(List.of());
        return new DefaultZeroZoneConfig(kafkaConfig);
    }
}
