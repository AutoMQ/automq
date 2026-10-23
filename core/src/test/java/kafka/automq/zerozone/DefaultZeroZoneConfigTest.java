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
import kafka.automq.zerozone.DefaultZeroZoneConfig.CIDRBlock;
import kafka.automq.zerozone.DefaultZeroZoneConfig.CIDRMatcher;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Comparator;
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
     * IPv4 client addresses checked against the matching of previous releases.
     */
    private static final List<String> IPV4_CLIENTS = List.of("0.10.0.5", "2.3.4.5", "44.1.2.3", "10.1.2.3",
        "10.0.0.1", "10.0.0.200", "10.0.1.5", "10.255.255.255", "11.0.0.1", "192.168.1.1", "255.255.255.255",
        "0.0.0.0", "255.0.0.1");
    /**
     * IPv6 blocks covering every client family member and sharing declared prefix lengths with IPv4 blocks.
     */
    private static final String IPV6_BLOCKS = "v6@::/0,2001:db8::/32,2001:db8::/72,2001:db8:8000::/33,::1/128,::/24";

    /**
     * Initializes {@link KafkaConfig} before {@link DefaultZeroZoneConfig}, as a broker does. These tests only use a
     * mocked {@link KafkaConfig}, and initializing {@link DefaultZeroZoneConfig} first would make the real
     * {@link KafkaConfig} of the same JVM miss the zone configs, which later tests such as
     * {@link DefaultZeroZoneConfigDynamicConfigTest} rely on.
     */
    @BeforeAll
    public static void initializeKafkaConfigFirst() {
        KafkaConfig.configNames();
    }

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

    static List<String> ipv4Configs() {
        return List.of(
            "az@10.0.0.0/24",
            "az@10.0.0/24",
            "az@1.2.3.4.5/8",
            "az@300.0.0.0/8",
            "az@10.0.0.0/33",
            "az@10.0.0.0/72",
            "az@10.0.0.0/96",
            "az@10.0.0.0/-1",
            "az@10.0.0.0/-32",
            "az@010.0.0.0/8",
            "az@0010.000.000.001/32",
            "az@10.0.0.0/8/9",
            "az@+10.0.0.0/+8",
            "az@10.0.0.0./8",
            "az@4294967295.0.0.0/8",
            "az@10.0.0.0/8,",
            "@10.0.0.0/8",
            "host@10.0.0.1/32<>all@10.0.0.0/33",
            "net@10.0.0.0/24<>odd@10.0.0.0/72",
            "a@10.0.0.0/8,10.0.0.0/16<>b@10.0.0.0/16<>c@0.0.0.0/0",
            "b@10.0.0.0/8<>a@10.0.0.0/8",
            "a@10.0.0.0/24@extra<>b@10.0.0.0/8",
            "10.0.0.0/24<>b@10.0.0.0/8",
            "az-a@10.0.0/24,10.0.1.0/24<>az-b@300.0.0.0/8<>az-c@192.168.0.0/16,0.0.0.0/0",
            "",
            "<>"
        );
    }

    /**
     * Given any IPv4-only value previous releases accepted, standard or not, when IPv4 clients are matched by address
     * and by address string, then every client gets the same block as with the matcher of previous releases.
     */
    @ParameterizedTest
    @MethodSource("ipv4Configs")
    public void testIPv4MatchingIsUnchanged(String config) throws Exception {
        LegacyCIDRMatcher legacy = new LegacyCIDRMatcher(config);
        CIDRMatcher matcher = new CIDRMatcher(config);
        for (String ip : IPV4_CLIENTS) {
            InetAddress address = InetAddress.getByName(ip);
            LegacyCIDRBlock expected = legacy.find(address.getHostAddress());
            assertSameBlock(expected, matcher.find(address), config + " / " + ip);
            assertSameBlock(expected, matcher.find(ip), config + " / " + ip);
        }
    }

    /**
     * Given any IPv4-only value previous releases accepted, when IPv6 blocks are added to it, then no IPv4 client
     * gets another block than with the matcher of previous releases on the IPv4 part alone.
     */
    @ParameterizedTest
    @MethodSource("ipv4Configs")
    public void testIPv6BlocksDoNotChangeIPv4Matching(String config) throws Exception {
        LegacyCIDRMatcher legacy = new LegacyCIDRMatcher(config);
        CIDRMatcher mixed = new CIDRMatcher(config + "<>" + IPV6_BLOCKS);
        for (String ip : IPV4_CLIENTS) {
            InetAddress address = InetAddress.getByName(ip);
            LegacyCIDRBlock expected = legacy.find(address.getHostAddress());
            assertSameBlock(expected, mixed.find(address), config + " / " + ip);
            assertSameBlock(expected, mixed.find(ip), config + " / " + ip);
        }
        assertEquals("v6", mixed.find(InetAddress.getByName("2001:db9::1")).zone());
    }

    /**
     * Given any IPv4-only value previous releases accepted, when it is validated and applied, then validation
     * passes and clients get the zone previous releases gave them.
     */
    @ParameterizedTest
    @MethodSource("ipv4Configs")
    public void testLegacyIPv4ValuesPassValidationAndApply(String blocks) throws Exception {
        LegacyCIDRMatcher legacy = new LegacyCIDRMatcher(blocks);
        DefaultZeroZoneConfig config = newConfig(Map.of());
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", blocks);
        config.validateReconfiguration(effective);
        config.reconfigure(effective);
        for (String ip : IPV4_CLIENTS) {
            LegacyCIDRBlock expected = legacy.find(ip);
            assertEquals(expected == null ? null : expected.zone, config.rack(ClientIdMetadata.of("c",
                InetAddress.getByName(ip), null)), blocks + " / " + ip);
        }
    }

    /**
     * Given an IPv4 value previous releases rejected, when it is parsed or validated, then it is still rejected,
     * now as a {@link ConfigException} on the zone CIDR blocks config.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "az@10.0.0.0",
        "az@a.b.c.d/8",
        "az@10.0.0.0/x",
        "az@10..0.0/8",
        "az@ 10.0.0.0/8",
        "az@-1.0.0.0/8",
        "az@,10.0.0.0/8",
        "az@10.0.0.0/",
        "az@/8",
        "az@/",
        "az@10.0.0.0/8 ,10.0.1.0/24",
        "az@.10.0.0.0/8",
        "az@99999999999.0.0.0/8",
        "az@10.0.0.0/24<>az-b@10.0.0.0",
    })
    public void testLegacyRejectedValuesAreStillRejected(String blocks) {
        assertThrows(RuntimeException.class, () -> new LegacyCIDRMatcher(blocks));
        ConfigException exception = assertThrows(ConfigException.class, () -> new CIDRMatcher(blocks));
        assertTrue(exception.getMessage().contains("automq.zone.cidr.blocks"), exception.getMessage());

        DefaultZeroZoneConfig config = newConfig(Map.of());
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", blocks);
        assertThrows(ConfigException.class, () -> config.validateReconfiguration(effective));
    }

    /**
     * Given non-standard IPv4 blocks previous releases accepted, when they are parsed, then each is reported with the
     * addresses it actually matches and the prefix length it is ranked by.
     */
    @Test
    public void testNonStandardIPv4BlocksAreReported() {
        CIDRMatcher matcher = new CIDRMatcher("az-a@300.0.0.0/8,10.0.0.0/24<>az-b@10.0.0/24,10.0.0.0/33,10.0.0.0/8/9");
        assertEquals(List.of(
            "Zone CIDR block az-a@300.0.0.0/8 is not a standard IPv4 CIDR block; it is kept for compatibility and"
                + " matches as 44.0.0.0/8, ranked by its declared prefix length 8",
            "Zone CIDR block az-b@10.0.0/24 is not a standard IPv4 CIDR block; it is kept for compatibility and"
                + " matches as 0.10.0.0/24, ranked by its declared prefix length 24",
            "Zone CIDR block az-b@10.0.0.0/33 is not a standard IPv4 CIDR block; it is kept for compatibility and"
                + " matches as 0.0.0.0/0, ranked by its declared prefix length 33",
            "Zone CIDR block az-b@10.0.0.0/8/9 is not a standard IPv4 CIDR block; it is kept for compatibility and"
                + " matches as 10.0.0.0/8, ranked by its declared prefix length 8"),
            matcher.warnings());
    }

    /**
     * Given each non-standard IPv4 form previous releases accepted, when it is parsed, then it is reported.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "az@10.0.0/24",
        "az@1.2.3.4.5/8",
        "az@300.0.0.0/8",
        "az@10.0.0.0/33",
        "az@10.0.0.0/72",
        "az@10.0.0.0/-1",
        "az@010.0.0.0/8",
        "az@10.0.0.0/08",
        "az@10.0.0.0/8/9",
        "az@+10.0.0.0/+8",
        "az@10.0.0.0./8",
        "az@4294967295.0.0.0/8",
    })
    public void testNonStandardIPv4FormsAreReported(String blocks) {
        assertEquals(1, new CIDRMatcher(blocks).warnings().size());
    }

    /**
     * Given standard IPv4 blocks and IPv6 blocks, when they are parsed, then nothing is reported.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "az@10.0.0.0/8",
        "az@0.0.0.0/0",
        "az@255.255.255.255/32",
        "az@192.168.1.0/24,172.16.0.0/12<>az-b@100.64.0.0/10",
        "az@2001:db8::/32",
        "az@::/0,2001:db8:1:a01::5/128",
        "az@10.0.0.0/19,2001:db8:1:a00::/64<>az-b@192.0.2.0/24,2001:db8:1:a01::/64",
        "",
    })
    public void testStandardBlocksAreNotReported(String blocks) {
        assertEquals(List.of(), new CIDRMatcher(blocks).warnings());
    }

    /**
     * Given a value holding non-standard IPv4 blocks next to IPv6 blocks, when it is validated and applied, then
     * every block applies and the IPv4 ones keep the meaning previous releases gave them.
     */
    @Test
    public void testLegacyBlocksKeepTheirLegacyMeaning() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of());
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", "az-a@10.0.0/24,10.0.1.0/24<>az-b@300.0.0.0/8,2001:db8::/64");
        config.validateReconfiguration(effective);
        config.reconfigure(effective);
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.1.5"), null)));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("0.10.0.5"), null)));
        assertEquals("az-b", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("44.1.2.3"), null)));
        assertEquals("az-b", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8::5"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.5"), null)));
    }

    /**
     * Given a static configuration with IPv6 and non-standard IPv4 blocks, when the broker starts, then it does not
     * fail, every block is active and the IPv4 ones keep their legacy meaning.
     */
    @Test
    public void testStaticConfigWithIPv6AndLegacyBlocks() throws Exception {
        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks",
            "az-a@2001:db8:1:a00::/64,10.0.0/24<>az-b@10.0.1.0/24"));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("2001:db8:1:a00::9"), null)));
        assertEquals("az-a", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("0.10.0.9"), null)));
        assertEquals("az-b", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.1.9"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.9"), null)));
    }

    /**
     * Given a lookup that fails unexpectedly, when a client address is matched, then no zone is returned instead of
     * the failure reaching the request.
     */
    @Test
    public void testFindFailureReturnsNoZone() {
        InetAddress address = mock(InetAddress.class);
        when(address.getAddress()).thenThrow(new IllegalStateException("unexpected"));
        CIDRMatcher matcher = new CIDRMatcher("az-a@0.0.0.0/0<>az-b@::/0");
        assertNull(matcher.find(address));

        DefaultZeroZoneConfig config = newConfig(Map.of("automq.zone.cidr.blocks", "az-a@0.0.0.0/0<>az-b@::/0"));
        assertNull(config.rack(ClientIdMetadata.of("c", address, null)));
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
        assertNull(new CIDRMatcher("az-a@0.0.0.0/0").find((InetAddress) null));
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

        // A legacy IPv4 block matching every IPv4 address still never matches an IPv6 client.
        assertNull(new CIDRMatcher("az-a@10.0.0.0/33").find(InetAddress.getByName("2001:db8::1")));
    }

    /**
     * Given blocks of both families, when they are checked directly, then a block only contains addresses of its
     * own family and addresses of an unknown length match nothing.
     */
    @Test
    public void testBlockContainsIsFamilyAware() {
        CIDRBlock ipv4 = new CIDRBlock("0.0.0.0/0", 0L, 0L, 0, "z");
        CIDRBlock ipv6 = new CIDRBlock("::/0", new byte[16], 0, "z");
        assertTrue(ipv4.contains(new byte[4]));
        assertFalse(ipv4.contains(new byte[16]));
        assertTrue(ipv4.contains(0L));
        assertTrue(ipv6.contains(new byte[16]));
        assertFalse(ipv6.contains(new byte[4]));
        assertFalse(ipv6.contains(0L));
        assertFalse(ipv4.contains(new byte[8]));
        assertFalse(ipv6.contains(new byte[8]));
        assertThrows(IllegalArgumentException.class, () -> new CIDRBlock("x", new byte[4], 0, "z"));
        assertThrows(IllegalArgumentException.class, () -> new CIDRBlock("x", new byte[16], 129, "z"));
        assertThrows(IllegalArgumentException.class, () -> new CIDRBlock("x", new byte[16], -1, "z"));
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
     * Given a zone segment holding more than one separator, when it is parsed, then it is skipped and reported while
     * the other segments still apply, which is the behavior of previous releases.
     */
    @Test
    public void testZoneSegmentWithSeveralSeparatorsIsSkipped() throws Exception {
        CIDRMatcher matcher = new CIDRMatcher("az-a@10.0.0.0/24@extra<>az-b@10.0.1.0/24");
        assertNull(matcher.find("10.0.0.5"));
        assertEquals("az-b", matcher.find("10.0.1.5").zone());
        assertEquals(1, matcher.warnings().size());
        assertTrue(matcher.warnings().get(0).startsWith("Ignoring zone segment az-a@10.0.0.0/24@extra "));

        DefaultZeroZoneConfig config = newConfig(Map.of());
        Map<String, Object> effective = new HashMap<>();
        effective.put("automq.zone.cidr.blocks", "az-a@10.0.0.0/24@extra<>az-b@10.0.1.0/24");
        config.validateReconfiguration(effective);
        config.reconfigure(effective);
        assertEquals("az-b", config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.1.5"), null)));
        assertNull(config.rack(ClientIdMetadata.of("c", InetAddress.getByName("10.0.0.5"), null)));
    }

    /**
     * Given a zone segment without a separator, when it is parsed, then it is skipped and reported, while a value
     * holding no segment at all is not reported.
     */
    @Test
    public void testZoneSegmentWithoutSeparatorIsReported() {
        CIDRMatcher matcher = new CIDRMatcher("10.0.0.0/24<>az-b@10.0.1.0/24");
        assertEquals("az-b", matcher.find("10.0.1.5").zone());
        assertEquals(1, matcher.warnings().size());
        assertTrue(matcher.warnings().get(0).startsWith("Ignoring zone segment 10.0.0.0/24 "));
        assertEquals(List.of(), new CIDRMatcher("").warnings());
        assertEquals(List.of(), new CIDRMatcher("<>").warnings());
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
     * Given an invalid IPv6 block, an IPv4-mapped block or an IPv4 block previous releases rejected, when it is
     * validated, then validation fails and the active mapping is left untouched.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "az@10.0.0.0",
        "az@10.0.0.0/x",
        "az@not-an-ip/24",
        "az@feed:not-an-ip/64",
        "az@2001:db8::/129",
        "az@2001:db8::/-1",
        "az@2001:db8::/x",
        "az@2001:db8::",
        "az@2001:db8::/64/8",
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
     * Given an IPv4-mapped IPv6 block, when it is parsed, then the rejection recommends the plain IPv4 form.
     */
    @Test
    public void testIPv4MappedBlockRecommendsThePlainForm() {
        ConfigException exception = assertThrows(ConfigException.class,
            () -> new CIDRMatcher("az@::ffff:10.0.0.0/104"));
        assertTrue(exception.getMessage().contains("plain IPv4 form"), exception.getMessage());
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
        assertNull(matcher.find("10.0.0.x"));
        assertNull(matcher.find(""));
        assertNull(matcher.find((String) null));
    }

    /**
     * Given no configured block, when the matcher is asked, then it reports itself as empty.
     */
    @Test
    public void testEmptyMatcher() {
        assertTrue(new CIDRMatcher("").isEmpty());
        assertTrue(new CIDRMatcher("az-a@10.0.0.0/24@extra").isEmpty());
        assertFalse(new CIDRMatcher("az-a@10.0.0/24").isEmpty());
        assertFalse(new CIDRMatcher("az-a@10.0.0.0/24").isEmpty());
        assertFalse(new CIDRMatcher("az-a@::/0").isEmpty());
    }

    private static void assertSameBlock(LegacyCIDRBlock expected, CIDRBlock actual, String message) {
        if (expected == null) {
            assertNull(actual, message);
            return;
        }
        assertEquals(expected.zone, actual == null ? null : actual.zone(), message);
        assertEquals(expected.cidr, actual.cidr(), message);
    }

    private static DefaultZeroZoneConfig newConfig(Map<String, Object> originals) {
        KafkaConfig kafkaConfig = mock(KafkaConfig.class);
        when(kafkaConfig.originals()).thenReturn(originals);
        when(kafkaConfig.getList(DefaultZeroZoneConfig.EXCLUDE_ZONES_CONFIG_KEY)).thenReturn(List.of());
        return new DefaultZeroZoneConfig(kafkaConfig);
    }

    /**
     * A verbatim copy of the IPv4-only matcher of previous releases, used as the reference for the IPv4 behavior
     * that must be preserved.
     */
    private static final class LegacyCIDRMatcher {
        private final Map<Integer, List<LegacyCIDRBlock>> maskLength2blocks = new HashMap<>();
        private final List<Integer> reverseMaskLengthList = new ArrayList<>();

        LegacyCIDRMatcher(String config) {
            for (String cidrBlocksOfZone : config.split("<>")) {
                String[] parts = cidrBlocksOfZone.split("@");
                if (parts.length != 2) {
                    continue;
                }
                String zone = parts[0];
                String[] cidrList = parts[1].split(",");
                for (String cidr : cidrList) {
                    LegacyCIDRBlock block = parseCidr(cidr, zone);
                    maskLength2blocks
                        .computeIfAbsent(block.prefixLength, k -> new ArrayList<>())
                        .add(block);
                }
            }
            reverseMaskLengthList.addAll(maskLength2blocks.keySet());
            reverseMaskLengthList.sort(Comparator.reverseOrder());
        }

        LegacyCIDRBlock find(String ip) {
            long ipLong = ipToLong(ip);
            for (int prefix : reverseMaskLengthList) {
                List<LegacyCIDRBlock> blocks = maskLength2blocks.get(prefix);
                if (blocks != null) {
                    for (LegacyCIDRBlock block : blocks) {
                        if (block.contains(ipLong)) {
                            return block;
                        }
                    }
                }
            }
            return null;
        }

        private LegacyCIDRBlock parseCidr(String cidr, String zone) {
            String[] parts = cidr.split("/");
            String ip = parts[0];
            int maskLength = Integer.parseInt(parts[1]);
            long ipLong = ipToLong(ip);
            long mask = (0xFFFFFFFFL << (32 - maskLength)) & 0xFFFFFFFFL;
            long networkAddress = ipLong & mask;
            return new LegacyCIDRBlock(cidr, networkAddress, mask, maskLength, zone);
        }

        private long ipToLong(String ipAddress) {
            String[] octets = ipAddress.split("\\.");
            long result = 0;
            for (String octet : octets) {
                result = (result << 8) | Integer.parseUnsignedInt(octet);
            }
            return result;
        }
    }

    private static final class LegacyCIDRBlock {
        private final String cidr;
        private final long networkAddress;
        private final long mask;
        private final int prefixLength;
        private final String zone;

        LegacyCIDRBlock(String cidr, long networkAddress, long mask, int prefixLength, String zone) {
            this.cidr = cidr;
            this.networkAddress = networkAddress;
            this.mask = mask;
            this.prefixLength = prefixLength;
            this.zone = zone;
        }

        boolean contains(long ip) {
            return (ip & mask) == networkAddress;
        }
    }
}
