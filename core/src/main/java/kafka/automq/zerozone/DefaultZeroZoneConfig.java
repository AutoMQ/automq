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
import kafka.server.DynamicBrokerConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.automq.stream.utils.LogSuppressor;
import com.google.common.net.InetAddresses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class DefaultZeroZoneConfig implements ZeroZoneConfig, Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultZeroZoneConfig.class);
    private static final LogSuppressor FIND_FAILURE_LOGGER = new LogSuppressor(LOGGER, 60000);
    private static final String ZONE_CIDR_BLOCKS_CONFIG_KEY = "automq.zone.cidr.blocks";
    private static final String ZONE_CIDR_BLOCKS_CONFIG_DOC = "The mapping of zone to IPv4 or IPv6 CIDR blocks. "
        + "Format: zone1@cidr1,cidr2<>zone2@cidr3,cidr4. IPv6 blocks must only be configured once every broker runs "
        + "a release that supports them, and must be removed before downgrading to an earlier release.";
    public static final String EXCLUDE_ZONES_CONFIG_KEY = "automq.zerozone.exclude.zones";
    public static final String EXCLUDE_ZONES_CONFIG_DOC = "The availability zones excluded from ZeroZone proxying. Format: zone1,zone2";
    private static final Set<String> RECONFIGURABLE_CONFIGS;
    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private final KafkaConfig kafkaConfig;
    private volatile CIDRMatcher cidrMatcher = new CIDRMatcher("");
    private volatile Set<String> excludeZones = Collections.emptySet();
    private final List<Consumer<Set<String>>> listeners = new CopyOnWriteArrayList<>();

    static {
        RECONFIGURABLE_CONFIGS = Set.of(
            ZONE_CIDR_BLOCKS_CONFIG_KEY,
            EXCLUDE_ZONES_CONFIG_KEY
        );
        RECONFIGURABLE_CONFIGS.forEach(DynamicBrokerConfig.AllDynamicConfigs()::add);
        CONFIG_DEF.define(ZONE_CIDR_BLOCKS_CONFIG_KEY, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, ZONE_CIDR_BLOCKS_CONFIG_DOC);
        CONFIG_DEF.define(EXCLUDE_ZONES_CONFIG_KEY, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, EXCLUDE_ZONES_CONFIG_DOC);
    }

    public DefaultZeroZoneConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        // Read static config from server.properties on initialization
        final String staticValue = (String) kafkaConfig.originals().get(ZONE_CIDR_BLOCKS_CONFIG_KEY);
        if (staticValue != null) {
            this.cidrMatcher = new CIDRMatcher(staticValue);
            LOGGER.info("Initialized with static zone CIDR blocks: {}", staticValue);
            logWarnings(this.cidrMatcher);
        }
        this.excludeZones = Collections.unmodifiableSet(new HashSet<>(kafkaConfig.getList(EXCLUDE_ZONES_CONFIG_KEY)));
    }

    @Override
    public String rack(ClientIdMetadata clientId) {
        String rack = clientId.rack();
        if (rack != null) {
            return rack;
        }
        CIDRMatcher matcher = cidrMatcher;
        InetAddress clientAddress = clientId.clientAddress();
        if (clientAddress == null || matcher.isEmpty()) {
            return null;
        }
        CIDRBlock block = matcher.find(clientAddress);
        if (block == null) {
            return null;
        }
        return block.zone();
    }

    @Override
    public Set<String> excludeZones() {
        return excludeZones;
    }

    @Override
    public void registerListener(Consumer<Set<String>> listener) {
        listeners.add(listener);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> map) throws ConfigException {
        config(map, true);
    }

    @Override
    public void reconfigure(Map<String, ?> map) {
        config(map, false);
    }

    @Override
    public void configure(Map<String, ?> map) {
        config(map, false);
    }

    private void config(Map<String, ?> map, boolean validate) {
        // Kafka supplies the full effective configuration, including defaults after deletion.
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, map, false);
        String zoneCidrBlocksConfig = config.getString(ZONE_CIDR_BLOCKS_CONFIG_KEY);
        // Validation and apply share the same parser, which accepts every value previous releases accepted, so the
        // replay of an already persisted value never fails.
        CIDRMatcher matcher = new CIDRMatcher(zoneCidrBlocksConfig == null ? "" : zoneCidrBlocksConfig);
        Set<String> zones = Set.copyOf(config.getList(EXCLUDE_ZONES_CONFIG_KEY));
        if (validate) {
            return;
        }
        cidrMatcher = matcher;
        LOGGER.info("apply new zone CIDR blocks {}", zoneCidrBlocksConfig);
        logWarnings(matcher);
        if (!zones.equals(excludeZones)) {
            excludeZones = zones;
            LOGGER.info("apply new ZeroZone excluded zones {}", zones);
            listeners.forEach(listener -> listener.accept(zones));
        }
    }

    private static void logWarnings(CIDRMatcher matcher) {
        matcher.warnings().forEach(warning -> LOGGER.warn(warning));
    }

    /**
     * Matches a client address against the configured zone CIDR blocks.
     *
     * <p>IPv4 and IPv6 blocks are both supported, and an address only ever matches blocks of its own family. Blocks
     * are ranked by their declared prefix length, the longest first, and blocks declaring the same prefix length are
     * tried in the order they are configured. IPv4 blocks are parsed and matched exactly as in previous releases,
     * including the non-standard forms those releases accepted.
     */
    public static class CIDRMatcher {
        private final CIDRBlock[] blocks;
        private final List<String> warnings = new ArrayList<>();

        /**
         * Parses an {@code automq.zone.cidr.blocks} value. A zone segment that does not split into exactly one zone
         * and one block list is skipped, as in previous releases, and reported in {@link #warnings()}, and so is an
         * IPv4 block that previous releases accepted although it is not a standard IPv4 CIDR block.
         *
         * @throws ConfigException if a block is an IPv4 block that previous releases rejected as well, or an invalid
         *                         IPv6 block
         */
        public CIDRMatcher(String config) {
            List<CIDRBlock> parsed = new ArrayList<>();
            for (String cidrBlocksOfZone : config.split("<>")) {
                String[] parts = cidrBlocksOfZone.split("@");
                if (parts.length != 2) {
                    if (!cidrBlocksOfZone.isBlank()) {
                        warnings.add("Ignoring zone segment " + cidrBlocksOfZone + " (expected the form "
                            + "<zone>@<block>,<block>): previous releases skipped it as well, so the rest of the value "
                            + "still applies, but none of its blocks ever matches a client");
                    }
                    continue;
                }
                String zone = parts[0];
                String[] cidrList = parts[1].split(",");
                for (String cidr : cidrList) {
                    parsed.add(parseCidr(cidr, zone));
                }
            }
            // The sort is stable, so blocks declaring the same prefix length keep their configured order.
            parsed.sort(Comparator.comparingInt((CIDRBlock block) -> block.prefixLength).reversed());
            blocks = parsed.toArray(new CIDRBlock[0]);
        }

        /**
         * Returns {@code true} when no block was configured, so no client can ever match.
         */
        public boolean isEmpty() {
            return blocks.length == 0;
        }

        /**
         * Returns the messages describing the parts of the value that are only kept for compatibility. They are meant
         * to be logged when a value is applied, not when it is validated.
         */
        public List<String> warnings() {
            return Collections.unmodifiableList(warnings);
        }

        /**
         * Finds the most specific block containing the given address literal. A string without {@code :} is read as
         * an IPv4 address the way previous releases read it, any other string as an IPv6 literal.
         *
         * @return the matching block, or {@code null} if nothing matches, the string is not an address literal, or
         *         the lookup fails unexpectedly. Host names are never resolved.
         */
        public CIDRBlock find(String ip) {
            if (ip == null) {
                return null;
            }
            try {
                if (ip.indexOf(':') < 0) {
                    long ipLong;
                    try {
                        ipLong = ipToLong(ip);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                    for (CIDRBlock block : blocks) {
                        if (block.contains(ipLong)) {
                            return block;
                        }
                    }
                    return null;
                }
                byte[] address;
                try {
                    // Literal parsing only, unlike InetAddress.getByName it never resolves host names.
                    address = InetAddresses.forString(stripScope(ip)).getAddress();
                } catch (IllegalArgumentException e) {
                    return null;
                }
                return findByAddress(address);
            } catch (RuntimeException e) {
                FIND_FAILURE_LOGGER.error("Failed to resolve the zone of client address {}, the client gets no zone", ip, e);
                return null;
            }
        }

        /**
         * Finds the most specific block containing the given address.
         *
         * @return the matching block, or {@code null} if nothing matches, the address is {@code null}, or the lookup
         *         fails unexpectedly, in which case the failure is logged instead of being thrown to the request
         */
        public CIDRBlock find(InetAddress address) {
            if (address == null) {
                return null;
            }
            try {
                return findByAddress(address.getAddress());
            } catch (RuntimeException e) {
                FIND_FAILURE_LOGGER.error("Failed to resolve the zone of client address {}, the client gets no zone", address, e);
                return null;
            }
        }

        private CIDRBlock findByAddress(byte[] address) {
            if (address.length == 4) {
                // Converted once, so each IPv4 block is checked with the legacy numeric match.
                long ip = CIDRBlock.ipv4ToLong(address);
                for (CIDRBlock block : blocks) {
                    if (block.contains(ip)) {
                        return block;
                    }
                }
                return null;
            }
            for (CIDRBlock block : blocks) {
                if (block.contains(address)) {
                    return block;
                }
            }
            return null;
        }

        private CIDRBlock parseCidr(String cidr, String zone) {
            int slash = cidr.indexOf('/');
            String ip = slash < 0 ? cidr : cidr.substring(0, slash);
            if (ip.indexOf(':') >= 0) {
                // Previous releases rejected every address holding a ':', so IPv6 blocks need no compatibility.
                try {
                    return parseIPv6Cidr(cidr, zone);
                } catch (IllegalArgumentException e) {
                    throw new ConfigException(ZONE_CIDR_BLOCKS_CONFIG_KEY, cidr, e.getMessage());
                }
            }
            CIDRBlock block;
            try {
                block = parseIPv4Cidr(cidr, zone);
            } catch (RuntimeException e) {
                throw new ConfigException(ZONE_CIDR_BLOCKS_CONFIG_KEY, cidr,
                    "expected an IPv4 or IPv6 <address>/<prefix length> block");
            }
            if (!isStandardIPv4Cidr(cidr)) {
                warnings.add("Zone CIDR block " + zone + "@" + cidr + " is not a standard IPv4 CIDR block; it is kept"
                    + " for compatibility and matches as " + block.effectiveIPv4Cidr() + ", ranked by its declared"
                    + " prefix length " + block.prefixLength);
            }
            return block;
        }

        /**
         * Parses an IPv4 block exactly as previous releases did. Values they accepted may already be persisted in
         * the metadata log, so this must neither reject nor reinterpret any of them, non-standard forms included.
         */
        private static CIDRBlock parseIPv4Cidr(String cidr, String zone) {
            String[] parts = cidr.split("/");
            String ip = parts[0];
            int maskLength = Integer.parseInt(parts[1]);
            long ipLong = ipToLong(ip);
            long mask = (0xFFFFFFFFL << (32 - maskLength)) & 0xFFFFFFFFL;
            long networkAddress = ipLong & mask;
            return new CIDRBlock(cidr, networkAddress, mask, maskLength, zone);
        }

        private static long ipToLong(String ipAddress) {
            String[] octets = ipAddress.split("\\.");
            long result = 0;
            for (String octet : octets) {
                result = (result << 8) | Integer.parseUnsignedInt(octet);
            }
            return result;
        }

        /**
         * Tells whether an IPv4 block is written in the standard form, four plain decimal octets up to 255 and a plain
         * decimal prefix length up to 32, without sign or leading zero.
         */
        private static boolean isStandardIPv4Cidr(String cidr) {
            String[] parts = cidr.split("/", -1);
            if (parts.length != 2 || !isPlainDecimal(parts[1], 32)) {
                return false;
            }
            String[] octets = parts[0].split("\\.", -1);
            if (octets.length != 4) {
                return false;
            }
            for (String octet : octets) {
                if (!isPlainDecimal(octet, 255)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isPlainDecimal(String value, int max) {
            if (value.isEmpty() || value.length() > 3 || (value.length() > 1 && value.charAt(0) == '0')) {
                return false;
            }
            int number = 0;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c < '0' || c > '9') {
                    return false;
                }
                number = number * 10 + (c - '0');
            }
            return number <= max;
        }

        /**
         * Parses an IPv6 block.
         *
         * @throws IllegalArgumentException if the block is not an IPv6 {@code <address>/<prefix length>} block
         */
        private static CIDRBlock parseIPv6Cidr(String cidr, String zone) {
            String[] parts = cidr.split("/", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("expected an <address>/<prefix length> block");
            }
            byte[] address;
            try {
                // Literal parsing only, unlike InetAddress.getByName it never resolves host names.
                address = InetAddresses.forString(stripScope(parts[0])).getAddress();
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("the address is not an IPv4 or IPv6 literal");
            }
            if (address.length == 4) {
                throw new IllegalArgumentException("an IPv4-mapped literal would read its prefix length as an IPv4 "
                    + "one, configure the block in the plain IPv4 form such as 10.0.0.0/24");
            }
            int prefixLength;
            try {
                prefixLength = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("the prefix length is not a number");
            }
            if (prefixLength < 0 || prefixLength > 128) {
                throw new IllegalArgumentException("the prefix length must be between 0 and 128");
            }
            return new CIDRBlock(cidr, address, prefixLength, zone);
        }

        /**
         * Drops the zone index of an IPv6 literal, which is meaningless for a CIDR block and which recent Guava
         * versions reject unless it names an interface of the local host.
         */
        private static String stripScope(String ip) {
            int scope = ip.indexOf('%');
            return scope < 0 ? ip : ip.substring(0, scope);
        }
    }

    /**
     * An immutable IPv4 or IPv6 CIDR block of a zone. The network address and mask are precomputed so that matching
     * needs no allocation, and an address only ever matches a block of its own family.
     */
    public static class CIDRBlock {
        private final String cidr;
        private final long networkAddress;
        private final long mask;
        // The network address and mask of an IPv6 block, null for an IPv4 block.
        private final byte[] ipv6NetworkAddress;
        private final byte[] ipv6Mask;
        final int prefixLength;
        private final String zone;

        public CIDRBlock(String cidr, long networkAddress, long mask, int prefixLength, String zone) {
            this.cidr = cidr;
            this.networkAddress = networkAddress;
            this.mask = mask;
            this.ipv6NetworkAddress = null;
            this.ipv6Mask = null;
            this.prefixLength = prefixLength;
            this.zone = zone;
        }

        /**
         * Creates an IPv6 block.
         *
         * @param cidr the block as it was configured, kept for reporting
         * @param address the 16 raw bytes of an address of the block
         * @param prefixLength the number of leading bits an address must share with {@code address}
         * @param zone the availability zone the block belongs to
         * @throws IllegalArgumentException if the address is not 16 bytes long or the prefix length is not between
         *                                  0 and 128
         */
        public CIDRBlock(String cidr, byte[] address, int prefixLength, String zone) {
            if (address.length != 16) {
                throw new IllegalArgumentException("an IPv6 block needs a 16 byte address, got " + address.length);
            }
            if (prefixLength < 0 || prefixLength > 128) {
                throw new IllegalArgumentException("prefix length " + prefixLength + " does not fit a 128 bit address");
            }
            this.cidr = cidr;
            this.networkAddress = 0;
            this.mask = 0;
            this.ipv6Mask = prefixMask(address.length, prefixLength);
            this.ipv6NetworkAddress = new byte[address.length];
            for (int i = 0; i < address.length; i++) {
                this.ipv6NetworkAddress[i] = (byte) (address[i] & ipv6Mask[i]);
            }
            this.prefixLength = prefixLength;
            this.zone = zone;
        }

        /**
         * Tells whether an IPv4 address, read as a number the way previous releases did, falls into this block.
         * An IPv6 block never matches.
         */
        public boolean contains(long ip) {
            return ipv6NetworkAddress == null && (ip & mask) == networkAddress;
        }

        /**
         * Tells whether the raw bytes of an address fall into this block. A 4 byte address only matches an IPv4
         * block and a 16 byte address only matches an IPv6 block, any other length matches nothing.
         */
        public boolean contains(byte[] address) {
            if (address.length == 4) {
                return contains(ipv4ToLong(address));
            }
            if (address.length != 16 || ipv6NetworkAddress == null) {
                return false;
            }
            for (int i = 0; i < address.length; i++) {
                if ((byte) (address[i] & ipv6Mask[i]) != ipv6NetworkAddress[i]) {
                    return false;
                }
            }
            return true;
        }

        public String cidr() {
            return cidr;
        }

        public String zone() {
            return zone;
        }

        /**
         * Describes the IPv4 addresses an IPv4 block actually matches, as a standard CIDR block.
         */
        String effectiveIPv4Cidr() {
            return ((networkAddress >>> 24) & 0xFF) + "." + ((networkAddress >>> 16) & 0xFF) + "."
                + ((networkAddress >>> 8) & 0xFF) + "." + (networkAddress & 0xFF) + "/" + Long.bitCount(mask);
        }

        /**
         * Reads the 4 raw bytes of an IPv4 address as the unsigned 32 bit number previous releases matched on.
         */
        private static long ipv4ToLong(byte[] address) {
            long ip = 0;
            for (byte octet : address) {
                ip = (ip << 8) | (octet & 0xFFL);
            }
            return ip;
        }

        private static byte[] prefixMask(int length, int prefixLength) {
            byte[] mask = new byte[length];
            for (int i = 0; i < length; i++) {
                int bits = Math.max(0, Math.min(8, prefixLength - i * 8));
                mask[i] = (byte) (0xFF << (8 - bits));
            }
            return mask;
        }
    }
}
