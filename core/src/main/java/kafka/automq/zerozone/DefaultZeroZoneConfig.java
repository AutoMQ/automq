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

import com.google.common.net.InetAddresses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class DefaultZeroZoneConfig implements ZeroZoneConfig, Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultZeroZoneConfig.class);
    private static final String ZONE_CIDR_BLOCKS_CONFIG_KEY = "automq.zone.cidr.blocks";
    private static final String ZONE_CIDR_BLOCKS_CONFIG_DOC = "The mapping of zone to IPv4 or IPv6 CIDR blocks. Format: zone1@cidr1,cidr2<>zone2@cidr3,cidr4";
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
        // Every apply, including the replay of already persisted configs, runs validation first and the
        // whole batch is dropped when it fails. Validation and apply therefore share the same parsing,
        // which only rejects what previous releases rejected as well.
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
     * <p>IPv4 and IPv6 blocks are both supported and are matched on the raw address bytes, so an IPv4 address
     * never matches an IPv6 block and vice versa. When several blocks match, the longest prefix wins.
     */
    public static class CIDRMatcher {
        private final Map<Integer, List<CIDRBlock>> maskLength2blocks = new HashMap<>();
        private final List<Integer> reverseMaskLengthList = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();

        /**
         * Parses an {@code automq.zone.cidr.blocks} value. A zone segment that does not split into exactly one
         * zone and one block list is skipped without notice, as in previous releases.
         *
         * @throws ConfigException if a block is malformed in a way that previous releases rejected as well.
         *                         Blocks that previous releases accepted but that do not mean what they look
         *                         like are handled for compatibility and reported by {@link #warnings()}.
         */
        public CIDRMatcher(String config) {
            for (String cidrBlocksOfZone : config.split("<>")) {
                String[] parts = cidrBlocksOfZone.split("@");
                if (parts.length != 2) {
                    continue;
                }
                String zone = parts[0];
                String[] cidrList = parts[1].split(",");
                for (String cidr : cidrList) {
                    CIDRBlock block = parseCidr(cidr, zone);
                    if (block == null) {
                        continue;
                    }
                    maskLength2blocks
                        .computeIfAbsent(block.prefixLength, k -> new ArrayList<>())
                        .add(block);
                }
            }
            reverseMaskLengthList.addAll(maskLength2blocks.keySet());
            reverseMaskLengthList.sort(Comparator.reverseOrder());
        }

        /**
         * Returns {@code true} when no block was configured, so no client can ever match.
         */
        public boolean isEmpty() {
            return maskLength2blocks.isEmpty();
        }

        /**
         * Returns the messages describing the blocks that needed a compatibility fallback. They are meant to be
         * logged when a value is applied, not when it is validated.
         */
        public List<String> warnings() {
            return Collections.unmodifiableList(warnings);
        }

        /**
         * Finds the most specific block containing the given address literal.
         *
         * @return the matching block, or {@code null} if nothing matches or the string is not an address literal.
         *         Host names are never resolved.
         */
        public CIDRBlock find(String ip) {
            byte[] address = ip == null ? null : parseAddress(ip);
            return address == null ? null : findByAddress(address);
        }

        /**
         * Finds the most specific block containing the given address, or {@code null} if nothing matches.
         *
         * @param address the client address, which must not be {@code null}
         */
        public CIDRBlock find(InetAddress address) {
            return findByAddress(address.getAddress());
        }

        private CIDRBlock findByAddress(byte[] address) {
            for (int prefix : reverseMaskLengthList) {
                for (CIDRBlock block : maskLength2blocks.get(prefix)) {
                    if (block.contains(address)) {
                        return block;
                    }
                }
            }
            return null;
        }

        /**
         * Parses one configured block.
         *
         * @return the block, or {@code null} when previous releases built one that cannot match a client.
         * @throws ConfigException if previous releases rejected the block as well.
         */
        private CIDRBlock parseCidr(String cidr, String zone) {
            String[] parts = cidr.split("/");
            boolean legacy = legacyAccepted(parts);
            // Previous releases read the address and the first prefix length and ignored any further component.
            // They only ever parsed IPv4 addresses, so the block they built is reproducible here.
            boolean extraComponents = legacy && parts.length > 2;
            try {
                if (parts.length != 2 && !extraComponents) {
                    throw new IllegalArgumentException("expected an <address>/<prefix length> block");
                }
                CIDRBlock block = newBlock(cidr, zone, parts[0], parts[1]);
                if (extraComponents) {
                    warnings.add("Zone CIDR block " + zone + "@" + cidr + " has extra '/' components, only "
                        + parts[0] + "/" + parts[1] + " is used");
                }
                return block;
            } catch (IllegalArgumentException e) {
                if (!legacy) {
                    throw new ConfigException(ZONE_CIDR_BLOCKS_CONFIG_KEY, cidr, e.getMessage());
                }
                warnings.add("Ignoring zone CIDR block " + zone + "@" + cidr + " (" + e.getMessage()
                    + "): previous releases accepted it, so the rest of the value still applies, but this block"
                    + " never matches a client");
                return null;
            }
        }

        /**
         * Builds a block from the address and prefix length of a configured block.
         *
         * @throws IllegalArgumentException if they are not an IPv4 or IPv6 CIDR block
         */
        private static CIDRBlock newBlock(String cidr, String zone, String ip, String prefix) {
            byte[] address = parseAddress(ip);
            if (address == null) {
                throw new IllegalArgumentException("the address is not an IPv4 or IPv6 literal");
            }
            if (address.length == 4 && ip.indexOf(':') >= 0) {
                throw new IllegalArgumentException("an IPv4-mapped literal would read its prefix length as an IPv4 "
                    + "one, configure the block in the plain IPv4 form such as 10.0.0.0/24");
            }
            int maxPrefixLength = address.length * 8;
            int prefixLength;
            try {
                prefixLength = Integer.parseInt(prefix);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("the prefix length is not a number");
            }
            if (prefixLength < 0 || prefixLength > maxPrefixLength) {
                throw new IllegalArgumentException("the prefix length must be between 0 and " + maxPrefixLength);
            }
            return new CIDRBlock(cidr, address, prefixLength, zone);
        }

        /**
         * Tells whether the IPv4-only parser of previous releases would have parsed the block without failing.
         * Such a block may already be persisted in the metadata log, so rejecting it would break the replay of
         * the whole dynamic config batch it belongs to.
         */
        private static boolean legacyAccepted(String[] parts) {
            if (parts.length < 2) {
                // The previous parser read the prefix length without checking that it is present.
                return false;
            }
            try {
                Integer.parseInt(parts[1]);
                for (String octet : parts[0].split("\\.")) {
                    Integer.parseUnsignedInt(octet);
                }
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        private static byte[] parseAddress(String ip) {
            byte[] address = parseDottedQuad(ip);
            if (address != null) {
                return address;
            }
            try {
                // Literal parsing only, unlike InetAddress.getByName it never resolves host names.
                return InetAddresses.forString(stripScope(ip)).getAddress();
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        /**
         * Drops the zone index of an IPv6 literal, which is meaningless for a CIDR block and which recent Guava
         * versions reject unless it names an interface of the local host.
         */
        private static String stripScope(String ip) {
            int scope = ip.indexOf('%');
            if (scope < 0 || ip.indexOf(':') < 0) {
                return ip;
            }
            return ip.substring(0, scope);
        }

        /**
         * Parses a dotted quad, also accepting the zero-padded octets that previous releases read as decimal
         * but that {@link InetAddresses#forString} rejects as ambiguous.
         */
        private static byte[] parseDottedQuad(String ip) {
            String[] octets = ip.split("\\.");
            if (octets.length != 4) {
                return null;
            }
            byte[] address = new byte[4];
            for (int i = 0; i < 4; i++) {
                String octet = octets[i];
                if (octet.isEmpty()) {
                    return null;
                }
                int value = 0;
                for (int j = 0; j < octet.length(); j++) {
                    char c = octet.charAt(j);
                    if (c < '0' || c > '9') {
                        return null;
                    }
                    value = value * 10 + (c - '0');
                    if (value > 255) {
                        return null;
                    }
                }
                address[i] = (byte) value;
            }
            return address;
        }
    }

    /**
     * An immutable CIDR block of a zone, held as the raw bytes of its network address and prefix mask so that
     * matching needs no allocation and stays family aware: only an address of the same family can match.
     */
    public static class CIDRBlock {
        private final String cidr;
        private final byte[] networkAddress;
        private final byte[] mask;
        final int prefixLength;
        private final String zone;

        /**
         * @param cidr the block as it was configured, kept for reporting
         * @param address the raw address bytes of the block, 4 bytes for IPv4 and 16 bytes for IPv6
         * @param prefixLength the number of leading bits an address must share with {@code address}
         * @param zone the availability zone the block belongs to
         * @throws IllegalArgumentException if the prefix length does not fit the address family
         */
        public CIDRBlock(String cidr, byte[] address, int prefixLength, String zone) {
            if (prefixLength < 0 || prefixLength > address.length * 8) {
                throw new IllegalArgumentException("prefix length " + prefixLength + " does not fit a "
                    + address.length * 8 + " bit address");
            }
            this.cidr = cidr;
            this.mask = prefixMask(address.length, prefixLength);
            this.networkAddress = new byte[address.length];
            for (int i = 0; i < address.length; i++) {
                this.networkAddress[i] = (byte) (address[i] & mask[i]);
            }
            this.prefixLength = prefixLength;
            this.zone = zone;
        }

        /**
         * Tells whether the raw bytes of an address fall into this block. Addresses of another family never match.
         */
        public boolean contains(byte[] address) {
            if (address.length != networkAddress.length) {
                return false;
            }
            for (int i = 0; i < address.length; i++) {
                if ((byte) (address[i] & mask[i]) != networkAddress[i]) {
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
