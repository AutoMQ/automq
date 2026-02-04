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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultClientRackProvider implements ClientRackProvider, Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientRackProvider.class);
    private static final String ZONE_CIDR_BLOCKS_CONFIG_KEY = "automq.zone.cidr.blocks";
    private static final String ZONE_CIDR_BLOCKS_CONFIG_DOC = "The mapping of zone to CIDR blocks. Format: zone1@cidr1,cidr2<>zone2@cidr3,cidr4";
    private static final Set<String> RECONFIGURABLE_CONFIGS;
    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private final KafkaConfig kafkaConfig;
    private CIDRMatcher cidrMatcher = new CIDRMatcher("");

    static {
        RECONFIGURABLE_CONFIGS = Set.of(
            ZONE_CIDR_BLOCKS_CONFIG_KEY
        );
        RECONFIGURABLE_CONFIGS.forEach(DynamicBrokerConfig.AllDynamicConfigs()::add);
        CONFIG_DEF.define(ZONE_CIDR_BLOCKS_CONFIG_KEY, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, ZONE_CIDR_BLOCKS_CONFIG_DOC);
    }

    public DefaultClientRackProvider(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        // Read static config from server.properties on initialization
        final String staticValue = (String) kafkaConfig.originals().get(ZONE_CIDR_BLOCKS_CONFIG_KEY);
        if (staticValue != null) {
            this.cidrMatcher = new CIDRMatcher(staticValue);
            LOGGER.info("Initialized with static zone CIDR blocks: {}", staticValue);
        }
    }

    @Override
    public String rack(ClientIdMetadata clientId) {
        String rack = clientId.rack();
        if (rack != null) {
            return rack;
        }
        CIDRBlock block = cidrMatcher.find(clientId.clientAddress().getHostAddress());
        if (block == null) {
            return null;
        }
        return block.zone();
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Set.of(ZONE_CIDR_BLOCKS_CONFIG_KEY);
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
        String zoneCidrBlocksConfig = (String) map.get(ZONE_CIDR_BLOCKS_CONFIG_KEY);
        if (zoneCidrBlocksConfig != null) {
            CIDRMatcher matcher = new CIDRMatcher(zoneCidrBlocksConfig);
            if (!validate) {
                cidrMatcher = matcher;
                LOGGER.info("apply new zone CIDR blocks {}", zoneCidrBlocksConfig);
            }
        }
    }

    public static class CIDRMatcher {
        private final Map<Integer, List<CIDRBlock>> maskLength2blocks = new HashMap<>();
        private final List<Integer> reverseMaskLengthList = new ArrayList<>();

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
                    maskLength2blocks
                        .computeIfAbsent(block.prefixLength, k -> new ArrayList<>())
                        .add(block);
                }
            }
            reverseMaskLengthList.addAll(maskLength2blocks.keySet());
            reverseMaskLengthList.sort(Comparator.reverseOrder());
        }

        public CIDRBlock find(String ip) {
            long ipLong = ipToLong(ip);
            for (int prefix : reverseMaskLengthList) {
                List<CIDRBlock> blocks = maskLength2blocks.get(prefix);
                if (blocks != null) {
                    for (CIDRBlock block : blocks) {
                        if (block.contains(ipLong)) {
                            return block;
                        }
                    }
                }
            }
            return null;
        }

        private CIDRBlock parseCidr(String cidr, String zone) {
            String[] parts = cidr.split("/");
            String ip = parts[0];
            int maskLength = Integer.parseInt(parts[1]);
            long ipLong = ipToLong(ip);
            long mask = (0xFFFFFFFFL << (32 - maskLength)) & 0xFFFFFFFFL;
            long networkAddress = ipLong & mask;
            return new CIDRBlock(cidr, networkAddress, mask, maskLength, zone);
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

    public static class CIDRBlock {
        private final String cidr;
        private final long networkAddress;
        private final long mask;
        final int prefixLength;
        private final String zone;

        public CIDRBlock(String cidr, long networkAddress, long mask, int prefixLength, String zone) {
            this.cidr = cidr;
            this.networkAddress = networkAddress;
            this.mask = mask;
            this.prefixLength = prefixLength;
            this.zone = zone;
        }

        public boolean contains(long ip) {
            return (ip & mask) == networkAddress;
        }

        public String cidr() {
            return cidr;
        }

        public String zone() {
            return zone;
        }
    }
}
