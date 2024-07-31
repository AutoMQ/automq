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

package kafka.autobalancer.goals;

import kafka.automq.AutoMQConfig;

import java.util.Map;

public abstract class AbstractNetworkUsageDistributionGoal extends AbstractResourceUsageDistributionGoal {

    protected long linearNormalizerThreshold = 100 * 1024 * 1024;

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG)) {
            Object nwBandwidth = configs.get(AutoMQConfig.S3_NETWORK_BASELINE_BANDWIDTH_CONFIG);
            try {
                if (nwBandwidth instanceof Long) {
                    this.linearNormalizerThreshold = (Long) nwBandwidth;
                } else if (nwBandwidth instanceof Integer) {
                    this.linearNormalizerThreshold = (Integer) nwBandwidth;
                } else if (nwBandwidth instanceof String) {
                    this.linearNormalizerThreshold = Long.parseLong((String) nwBandwidth);
                } else {
                    LOGGER.error("Failed to parse max normalized load bytes from config {}, using default value", nwBandwidth);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to parse max normalized load bytes from config {}, using default value", nwBandwidth, e);
            }
        }
        LOGGER.info("{} using linearNormalizerThreshold: {}", name(), this.linearNormalizerThreshold);
    }

    @Override
    public double linearNormalizerThreshold() {
        return linearNormalizerThreshold;
    }

    @Override
    public String group() {
        return "NetworkUsageDistribution";
    }
}
