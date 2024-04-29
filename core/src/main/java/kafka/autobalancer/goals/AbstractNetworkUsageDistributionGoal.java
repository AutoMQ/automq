/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.goals;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;

import java.util.Map;

public abstract class AbstractNetworkUsageDistributionGoal extends AbstractResourceUsageDistributionGoal {
    private static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);

    protected long linearNormalizerThreshold = 100 * 1024 * 1024;

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(KafkaConfig.S3NetworkBaselineBandwidthProp())) {
            Object nwBandwidth = configs.get(KafkaConfig.S3NetworkBaselineBandwidthProp());
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
