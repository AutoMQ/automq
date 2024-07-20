/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.goals;

import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;

import java.util.HashMap;
import java.util.Map;

public class NetworkOutUsageDistributionGoal extends AbstractNetworkUsageDistributionGoal {

    @Override
    public String name() {
        return NetworkOutUsageDistributionGoal.class.getSimpleName();
    }

    @Override
    protected byte resource() {
        return Resource.NW_OUT;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(configs, false);
        this.usageDetectThreshold = controllerConfig.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
        this.usageAvgDeviationRatio = Math.max(0.0, Math.min(1.0,
                controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION)));
        this.usageTrivialRatio = controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO);
    }

    @Override
    public void onBalanceFailed(BrokerUpdater.Broker broker) {
        LOGGER.warn("Failed to balance broker {} network outbound load after iterating all partitions", broker.getBrokerId());
    }

    @Override
    public double weight() {
        return 1.0;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        try {
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD)) {
                long detectThreshold = ConfigUtils.getLong(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
                if (detectThreshold < 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD, detectThreshold);
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION)) {
                double avgDeviation = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION);
                if (avgDeviation < 0 || avgDeviation > 1) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION, avgDeviation,
                            "Value must be in between 0 and 1");
                }
            }
        } catch (Exception e) {
            throw new ConfigException("Reconfiguration validation error", e);
        }
    }
}
