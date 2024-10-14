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

import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.model.BrokerUpdater;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ConfigUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NetworkInUsageDistributionGoal extends AbstractNetworkUsageDistributionGoal {

    @Override
    public String name() {
        return NetworkInUsageDistributionGoal.class.getSimpleName();
    }

    @Override
    protected byte resource() {
        return Resource.NW_IN;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        AutoBalancerControllerConfig controllerConfig = new AutoBalancerControllerConfig(configs, false);
        this.usageDetectThreshold = controllerConfig.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
        this.usageAvgDeviationRatio = Math.max(0.0, Math.min(1.0,
                controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION)));
        this.usageTrivialRatio = controllerConfig.getDouble(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO);
    }

    @Override
    public void onBalanceFailed(BrokerUpdater.Broker broker) {
        LOGGER.warn("Failed to balance broker {} network inbound load after iterating all partitions", broker.getBrokerId());
    }

    @Override
    public double weight() {
        return 1.0;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        // NOT USED
        return Collections.emptySet();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        try {
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD)) {
                long detectThreshold = ConfigUtils.getLong(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
                if (detectThreshold < 0) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD, detectThreshold);
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION)) {
                double avgDeviation = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION);
                if (avgDeviation < 0 || avgDeviation > 1) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION, avgDeviation,
                            "Value must be in between 0 and 1");
                }
            }
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO)) {
                double trivialRatio = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO);
                if (trivialRatio < 0 || trivialRatio > 1) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO, trivialRatio,
                            "Value must be in between 0 and 1");
                }
            }
        } catch (Exception e) {
            throw new ConfigException("Reconfiguration validation error", e);
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD)) {
            this.usageDetectThreshold = ConfigUtils.getLong(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
        }
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION)) {
            this.usageAvgDeviationRatio = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_DISTRIBUTION_DETECT_AVG_DEVIATION);
        }
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO)) {
            this.usageTrivialRatio = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_IN_TRIVIAL_CHANGE_RATIO);
        }
    }
}
