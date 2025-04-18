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
    public Set<String> reconfigurableConfigs() {
        // NOT USED
        return Collections.emptySet();
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
            if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO)) {
                double trivialChangeRatio = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO);
                if (trivialChangeRatio < 0 || trivialChangeRatio > 1) {
                    throw new ConfigException(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO, trivialChangeRatio,
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
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD)) {
            this.usageDetectThreshold = ConfigUtils.getLong(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_USAGE_DISTRIBUTION_DETECT_THRESHOLD);
        }
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION)) {
            this.usageAvgDeviationRatio = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_DISTRIBUTION_DETECT_AVG_DEVIATION);
        }
        if (configs.containsKey(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO)) {
            this.usageTrivialRatio = ConfigUtils.getDouble(objectConfigs, AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_NETWORK_OUT_TRIVIAL_CHANGE_RATIO);
        }
    }
}
