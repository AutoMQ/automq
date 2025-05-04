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
