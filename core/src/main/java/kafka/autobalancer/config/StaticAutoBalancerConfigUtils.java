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

package kafka.autobalancer.config;

import java.util.Map;
import java.util.Properties;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 */
public class StaticAutoBalancerConfigUtils {

    /**
     * Convert shared Auto Balancer client settings to standard Kafka client settings. The Kafka producer and
     * consumer ConfigDefs are responsible for validation and type conversion.
     *
     * @param configs Auto Balancer settings
     * @return shared Kafka client settings
     */
    public static Properties parseClientConfigs(Map<String, ?> configs) {
        Properties clientConfigs = new Properties();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            if (entry.getKey().startsWith(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_PREFIX)
                    && entry.getValue() != null) {
                clientConfigs.put(entry.getKey().substring(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_PREFIX.length()),
                        entry.getValue());
            }
        }
        return clientConfigs;
    }
}
