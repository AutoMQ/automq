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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;

import java.util.Properties;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 */
public class StaticAutoBalancerConfigUtils {

    /**
     * Parse AdminClient configs based on the given {@link StaticAutoBalancerConfig configs}.
     *
     * @param clientConfigs Configs that will be return with SSL configs.
     * @param configs            Configs to be used for parsing AdminClient SSL configs.
     */
    public static void addSslConfigs(Properties clientConfigs, StaticAutoBalancerConfig configs) {
        // Add security protocol (if specified).
        try {
            setStringConfigIfExists(configs, clientConfigs, StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL,
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
            setStringConfigIfExists(configs, clientConfigs, StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM,
                    SaslConfigs.SASL_MECHANISM);
            setPasswordConfigIfExists(configs, clientConfigs, StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG,
                    SaslConfigs.SASL_JAAS_CONFIG);
        } catch (ConfigException ce) {
            // let it go.
        }
    }

    private static void setPasswordConfigIfExists(StaticAutoBalancerConfig configs, Properties props, String name, String originalName) {
        try {
            Password pwd = configs.getPassword(name);
            if (pwd != null) {
                props.put(originalName, pwd);
            }
        } catch (ConfigException ce) {
            // let it go.
        }
    }

    private static void setStringConfigIfExists(StaticAutoBalancerConfig configs, Properties props, String name, String originalName) {
        try {
            props.put(originalName, configs.getString(name));
        } catch (ConfigException ce) {
            // let it go.
        }
    }
}
