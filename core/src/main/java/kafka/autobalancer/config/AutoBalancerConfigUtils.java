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

package kafka.autobalancer.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;

import java.util.Properties;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 */
public class AutoBalancerConfigUtils {

    /**
     * Parse AdminClient configs based on the given {@link AutoBalancerConfig configs}.
     *
     * @param clientConfigs Configs that will be return with SSL configs.
     * @param configs            Configs to be used for parsing AdminClient SSL configs.
     */
    public static void addSslConfigs(Properties clientConfigs, AutoBalancerConfig configs) {
        // Add security protocol (if specified).
        try {
            String securityProtocol = configs.getString(AutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL);
            clientConfigs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            setStringConfigIfExists(configs, clientConfigs, AutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL,
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
            setStringConfigIfExists(configs, clientConfigs, AutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM,
                    SaslConfigs.SASL_MECHANISM);
            setPasswordConfigIfExists(configs, clientConfigs, AutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG,
                    SaslConfigs.SASL_JAAS_CONFIG);
        } catch (ConfigException ce) {
            // let it go.
        }
    }

    private static void setPasswordConfigIfExists(AutoBalancerConfig configs, Properties props, String name, String originalName) {
        try {
            Password pwd = configs.getPassword(name);
            if (pwd != null) {
                props.put(originalName, pwd);
            }
        } catch (ConfigException ce) {
            // let it go.
        }
    }

    private static void setStringConfigIfExists(AutoBalancerConfig configs, Properties props, String name, String originalName) {
        try {
            props.put(originalName, configs.getString(name));
        } catch (ConfigException ce) {
            // let it go.
        }
    }
}
