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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

public class StaticAutoBalancerConfig extends AbstractConfig {
    protected static final ConfigDef CONFIG;
    private static final String PREFIX = "autobalancer.";

    /* Configurations */
    public static final String AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL = PREFIX + "client.auth." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    public static final String AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM = PREFIX + "client.auth." + SaslConfigs.SASL_MECHANISM;
    public static final String AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG = PREFIX + "client.auth." + SaslConfigs.SASL_JAAS_CONFIG;
    /* Default values */
    public static final String DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
    public static final String DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM = SaslConfigs.DEFAULT_SASL_MECHANISM;
    public static final Password DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG = null;
    /* Documents */
    public static final String AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;
    public static final String AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM_DOC = SaslConfigs.SASL_MECHANISM_DOC;
    public static final String AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;


    static {
        CONFIG = new ConfigDef()
                .define(AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL,
                        ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL_DOC)
                .define(AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM,
                        ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM,
                        ConfigDef.Importance.MEDIUM,
                        AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM_DOC)
                .define(AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG,
                        ConfigDef.Importance.MEDIUM,
                        AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG_DOC);
    }

    public StaticAutoBalancerConfig(Map<?, ?> originals, boolean doLogs) {
        super(CONFIG, originals, doLogs);
    }
}
