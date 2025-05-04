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
    public static final String AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG = PREFIX + "client.listener.name";
    /* Default values */
    public static final String DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL;
    public static final String DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM = SaslConfigs.DEFAULT_SASL_MECHANISM;
    public static final Password DEFAULT_AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG = null;
    public static final String DEFAULT_AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG = "";
    /* Documents */
    public static final String AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;
    public static final String AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM_DOC = SaslConfigs.SASL_MECHANISM_DOC;
    public static final String AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;
    public static final String AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG_DOC = "The listener name used for the client to connect to the broker.";


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
                        AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG_DOC)
                .define(AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG,
                        ConfigDef.Importance.MEDIUM,
                        AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG_DOC);
    }

    public StaticAutoBalancerConfig(Map<?, ?> originals, boolean doLogs) {
        super(CONFIG, originals, doLogs);
    }
}
