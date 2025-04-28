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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AutoBalancerConfigTest {

    @Test
    public void testNoPredefinedConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "2");
        props.put("some.other.config", "some-value");
        AutoBalancerControllerConfig config = new AutoBalancerControllerConfig(props, false);
        Assertions.assertEquals(2, config.getInt(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG));
        Assertions.assertThrowsExactly(ConfigException.class, () -> config.getString("some.other.config"));
    }

    @Test
    public void testSSLConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SECURITY_PROTOCOL, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SASL_MECHANISM, "PLAIN");
        Password pwd = new Password("jaas-config");
        props.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_AUTH_SASL_JAAS_CONFIG, pwd);
        props.put(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_CONFIG, "20");
        AutoBalancerMetricsReporterConfig reporterConfig = new AutoBalancerMetricsReporterConfig(props, false);
        Assertions.assertEquals(20L, reporterConfig.getLong(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_CONFIG));

        StaticAutoBalancerConfig config = new StaticAutoBalancerConfig(reporterConfig.originals(), false);
        Properties clientConfig = new Properties();
        StaticAutoBalancerConfigUtils.addSslConfigs(clientConfig, config);
        Assertions.assertEquals(SecurityProtocol.SASL_PLAINTEXT.name, clientConfig.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
        Assertions.assertEquals("PLAIN", clientConfig.getProperty(SaslConfigs.SASL_MECHANISM));
        Assertions.assertEquals(pwd, clientConfig.get(SaslConfigs.SASL_JAAS_CONFIG));
    }
}
