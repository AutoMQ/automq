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
