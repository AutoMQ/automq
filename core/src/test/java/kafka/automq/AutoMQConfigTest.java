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

package kafka.automq;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AutoMQConfigTest {

    @Test
    public void testPrometheusAuthenticationDefaultsToNone() {
        ConfigDef configDef = configDef();

        Map<String, Object> parsed = configDef.parse(new HashMap<>());

        Assertions.assertEquals(
            "none",
            parsed.get(AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_TYPE_CONFIG)
        );
        Assertions.assertNull(
            parsed.get(AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_USERNAME_CONFIG)
        );
        Assertions.assertNull(
            parsed.get(AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_PASSWORD_CONFIG)
        );
    }

    @Test
    public void testPrometheusBasicAuthenticationConfiguration() {
        ConfigDef configDef = configDef();

        Map<String, Object> properties = new HashMap<>();
        properties.put(
            AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_TYPE_CONFIG,
            "basic"
        );
        properties.put(
            AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_USERNAME_CONFIG,
            "prometheus"
        );
        properties.put(
            AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_PASSWORD_CONFIG,
            "secret"
        );

        Map<String, Object> parsed = configDef.parse(properties);

        Assertions.assertEquals(
            "basic",
            parsed.get(AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_TYPE_CONFIG)
        );
        Assertions.assertEquals(
            "prometheus",
            parsed.get(AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_USERNAME_CONFIG)
        );

        Password password = (Password) parsed.get(
            AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_PASSWORD_CONFIG
        );

        Assertions.assertNotNull(password);
        Assertions.assertEquals("secret", password.value());
    }

    @Test
    public void testRejectsInvalidPrometheusAuthenticationType() {
        ConfigDef configDef = configDef();

        Map<String, Object> properties = new HashMap<>();
        properties.put(
            AutoMQConfig.S3_TELEMETRY_METRICS_PROMETHEUS_AUTH_TYPE_CONFIG,
            "unsupported"
        );

        Assertions.assertThrows(
            ConfigException.class,
            () -> configDef.parse(properties)
        );
    }

    private static ConfigDef configDef() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);
        return configDef;
    }
}
