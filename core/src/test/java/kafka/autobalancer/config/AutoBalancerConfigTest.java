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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Tag("S3Unit")
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
    public void testClientConfigPassthrough() {
        Map<String, Object> props = new HashMap<>();
        props.put(StaticAutoBalancerConfig.clientAuthConfig(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), "SSL");
        props.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG), "TLSv1.2,TLSv1.3");
        props.put(StaticAutoBalancerConfig.clientAuthConfig(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG), "trust-store-password");
        props.put(StaticAutoBalancerConfig.clientAuthConfig(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), StringDeserializer.class.getName());
        props.put(StaticAutoBalancerConfig.clientAuthConfig(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), StringDeserializer.class.getName());
        props.put(StaticAutoBalancerConfig.clientAuthConfig("future.kafka.config"), "future-value");
        props.put(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG, "INTERNAL_SSL");

        Properties clientConfigs = StaticAutoBalancerConfigUtils.parseClientConfigs(props);

        Assertions.assertEquals("SSL", clientConfigs.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        Assertions.assertEquals("TLSv1.2,TLSv1.3", clientConfigs.getProperty(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
        Assertions.assertEquals("trust-store-password", clientConfigs.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        Assertions.assertEquals("future-value", clientConfigs.getProperty("future.kafka.config"));
        Assertions.assertFalse(clientConfigs.containsKey(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG));

        ConsumerConfig consumerConfig = new ConsumerConfig(clientConfigs);
        Assertions.assertEquals(new Password("trust-store-password"),
                consumerConfig.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    }
}
