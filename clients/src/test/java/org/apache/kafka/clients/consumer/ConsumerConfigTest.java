/*
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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ConsumerConfigTest {

    private final Deserializer<byte[]> keyDeserializer = new ByteArrayDeserializer();
    private final Deserializer<String> valueDeserializer = new StringDeserializer();
    private final String keyDeserializerClassName = keyDeserializer.getClass().getName();
    private final String valueDeserializerClassName = valueDeserializer.getClass().getName();
    private final Object keyDeserializerClass = keyDeserializer.getClass();
    private final Object valueDeserializerClass = valueDeserializer.getClass();
    private final Properties properties = new Properties();

    @BeforeEach
    public void setUp() {
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
    }

    @Test
    public void testOverrideClientId() {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        ConsumerConfig config = new ConsumerConfig(properties);
        assertFalse(config.getString(ConsumerConfig.CLIENT_ID_CONFIG).isEmpty());
    }

    @Test
    public void testOverrideEnableAutoCommit() {
        ConsumerConfig config = new ConsumerConfig(properties);
        boolean overrideEnableAutoCommit = config.maybeOverrideEnableAutoCommit();
        assertFalse(overrideEnableAutoCommit);

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config = new ConsumerConfig(properties);
        try {
            config.maybeOverrideEnableAutoCommit();
            fail("Should have thrown an exception");
        } catch (InvalidConfigurationException e) {
            // expected
        }
    }

    @Test
    public void testAppendDeserializerToConfig() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        Map<String, Object> newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, null, null);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, null);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, null, valueDeserializer);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);

        configs.clear();
        newConfigs = ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer);
        assertEquals(newConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG), keyDeserializerClass);
        assertEquals(newConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), valueDeserializerClass);
    }

    @Test
    public void testAppendDeserializerToConfigWithException() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, null);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        assertThrows(ConfigException.class, () -> ConsumerConfig.appendDeserializerToConfig(configs, null, valueDeserializer));

        configs.clear();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, null);
        assertThrows(ConfigException.class, () -> ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, null));
    }

    @Test
    public void ensureDefaultThrowOnUnsupportedStableFlagToFalse() {
        assertFalse(new ConsumerConfig(properties).getBoolean(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED));
    }

    @Test
    public void testDefaultPartitionAssignor() {
        assertEquals(Arrays.asList(RangeAssignor.class, CooperativeStickyAssignor.class),
            new ConsumerConfig(properties).getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
    }

    @Test
    public void testInvalidGroupInstanceId() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "");
        ConfigException ce = assertThrows(ConfigException.class, () -> new ConsumerConfig(configs));
        assertTrue(ce.getMessage().contains(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    public void testInvalidSecurityProtocol() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc");
        ConfigException ce = assertThrows(ConfigException.class, () -> new ConsumerConfig(configs));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testCaseInsensitiveSecurityProtocol() {
        final String saslSslLowerCase = SecurityProtocol.SASL_SSL.name.toLowerCase(Locale.ROOT);
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, saslSslLowerCase);
        final ConsumerConfig consumerConfig = new ConsumerConfig(configs);
        assertEquals(saslSslLowerCase, consumerConfig.originals().get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }
}
