/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.retrystorm;

import kafka.automq.AutoMQConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers retry storm static and dynamic configuration parsing semantics.
 */
@Tag("S3Unit")
public class RetryStormBackoffConfigTest {

    /**
     * Given AutoMQ broker config definitions, retry storm is disabled by default with 1000ms max delay.
     */
    @Test
    public void testConfigDefDefaults() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);

        Map<String, Object> defaults = configDef.defaultValues();
        assertEquals(false, defaults.get(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG));
        assertEquals(1000L, defaults.get(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG));
    }

    /**
     * Given a dynamic config update, validation rejects a negative max delay before runtime update.
     */
    @Test
    public void testValidateRejectsNegativeMaxDelayMs() {
        assertThrows(ConfigException.class, () -> RetryStormBackoffConfig.validate(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, -1L
        )));
    }

    /**
     * Given static broker config parsing, ConfigDef rejects negative max delay values.
     */
    @Test
    public void testConfigDefRejectsNegativeMaxDelayMs() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);

        assertThrows(ConfigException.class, () -> configDef.parse(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, -1L
        )));
    }

    /**
     * Given retry storm delay has a bounded retention contract, static config rejects values above 10 seconds.
     */
    @Test
    public void testConfigDefRejectsMaxDelayAboveUpperBound() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);

        assertThrows(ConfigException.class, () -> configDef.parse(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, 10001L
        )));
    }

    /**
     * Given a dynamic config update, validation rejects values above the retry storm delay upper bound.
     */
    @Test
    public void testValidateRejectsMaxDelayAboveUpperBound() {
        assertThrows(ConfigException.class, () -> RetryStormBackoffConfig.validate(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, 10001L
        )));
    }

    /**
     * Given explicit runtime construction, config snapshots reject values above the delay upper bound.
     */
    @Test
    public void testConstructorRejectsMaxDelayAboveUpperBound() {
        assertThrows(ConfigException.class, () -> new RetryStormBackoffConfig(true, 10001L));
    }

    /**
     * Given partial dynamic updates, only the supplied retry storm key changes at runtime.
     */
    @Test
    public void testPartialDynamicUpdate() {
        RetryStormBackoffConfig config = RetryStormBackoffConfig.from(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG, true,
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, 1000L
        ));

        config.update(Map.of(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG, false));
        assertFalse(config.enabled());
        assertEquals(1000L, config.maxDelayMs());

        config.update(Map.of(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, "250"));
        assertFalse(config.enabled());
        assertEquals(250L, config.maxDelayMs());
    }

    /**
     * Given Kafka dynamic config registration, both retry storm keys are exposed as reconfigurable.
     */
    @Test
    public void testReconfigurableConfigKeys() {
        assertTrue(RetryStormBackoffConfig.RECONFIGURABLE_CONFIGS.contains(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG));
        assertTrue(RetryStormBackoffConfig.RECONFIGURABLE_CONFIGS.contains(AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG));
        assertEquals(2, RetryStormBackoffConfig.RECONFIGURABLE_CONFIGS.size());
    }
}
