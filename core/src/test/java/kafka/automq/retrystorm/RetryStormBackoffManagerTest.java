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

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RetryStormBackoffManagerTest {

    @Test
    public void testReconfigureUpdatesRuntimeConfig() {
        RetryStormBackoffConfig config = new RetryStormBackoffConfig(true, 1000L);
        RetryStormBackoffManager manager = new RetryStormBackoffManager(config);

        manager.reconfigure(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG, false,
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, 250L
        ));

        assertFalse(config.enabled());
        assertEquals(250L, config.maxDelayMs());
    }

    @Test
    public void testValidateRejectsInvalidReconfiguration() {
        RetryStormBackoffManager manager = new RetryStormBackoffManager(new RetryStormBackoffConfig(true, 1000L));

        assertThrows(ConfigException.class, () -> manager.validateReconfiguration(Map.of(
            AutoMQConfig.RETRY_STORM_BACKOFF_MAX_DELAY_MS_CONFIG, -1L
        )));
    }
}
