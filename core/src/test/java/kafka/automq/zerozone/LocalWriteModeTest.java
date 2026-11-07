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

package kafka.automq.zerozone;

import kafka.automq.AutoMQConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the external configuration names for ZeroZone local write modes.
 */
@Tag("S3Unit")
public class LocalWriteModeTest {

    /**
     * Given supported values with different cases, parsing returns the canonical enum constants.
     */
    @Test
    public void testParseSupportedModesCaseInsensitively() {
        assertEquals(LocalWriteMode.ROUTER_CHANNEL, LocalWriteMode.fromName("router_channel"));
        assertEquals(LocalWriteMode.ROUTER_CHANNEL, LocalWriteMode.fromName("ROUTER_CHANNEL"));
        assertEquals(LocalWriteMode.DIRECT, LocalWriteMode.fromName("direct"));
    }

    /**
     * Given an unsupported value, parsing rejects it instead of silently selecting a write path.
     */
    @Test
    public void testRejectUnsupportedMode() {
        assertThrows(IllegalArgumentException.class, () -> LocalWriteMode.fromName("passthrough"));
    }

    /**
     * Given no explicit setting, broker configuration preserves the RouterChannel write path.
     */
    @Test
    public void testConfigDefaultsToRouterChannel() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);

        assertEquals(LocalWriteMode.ROUTER_CHANNEL.configName(),
            configDef.defaultValues().get(AutoMQConfig.ZONE_ROUTER_LOCAL_WRITE_MODE_CONFIG));
    }

    /**
     * Given the direct setting in a different case, broker configuration accepts the value.
     */
    @Test
    public void testConfigAcceptsModeCaseInsensitively() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);

        Map<String, Object> parsed = configDef.parse(Map.of(
            AutoMQConfig.ZONE_ROUTER_LOCAL_WRITE_MODE_CONFIG, "DIRECT"
        ));

        assertEquals("DIRECT", parsed.get(AutoMQConfig.ZONE_ROUTER_LOCAL_WRITE_MODE_CONFIG));
    }

    /**
     * Given an unsupported setting, broker configuration fails before startup.
     */
    @Test
    public void testConfigRejectsUnsupportedMode() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);

        assertThrows(ConfigException.class, () -> configDef.parse(Map.of(
            AutoMQConfig.ZONE_ROUTER_LOCAL_WRITE_MODE_CONFIG, "passthrough"
        )));
    }
}
