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

package kafka.automq;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("S3Unit")
public class ControlledShutdownConfigTest {
    /**
     * Given no override, cross-rack shutdown placement remains disabled.
     */
    @Test
    public void testDisabledByDefault() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);
        assertEquals(false, configDef.parse(Map.of()).get(AutoMQConfig.CONTROLLED_SHUTDOWN_CROSS_RACK_ENABLED_CONFIG));
    }

    /**
     * Given a properties-file override, both boolean values are parsed explicitly.
     */
    @Test
    public void testExplicitValues() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);
        String key = AutoMQConfig.CONTROLLED_SHUTDOWN_CROSS_RACK_ENABLED_CONFIG;
        assertEquals(true, configDef.parse(Map.of(key, "true")).get(key));
        assertEquals(false, configDef.parse(Map.of(key, "false")).get(key));
    }

    /**
     * Given an invalid override, configuration validation rejects it rather than silently enabling placement.
     */
    @Test
    public void testInvalidValue() {
        ConfigDef configDef = new ConfigDef();
        AutoMQConfig.define(configDef);
        assertThrows(ConfigException.class, () ->
            configDef.parse(Map.of(AutoMQConfig.CONTROLLED_SHUTDOWN_CROSS_RACK_ENABLED_CONFIG, "invalid")));
    }
}
