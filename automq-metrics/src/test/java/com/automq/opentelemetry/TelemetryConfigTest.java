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

package com.automq.opentelemetry;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TelemetryConfigTest {

    @Test
    void getPropertiesWithPrefixStripsPrefixAndIgnoresOthers() {
        Properties properties = new Properties();
        properties.setProperty("automq.telemetry.s3.selector.type", "kafka");
        properties.setProperty("automq.telemetry.s3.selector.kafka.bootstrap.servers", "localhost:9092");
        properties.setProperty("automq.telemetry.s3.selector.kafka.security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("unrelated.key", "value");

        TelemetryConfig config = new TelemetryConfig(properties);
        Map<String, String> result = config.getPropertiesWithPrefix("automq.telemetry.s3.selector.");

        assertEquals("kafka", result.get("type"));
        assertEquals("localhost:9092", result.get("kafka.bootstrap.servers"));
        assertEquals("SASL_PLAINTEXT", result.get("kafka.security.protocol"));
        assertFalse(result.containsKey("unrelated.key"));
    }
}
