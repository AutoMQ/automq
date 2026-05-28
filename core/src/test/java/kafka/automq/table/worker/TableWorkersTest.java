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

package kafka.automq.table.worker;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import org.apache.kafka.server.config.ZkConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TableWorkersTest {
    @Test
    void tableTopicSchemaRegistryClientConfigsMapsAutoMQPrefix() {
        Properties properties = new Properties();
        properties.setProperty("broker.id", "1");
        properties.setProperty(ZkConfigs.ZK_CONNECT_CONFIG, "somewhere");
        properties.setProperty(AutoMQConfig.TABLE_TOPIC_SCHEMA_REGISTRY_CONFIG_PREFIX + "ssl.truststore.type", "PEM");
        properties.setProperty(AutoMQConfig.TABLE_TOPIC_SCHEMA_REGISTRY_CONFIG_PREFIX + "ssl.truststore.certificates", "ca-pem");
        properties.setProperty(AutoMQConfig.TABLE_TOPIC_SCHEMA_REGISTRY_CONFIG_PREFIX + "ssl.endpoint.identification.algorithm", "");
        properties.setProperty("schema.registry.ssl.truststore.type", "ignored-global");

        KafkaConfig config = KafkaConfig.fromProps(properties);

        Map<String, Object> clientConfigs = TableWorkers.tableTopicSchemaRegistryClientConfigs(config);
        assertEquals("PEM", clientConfigs.get("schema.registry.ssl.truststore.type"));
        assertEquals("ca-pem", clientConfigs.get("schema.registry.ssl.truststore.certificates"));
        assertEquals("", clientConfigs.get("schema.registry.ssl.endpoint.identification.algorithm"));
        assertFalse(clientConfigs.containsKey("ssl.truststore.type"));
        assertFalse(clientConfigs.containsValue("ignored-global"));
    }
}
