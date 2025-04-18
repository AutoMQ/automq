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

import kafka.automq.AutoMQConfig
import kafka.server.{ControllerConfigurationValidator, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import org.apache.kafka.common.config.TopicConfig.TABLE_TOPIC_SCHEMA_TYPE_CONFIG
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.server.record.TableTopicSchemaType
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{Tag, Test, Timeout}

import java.util

@Timeout(60)
@Tag("S3Unit")
class ControllerConfigurationValidatorTableTest {
    val config = new KafkaConfig(TestUtils.createDummyBrokerConfig())
    val validator = new ControllerConfigurationValidator(config)

    @Test
    def testInvalidTableTopicSchemaConfig(): Unit = {
        val config = new util.TreeMap[String, String]()
        config.put(TABLE_TOPIC_SCHEMA_TYPE_CONFIG, TableTopicSchemaType.SCHEMA.name)

        // Test without schema registry URL configured
        val exception = assertThrows(classOf[InvalidConfigurationException], () => {
            validator.validate(new ConfigResource(TOPIC, "foo"), config, config)
        })
        assertEquals("Table topic schema type is set to SCHEMA but schema registry URL is not configured", exception.getMessage)

        // Test with schema registry URL configured
        val brokerConfigWithSchemaRegistry = TestUtils.createDummyBrokerConfig()
        brokerConfigWithSchemaRegistry.put(AutoMQConfig.TABLE_TOPIC_SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

        val kafkaConfigWithSchemaRegistry = new KafkaConfig(brokerConfigWithSchemaRegistry)
        val validatorWithSchemaRegistry = new ControllerConfigurationValidator(kafkaConfigWithSchemaRegistry)

        // No exception should be thrown when schema registry URL is configured properly
        validatorWithSchemaRegistry.validate(new ConfigResource(TOPIC, "foo"), config, config)
    }
}
