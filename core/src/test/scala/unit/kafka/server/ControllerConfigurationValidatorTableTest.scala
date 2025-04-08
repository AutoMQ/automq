/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
