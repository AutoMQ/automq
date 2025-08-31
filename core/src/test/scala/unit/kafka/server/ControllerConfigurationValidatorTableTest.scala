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

package unit.kafka.server

import kafka.automq.AutoMQConfig
import kafka.server.{ControllerConfigurationValidator, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import org.apache.kafka.common.config.TopicConfig.{AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, AUTOMQ_TABLE_TOPIC_TRANSFORM_TYPES_CONFIG, TABLE_TOPIC_SCHEMA_TYPE_CONFIG}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.server.record.{TableTopicConvertType, TableTopicSchemaType, TableTopicTransformType}
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertThrows}
import org.junit.jupiter.api.{BeforeEach, Tag, Test, Timeout}

import java.util
import java.util.Locale

@Timeout(60)
@Tag("S3Unit")
class ControllerConfigurationValidatorTableTest {

    private var validator: ControllerConfigurationValidator = _
    private var validatorWithSchemaRegistry: ControllerConfigurationValidator = _

    @BeforeEach
    def setUp(): Unit = {
        val config = new KafkaConfig(TestUtils.createDummyBrokerConfig())
        validator = new ControllerConfigurationValidator(config)

        val brokerConfigWithSchemaRegistry = TestUtils.createDummyBrokerConfig()
        brokerConfigWithSchemaRegistry.put(AutoMQConfig.TABLE_TOPIC_SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
        val kafkaConfigWithSchemaRegistry = new KafkaConfig(brokerConfigWithSchemaRegistry)
        validatorWithSchemaRegistry = new ControllerConfigurationValidator(kafkaConfigWithSchemaRegistry)
    }

    @Test
    def testMixedOldAndNewConfigs(): Unit = {
        val config = new util.TreeMap[String, String]()
        config.put(TABLE_TOPIC_SCHEMA_TYPE_CONFIG, TableTopicSchemaType.SCHEMA.name)
        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.RAW.name)

        val exception = assertThrows(classOf[InvalidConfigurationException], () => {
            validator.validate(new ConfigResource(TOPIC, "foo"), config)
        })
        assertEquals("Cannot set both old '" + TABLE_TOPIC_SCHEMA_TYPE_CONFIG +
            "' and new '" + AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG + "' or '" +
            AUTOMQ_TABLE_TOPIC_TRANSFORM_TYPES_CONFIG + "' configurations.", exception.getMessage)
    }

    @Test
    def testConvertTypeWithSchemaRegistryUrlNotConfigured(): Unit = {
        val config = new util.TreeMap[String, String]()
        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.BY_SCHEMA_ID.name)

        var exception = assertThrows(classOf[InvalidConfigurationException], () => {
            validator.validate(new ConfigResource(TOPIC, "foo"), config)
        })
        assertEquals("Table topic convert type is set to 'by_schema_id' but schema registry URL is not configured", exception.getMessage)

        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.BY_SUBJECT_NAME.name)
        exception = assertThrows(classOf[InvalidConfigurationException], () => {
            validator.validate(new ConfigResource(TOPIC, "foo"), config)
        })
        assertEquals("Table topic convert type is set to 'by_subject_name' but schema registry URL is not configured", exception.getMessage)
    }

    @Test
    def testConvertTypeWithSchemaRegistryUrlConfigured(): Unit = {
        val config = new util.TreeMap[String, String]()
        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.BY_SCHEMA_ID.name)
        assertDoesNotThrow(new org.junit.jupiter.api.function.Executable { def execute(): Unit = validatorWithSchemaRegistry.validate(new ConfigResource(TOPIC, "foo"), config) })

        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.BY_SUBJECT_NAME.name)
        assertDoesNotThrow(new org.junit.jupiter.api.function.Executable { def execute(): Unit = validatorWithSchemaRegistry.validate(new ConfigResource(TOPIC, "foo"), config) })
    }

    @Test
    def testRawConvertTypeWithDebeziumUnwrapTransform(): Unit = {
        val config = new util.TreeMap[String, String]()
        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.RAW.name)
        config.put(AUTOMQ_TABLE_TOPIC_TRANSFORM_TYPES_CONFIG, TableTopicTransformType.DEBEZIUM_UNWRAP.name().toLowerCase(Locale.ROOT))

        val exception = assertThrows(classOf[InvalidConfigurationException], () => {
            validator.validate(new ConfigResource(TOPIC, "foo"), config)
        })
        assertEquals("'raw' convert type cannot be used with 'debezium_unwrap' transform type", exception.getMessage)
    }

    @Test
    def testValidRawConvertType(): Unit = {
        val config = new util.TreeMap[String, String]()
        config.put(AUTOMQ_TABLE_TOPIC_CONVERT_TYPE_CONFIG, TableTopicConvertType.RAW.name)

        assertDoesNotThrow(new org.junit.jupiter.api.function.Executable { def execute(): Unit = validator.validate(new ConfigResource(TOPIC, "foo"), config) })
    }
}
