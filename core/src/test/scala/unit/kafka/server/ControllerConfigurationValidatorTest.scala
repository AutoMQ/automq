/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.server

import kafka.automq.AutoMQConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER, CLIENT_METRICS, TOPIC}
import org.apache.kafka.common.config.TopicConfig.{SEGMENT_BYTES_CONFIG, SEGMENT_JITTER_MS_CONFIG, SEGMENT_MS_CONFIG, TABLE_TOPIC_SCHEMA_TYPE_CONFIG}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidRequestException, InvalidTopicException}
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.apache.kafka.server.record.TableTopicSchemaType
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{Tag, Test}

import java.util
import java.util.Collections.emptyMap

class ControllerConfigurationValidatorTest {
  val config = new KafkaConfig(TestUtils.createDummyBrokerConfig())
  val validator = new ControllerConfigurationValidator(config)

  @Test
  def testDefaultTopicResourceIsRejected(): Unit = {
    assertEquals("Default topic resources are not allowed.",
        assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(TOPIC, ""), emptyMap())). getMessage)
  }

  @Test
  def testInvalidTopicNameRejected(): Unit = {
    assertEquals("Topic name is invalid: '(<-invalid->)' contains " +
      "one or more characters other than ASCII alphanumerics, '.', '_' and '-'",
        assertThrows(classOf[InvalidTopicException], () => validator.validate(
          new ConfigResource(TOPIC, "(<-invalid->)"), emptyMap())). getMessage)
  }

  @Test
  def testUnknownResourceType(): Unit = {
    assertEquals("Unknown resource type BROKER_LOGGER",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER_LOGGER, "foo"), emptyMap())). getMessage)
  }

  @Test
  def testNullTopicConfigValue(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "10")
    config.put(SEGMENT_BYTES_CONFIG, null)
    config.put(SEGMENT_MS_CONFIG, null)
    assertEquals("Null value not supported for topic configs: segment.bytes,segment.ms",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config)). getMessage)
  }

  @Test
  def testValidTopicConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    validator.validate(new ConfigResource(TOPIC, "foo"), config)
  }

  @Test
  def testInvalidTopicConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    config.put(SEGMENT_BYTES_CONFIG, "67108864")
    config.put("foobar", "abc")
    assertEquals("Unknown topic config name: foobar",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(TOPIC, "foo"), config)). getMessage)
  }

  @Test
  def testInvalidBrokerEntity(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Unable to parse broker name as a base 10 number.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "blah"), config)). getMessage)
  }

  @Test
  def testInvalidNegativeBrokerId(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(SEGMENT_JITTER_MS_CONFIG, "1000")
    assertEquals("Invalid negative broker ID.",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(BROKER, "-1"), config)). getMessage)
  }

  @Test
  def testValidClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "2000")
    config.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency")
    config.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538,client_id=1" +
      ",client_software_name=apache-kafka-java,client_software_version=2.8.0-SNAPSHOT,client_source_address=127.0.0.1," +
      "client_source_port=1234")
    validator.validate(new ConfigResource(CLIENT_METRICS, "subscription-1"), config)
  }

  @Test
  def testInvalidSubscriptionNameClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    assertEquals("Subscription name can't be empty",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, ""), config)). getMessage)
  }

  @Test
  def testInvalidIntervalClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "10")
    assertEquals("Invalid value 10 for interval.ms, interval must be between 100 and 3600000 (1 hour)",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config)). getMessage)

    config.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, "3600001")
    assertEquals("Invalid value 3600001 for interval.ms, interval must be between 100 and 3600000 (1 hour)",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config)). getMessage)
  }

  @Test
  def testUndefinedConfigClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put("random", "10")
    assertEquals("Unknown client metrics configuration: random",
      assertThrows(classOf[InvalidRequestException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config)). getMessage)
  }

  @Test
  def testInvalidMatchClientMetricsConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, "10")
    assertEquals("Illegal client matching pattern: 10",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(CLIENT_METRICS, "subscription-1"), config)). getMessage)
  }

  @Test
  def testInvalidTableTopicSchemaConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put(TABLE_TOPIC_SCHEMA_TYPE_CONFIG, TableTopicSchemaType.SCHEMA.name)

    // Test without schema registry URL configured
    val exception = assertThrows(classOf[InvalidRequestException], () => {
      validator.validate(new ConfigResource(TOPIC, "foo"), config)
    })
    assertEquals("Table topic schema type is set to SCHEMA but schema registry URL is not configured", exception.getMessage)

    // Test with schema registry URL configured
    val brokerConfigWithSchemaRegistry = TestUtils.createDummyBrokerConfig()
    brokerConfigWithSchemaRegistry.put(AutoMQConfig.TABLE_TOPIC_SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

    val kafkaConfigWithSchemaRegistry = new KafkaConfig(brokerConfigWithSchemaRegistry)
    val validatorWithSchemaRegistry = new ControllerConfigurationValidator(kafkaConfigWithSchemaRegistry)

    // No exception should be thrown when schema registry URL is configured properly
    validatorWithSchemaRegistry.validate(new ConfigResource(TOPIC, "foo"), config)
  }

  // AutoMQ inject start
  private val ZONE_CIDR_BLOCKS_CONFIG = "automq.zone.cidr.blocks"

  private def zoneCidrBlocks(value: String): util.Map[String, String] = {
    val configs = new util.HashMap[String, String]()
    configs.put(ZONE_CIDR_BLOCKS_CONFIG, value)
    configs
  }

  private def validateAlteredConfigs(
    resource: ConfigResource,
    alteredConfigs: util.Map[String, String],
    existingConfigs: util.Map[String, String]
  ): Executable = () => validator.validateAlteredConfigs(resource, alteredConfigs, existingConfigs)

  /**
   * Given a broker resource, when a request sets a zone CIDR block only earlier releases accepted, then it is rejected.
   */
  @Tag("S3Unit")
  @Test
  def testNewLegacyZoneCidrBlocksRejected(): Unit = {
    val exception = assertThrows(classOf[ConfigException], validateAlteredConfigs(
      new ConfigResource(BROKER, "0"), zoneCidrBlocks("az-a@10.0.0/24"), emptyMap()))
    assertTrue(exception.getMessage.contains(
      "Block az-a@10.0.0/24 is not a supported CIDR block (the address is not an IPv4 or IPv6 literal)"))
  }

  /**
   * Given the cluster default resource, when a request sets a zone CIDR block only earlier releases accepted, then
   * it is rejected, while re-sending the persisted value unchanged is accepted.
   */
  @Tag("S3Unit")
  @Test
  def testClusterDefaultLegacyZoneCidrBlocks(): Unit = {
    val clusterDefault = new ConfigResource(BROKER, "")
    assertThrows(classOf[ConfigException], validateAlteredConfigs(
      clusterDefault, zoneCidrBlocks("az-a@10.0.0/24"), emptyMap()))
    assertDoesNotThrow(validateAlteredConfigs(
      clusterDefault, zoneCidrBlocks("az-a@10.0.0/24"), zoneCidrBlocks("az-a@10.0.0/24")))
  }

  /**
   * Given a persisted zone CIDR blocks value, when a request re-sends it unchanged or deletes it, then it is accepted.
   */
  @Tag("S3Unit")
  @Test
  def testPersistedLegacyZoneCidrBlocksAccepted(): Unit = {
    val existing = zoneCidrBlocks("az-a@10.0.0/24")
    val broker = new ConfigResource(BROKER, "0")
    assertDoesNotThrow(validateAlteredConfigs(broker, zoneCidrBlocks("az-a@10.0.0/24"), existing))
    assertDoesNotThrow(validateAlteredConfigs(broker, zoneCidrBlocks(null), existing))
    assertDoesNotThrow(validateAlteredConfigs(broker, emptyMap(), existing))
  }

  /**
   * Given a broker resource, when a request sets a fully supported zone CIDR blocks value, then it is accepted.
   */
  @Tag("S3Unit")
  @Test
  def testSupportedZoneCidrBlocksAccepted(): Unit = {
    assertDoesNotThrow(validateAlteredConfigs(new ConfigResource(BROKER, "0"),
      zoneCidrBlocks("az-a@192.0.2.0/24<>az-b@2001:db8::/32"), emptyMap()))
  }

  /**
   * Given a resource that is not a broker, when a request alters it, then the zone CIDR blocks are not validated.
   */
  @Tag("S3Unit")
  @Test
  def testNonBrokerResourcesAreNotValidated(): Unit = {
    assertDoesNotThrow(validateAlteredConfigs(
      new ConfigResource(TOPIC, "foo"), zoneCidrBlocks("az-a@10.0.0/24"), emptyMap()))
    assertDoesNotThrow(validateAlteredConfigs(
      new ConfigResource(CLIENT_METRICS, "subscription-1"), zoneCidrBlocks("az-a@10.0.0/24"), emptyMap()))
  }
  // AutoMQ inject end
}
