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

package kafka.log

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.INT
import org.apache.kafka.common.config.{ConfigException, TopicConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.{Collections, Properties}
import org.apache.kafka.server.common.MetadataVersion.IBP_3_0_IV1
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.storage.internals.log.{LogConfig, ThrottledReplicaListValidator}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

class LogConfigTest {

  /**
   * This test verifies that KafkaConfig object initialization does not depend on
   * LogConfig initialization. Bad things happen due to static initialization
   * order dependencies. For example, LogConfig.configDef ends up adding null
   * values in serverDefaultConfigNames. This test ensures that the mapping of
   * keys from LogConfig to KafkaConfig are not missing values.
   */
  @Test
  def ensureNoStaticInitializationOrderDependency(): Unit = {
    // Access any KafkaConfig val to load KafkaConfig object before LogConfig.
    assertNotNull(KafkaConfig.LogRetentionTimeMillisProp)
    assertTrue(LogConfig.configNames.asScala
      .filter(config => !LogConfig.CONFIGS_WITH_NO_SERVER_DEFAULTS.contains(config))
      .forall { config =>
        val serverConfigOpt = LogConfig.serverConfigName(config)
        serverConfigOpt.isPresent && (serverConfigOpt.get != null)
      })
  }

  @nowarn("cat=deprecation")
  @Test
  def testKafkaConfigToProps(): Unit = {
    val millisInHour = 60L * 60L * 1000L
    val millisInDay = 24L * millisInHour
    val bytesInGB: Long = 1024 * 1024 * 1024
    val kafkaProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    kafkaProps.put(KafkaConfig.LogRollTimeHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRollTimeJitterHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRetentionTimeHoursProp, "960") // 40 days
    kafkaProps.put(KafkaConfig.LogMessageFormatVersionProp, "0.11.0")
    kafkaProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2592000000") // 30 days
    kafkaProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967296") // 4 GB

    val logProps = KafkaConfig.fromProps(kafkaProps).extractLogConfigMap
    assertEquals(2 * millisInHour, logProps.get(TopicConfig.SEGMENT_MS_CONFIG))
    assertEquals(2 * millisInHour, logProps.get(TopicConfig.SEGMENT_JITTER_MS_CONFIG))
    assertEquals(40 * millisInDay, logProps.get(TopicConfig.RETENTION_MS_CONFIG))
    // The message format version should always be 3.0 if the inter-broker protocol version is 3.0 or higher
    assertEquals(IBP_3_0_IV1.version, logProps.get(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG))
    assertEquals(30 * millisInDay, logProps.get(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG))
    assertEquals(4 * bytesInGB, logProps.get(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG))
  }

  @nowarn("cat=deprecation")
  @Test
  def testFromPropsInvalid(): Unit = {
    LogConfig.configNames.forEach(name => name match {
      case TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG => assertPropertyInvalid(name, "not a boolean")
      case TopicConfig.RETENTION_BYTES_CONFIG => assertPropertyInvalid(name, "not_a_number")
      case TopicConfig.RETENTION_MS_CONFIG => assertPropertyInvalid(name, "not_a_number" )
      case TopicConfig.CLEANUP_POLICY_CONFIG => assertPropertyInvalid(name, "true", "foobar")
      case TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG => assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2")
      case TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG => assertPropertyInvalid(name, "not_a_number", "0", "-1")
      case TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG => assertPropertyInvalid(name, "")
      case TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG => assertPropertyInvalid(name, "not_a_boolean")
      case TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG => assertPropertyInvalid(name, "not_a_number", "-3")
      case TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG => assertPropertyInvalid(name, "not_a_number", "-3")

      case _ => assertPropertyInvalid(name, "not_a_number", "-1")
    })
  }

  @Test
  def testInvalidCompactionLagConfig(): Unit = {
    val props = new Properties
    props.setProperty(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "100")
    props.setProperty(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "200")
    assertThrows(classOf[Exception], () => LogConfig.validate(props))
  }

  @Test
  def shouldValidateThrottledReplicasConfig(): Unit = {
    assertTrue(isValid("*"))
    assertTrue(isValid("* "))
    assertTrue(isValid(""))
    assertTrue(isValid(" "))
    assertTrue(isValid("100:10"))
    assertTrue(isValid("100:10,12:10"))
    assertTrue(isValid("100:10,12:10,15:1"))
    assertTrue(isValid("100:10,12:10,15:1  "))
    assertTrue(isValid("100:0,"))

    assertFalse(isValid("100"))
    assertFalse(isValid("100:"))
    assertFalse(isValid("100:0,10"))
    assertFalse(isValid("100:0,10:"))
    assertFalse(isValid("100:0,10:   "))
    assertFalse(isValid("100 :0,10:   "))
    assertFalse(isValid("100: 0,10:   "))
    assertFalse(isValid("100:0,10 :   "))
    assertFalse(isValid("*,100:10"))
    assertFalse(isValid("* ,100:10"))
  }

  /* Sanity check that toHtmlTable produces one of the expected configs */
  @Test
  def testToHtmlTable(): Unit = {
    val html = LogConfig.configDefCopy.toHtmlTable
    val expectedConfig = "<td>file.delete.delay.ms</td>"
    assertTrue(html.contains(expectedConfig), s"Could not find `$expectedConfig` in:\n $html")
  }

  /* Sanity check that toHtml produces one of the expected configs */
  @Test
  def testToHtml(): Unit = {
    val html = LogConfig.configDefCopy.toHtml(4, (key: String) => "prefix_" + key, Collections.emptyMap())
    val expectedConfig = "<h4><a id=\"file.delete.delay.ms\"></a><a id=\"prefix_file.delete.delay.ms\" href=\"#prefix_file.delete.delay.ms\">file.delete.delay.ms</a></h4>"
    assertTrue(html.contains(expectedConfig), s"Could not find `$expectedConfig` in:\n $html")
  }

  /* Sanity check that toEnrichedRst produces one of the expected configs */
  @Test
  def testToEnrichedRst(): Unit = {
    val rst = LogConfig.configDefCopy.toEnrichedRst
    val expectedConfig = "``file.delete.delay.ms``"
    assertTrue(rst.contains(expectedConfig), s"Could not find `$expectedConfig` in:\n $rst")
  }

  /* Sanity check that toEnrichedRst produces one of the expected configs */
  @Test
  def testToRst(): Unit = {
    val rst = LogConfig.configDefCopy.toRst
    val expectedConfig = "``file.delete.delay.ms``"
    assertTrue(rst.contains(expectedConfig), s"Could not find `$expectedConfig` in:\n $rst")
  }

  @Test
  def testGetConfigValue(): Unit = {
    // Add a config that doesn't set the `serverDefaultConfigName`
    val configDef = LogConfig.configDefCopy
    val configNameWithNoServerMapping = "log.foo"
    configDef.define(configNameWithNoServerMapping, INT, 1, MEDIUM, s"$configNameWithNoServerMapping doc")

    val deleteDelayKey = configDef.configKeys.get(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG)
    val deleteDelayServerDefault = configDef.getConfigValue(deleteDelayKey, LogConfig.SERVER_DEFAULT_HEADER_NAME)
    assertEquals(KafkaConfig.LogDeleteDelayMsProp, deleteDelayServerDefault)

    val keyWithNoServerMapping = configDef.configKeys.get(configNameWithNoServerMapping)
    val nullServerDefault = configDef.getConfigValue(keyWithNoServerMapping, LogConfig.SERVER_DEFAULT_HEADER_NAME)
    assertNull(nullServerDefault)
  }

  @Test
  def testOverriddenConfigsAsLoggableString(): Unit = {
    val kafkaProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    kafkaProps.put("unknown.broker.password.config", "aaaaa")
    kafkaProps.put(KafkaConfig.SslKeyPasswordProp, "somekeypassword")
    kafkaProps.put(KafkaConfig.LogRetentionBytesProp, "50")
    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)
    val topicOverrides = new Properties
    // Only set as a topic config
    topicOverrides.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    // Overrides value from broker config
    topicOverrides.setProperty(TopicConfig.RETENTION_BYTES_CONFIG, "100")
    // Unknown topic config, but known broker config
    topicOverrides.setProperty(KafkaConfig.SslTruststorePasswordProp, "sometrustpasswrd")
    // Unknown config
    topicOverrides.setProperty("unknown.topic.password.config", "bbbb")
    // We don't currently have any sensitive topic configs, if we add them, we should set one here
    val logConfig = LogConfig.fromProps(kafkaConfig.extractLogConfigMap, topicOverrides)
    assertEquals("{min.insync.replicas=2, retention.bytes=100, ssl.truststore.password=(redacted), unknown.topic.password.config=(redacted)}",
      logConfig.overriddenConfigsAsLoggableString)
  }

  private def isValid(configValue: String): Boolean = {
    try {
      ThrottledReplicaListValidator.ensureValidString("", configValue)
      true
    } catch {
      case _: ConfigException => false
    }
  }

  private def assertPropertyInvalid(name: String, values: AnyRef*): Unit = {
    values.foreach((value) => {
      val props = new Properties
      props.setProperty(name, value.toString)
      assertThrows(classOf[Exception], () => new LogConfig(props))
    })
  }

  @Test
  def testLocalLogRetentionDerivedProps(): Unit = {
    val props = new Properties()
    val retentionBytes = 1024
    val retentionMs = 1000L
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes.toString)
    props.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString)
    val logConfig = new LogConfig(props)

    assertEquals(retentionMs, logConfig.localRetentionMs)
    assertEquals(retentionBytes, logConfig.localRetentionBytes)
  }

  @Test
  def testLocalLogRetentionDerivedDefaultProps(): Unit = {
    val logConfig = new LogConfig(new Properties())

    // Local retention defaults are derived from retention properties which can be default or custom.
    assertEquals(LogConfig.DEFAULT_RETENTION_MS, logConfig.localRetentionMs)
    assertEquals(LogConfig.DEFAULT_RETENTION_BYTES, logConfig.localRetentionBytes)
  }

  @Test
  def testLocalLogRetentionProps(): Unit = {
    val props = new Properties()
    val localRetentionMs = 500
    val localRetentionBytes = 1000
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, 2000.toString)
    props.put(TopicConfig.RETENTION_MS_CONFIG, 1000.toString)

    props.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs.toString)
    props.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes.toString)
    val logConfig = new LogConfig(props)

    assertEquals(localRetentionMs, logConfig.localRetentionMs)
    assertEquals(localRetentionBytes, logConfig.localRetentionBytes)
  }

  @Test
  def testInvalidLocalLogRetentionProps(): Unit = {
    // Check for invalid localRetentionMs, < -2
    doTestInvalidLocalLogRetentionProps(-3, 10, 2, 500L)

    // Check for invalid localRetentionBytes < -2
    doTestInvalidLocalLogRetentionProps(500L, -3, 2, 1000L)

    // Check for invalid case of localRetentionMs > retentionMs
    doTestInvalidLocalLogRetentionProps(2000L, 2, 100, 1000L)

    // Check for invalid case of localRetentionBytes > retentionBytes
    doTestInvalidLocalLogRetentionProps(500L, 200, 100, 1000L)

    // Check for invalid case of localRetentionMs (-1 viz unlimited) > retentionMs,
    doTestInvalidLocalLogRetentionProps(-1, 200, 100, 1000L)

    // Check for invalid case of localRetentionBytes(-1 viz unlimited) > retentionBytes
    doTestInvalidLocalLogRetentionProps(2000L, -1, 100, 1000L)
  }

  private def doTestInvalidLocalLogRetentionProps(localRetentionMs: Long,
                                                  localRetentionBytes: Int,
                                                  retentionBytes: Int,
                                                  retentionMs: Long) = {
    val kafkaProps = TestUtils.createDummyBrokerConfig()
    kafkaProps.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)

    val props = new Properties()
    props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes.toString)
    props.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString)

    props.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs.toString)
    props.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes.toString)
    assertThrows(classOf[ConfigException],
      () => LogConfig.validate(props, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
  }

  @Test
  def testEnableRemoteLogStorageOnCompactedTopic(): Unit = {
    val kafkaProps = TestUtils.createDummyBrokerConfig()
    kafkaProps.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)

    val logProps = new Properties()
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled)

    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    assertThrows(classOf[ConfigException],
      () => LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete,compact")
    assertThrows(classOf[ConfigException],
      () => LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete")
    assertThrows(classOf[ConfigException],
      () => LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
  }

  @ParameterizedTest(name = "testEnableRemoteLogStorage with sysRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testEnableRemoteLogStorage(sysRemoteStorageEnabled: Boolean): Unit = {
    val kafkaProps = TestUtils.createDummyBrokerConfig()
    kafkaProps.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, sysRemoteStorageEnabled.toString)
    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)

    val logProps = new Properties()
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    if (sysRemoteStorageEnabled) {
      LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled)
    } else {
      val message = assertThrows(classOf[ConfigException],
        () => LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
      assertTrue(message.getMessage.contains("Tiered Storage functionality is disabled in the broker"))
    }
  }

  @ParameterizedTest(name = "testTopicCreationWithInvalidRetentionTime with sysRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testTopicCreationWithInvalidRetentionTime(sysRemoteStorageEnabled: Boolean): Unit = {
    val kafkaProps = TestUtils.createDummyBrokerConfig()
    kafkaProps.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, sysRemoteStorageEnabled.toString)
    kafkaProps.put(KafkaConfig.LogRetentionTimeMillisProp, "1000")
    kafkaProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "900")
    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)

    // Topic local log retention time inherited from Broker is greater than the topic's complete log retention time
    val logProps = new Properties()
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, sysRemoteStorageEnabled.toString)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, "500")
    if (sysRemoteStorageEnabled) {
      val message = assertThrows(classOf[ConfigException],
        () => LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
      assertTrue(message.getMessage.contains(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG))
    } else {
      LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled)
    }
  }

  @ParameterizedTest(name = "testTopicCreationWithInvalidRetentionSize with sysRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testTopicCreationWithInvalidRetentionSize(sysRemoteStorageEnabled: Boolean): Unit = {
    val props = TestUtils.createDummyBrokerConfig()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, sysRemoteStorageEnabled.toString)
    props.put(KafkaConfig.LogRetentionBytesProp, "1024")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "512")
    val kafkaConfig = KafkaConfig.fromProps(props)

    // Topic local retention size inherited from Broker is greater than the topic's complete log retention size
    val logProps = new Properties()
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, sysRemoteStorageEnabled.toString)
    logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, "128")
    if (sysRemoteStorageEnabled) {
      val message = assertThrows(classOf[ConfigException],
        () => LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
      assertTrue(message.getMessage.contains(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG))
    } else {
      LogConfig.validate(logProps, kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled)
    }
  }

  @ParameterizedTest(name = "testValidateBrokerLogConfigs with sysRemoteStorageEnabled: {0}")
  @ValueSource(booleans = Array(true, false))
  def testValidateBrokerLogConfigs(sysRemoteStorageEnabled: Boolean): Unit = {
    val props = TestUtils.createDummyBrokerConfig()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, sysRemoteStorageEnabled.toString)
    props.put(KafkaConfig.LogRetentionBytesProp, "1024")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "2048")
    val kafkaConfig = KafkaConfig.fromProps(props)

    if (sysRemoteStorageEnabled) {
      val message = assertThrows(classOf[ConfigException],
        () => LogConfig.validateBrokerLogConfigValues(kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled))
      assertTrue(message.getMessage.contains(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG))
    } else {
      LogConfig.validateBrokerLogConfigValues(kafkaConfig.extractLogConfigMap, kafkaConfig.isRemoteLogStorageSystemEnabled)
    }
  }

  /* Verify that when the deprecated config LogMessageTimestampDifferenceMaxMsProp has non default value the new configs
   * LogMessageTimestampBeforeMaxMsProp and LogMessageTimestampAfterMaxMsProp are not changed from the default we are using
   * the deprecated config for backward compatibility.
   * See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details */
  @nowarn("cat=deprecation")
  @Test
  def testTimestampBeforeMaxMsUsesDeprecatedConfig(): Unit = {
    val oneDayInMillis = 24 * 60 * 60 * 1000L
    val kafkaProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    kafkaProps.put(KafkaConfig.LogMessageTimestampBeforeMaxMsProp, Long.MaxValue.toString)
    kafkaProps.put(KafkaConfig.LogMessageTimestampAfterMaxMsProp, Long.MaxValue.toString)
    kafkaProps.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, oneDayInMillis.toString)

    val logProps = KafkaConfig.fromProps(kafkaProps).extractLogConfigMap
    assertEquals(oneDayInMillis, logProps.get(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG))
    assertEquals(oneDayInMillis, logProps.get(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG))
  }
}
