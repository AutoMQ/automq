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

import kafka.server.{KafkaConfig, ThrottledReplicaListValidator}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM
import org.apache.kafka.common.config.ConfigDef.Type.INT
import org.apache.kafka.common.config.{ConfigException, TopicConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.{Collections, Properties}

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
    assertTrue(LogConfig.configNames.forall { config =>
      val serverConfigOpt = LogConfig.serverConfigName(config)
      serverConfigOpt.isDefined && (serverConfigOpt.get != null)
    })
  }

  @Test
  def testKafkaConfigToProps(): Unit = {
    val millisInHour = 60L * 60L * 1000L
    val kafkaProps = TestUtils.createBrokerConfig(nodeId = 0, zkConnect = "")
    kafkaProps.put(KafkaConfig.LogRollTimeHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRollTimeJitterHoursProp, "2")
    kafkaProps.put(KafkaConfig.LogRetentionTimeHoursProp, "2")

    val kafkaConfig = KafkaConfig.fromProps(kafkaProps)
    val logProps = LogConfig.extractLogConfigMap(kafkaConfig)
    assertEquals(2 * millisInHour, logProps.get(LogConfig.SegmentMsProp))
    assertEquals(2 * millisInHour, logProps.get(LogConfig.SegmentJitterMsProp))
    assertEquals(2 * millisInHour, logProps.get(LogConfig.RetentionMsProp))
  }

  @Test
  def testFromPropsEmpty(): Unit = {
    val p = new Properties()
    val config = LogConfig(p)
    assertEquals(LogConfig(), config)
  }

  @Test
  def testFromPropsInvalid(): Unit = {
    LogConfig.configNames.foreach(name => name match {
      case LogConfig.UncleanLeaderElectionEnableProp => assertPropertyInvalid(name, "not a boolean")
      case LogConfig.RetentionBytesProp => assertPropertyInvalid(name, "not_a_number")
      case LogConfig.RetentionMsProp => assertPropertyInvalid(name, "not_a_number" )
      case LogConfig.CleanupPolicyProp => assertPropertyInvalid(name, "true", "foobar")
      case LogConfig.MinCleanableDirtyRatioProp => assertPropertyInvalid(name, "not_a_number", "-0.1", "1.2")
      case LogConfig.MinInSyncReplicasProp => assertPropertyInvalid(name, "not_a_number", "0", "-1")
      case LogConfig.MessageFormatVersionProp => assertPropertyInvalid(name, "")
      case _ => assertPropertyInvalid(name, "not_a_number", "-1")
    })
  }

  @Test
  def testInvalidCompactionLagConfig(): Unit = {
    val props = new Properties
    props.setProperty(LogConfig.MaxCompactionLagMsProp, "100")
    props.setProperty(LogConfig.MinCompactionLagMsProp, "200")
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
    val deleteDelayServerDefault = configDef.getConfigValue(deleteDelayKey, LogConfig.ServerDefaultHeaderName)
    assertEquals(KafkaConfig.LogDeleteDelayMsProp, deleteDelayServerDefault)

    val keyWithNoServerMapping = configDef.configKeys.get(configNameWithNoServerMapping)
    val nullServerDefault = configDef.getConfigValue(keyWithNoServerMapping, LogConfig.ServerDefaultHeaderName)
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
    topicOverrides.setProperty(LogConfig.MinInSyncReplicasProp, "2")
    // Overrides value from broker config
    topicOverrides.setProperty(LogConfig.RetentionBytesProp, "100")
    // Unknown topic config, but known broker config
    topicOverrides.setProperty(KafkaConfig.SslTruststorePasswordProp, "sometrustpasswrd")
    // Unknown config
    topicOverrides.setProperty("unknown.topic.password.config", "bbbb")
    // We don't currently have any sensitive topic configs, if we add them, we should set one here
    val logConfig = LogConfig.fromProps(LogConfig.extractLogConfigMap(kafkaConfig), topicOverrides)
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
      assertThrows(classOf[Exception], () => LogConfig(props))
    })
  }

}
