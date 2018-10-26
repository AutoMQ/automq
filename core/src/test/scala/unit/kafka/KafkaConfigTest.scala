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
package kafka

import java.io.File
import java.nio.file.Files
import java.util

import kafka.server.KafkaConfig
import kafka.utils.Exit
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.internals.FatalExitError
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs

class KafkaTest {

  @Before
  def setUp(): Unit = Exit.setExitProcedure((status, _) => throw new FatalExitError(status))

  @After
  def tearDown(): Unit = Exit.resetExitProcedure()

  @Test
  def testGetKafkaConfigFromArgs(): Unit = {
    val propertiesFile = prepareDefaultConfig()

    // We should load configuration file without any arguments
    val config1 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertEquals(1, config1.brokerId)

    // We should be able to override given property on command line
    val config2 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "broker.id=2")))
    assertEquals(2, config2.brokerId)

    // We should be also able to set completely new property
    val config3 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "log.cleanup.policy=compact")))
    assertEquals(1, config3.brokerId)
    assertEquals(util.Arrays.asList("compact"), config3.logCleanupPolicy)

    // We should be also able to set several properties
    val config4 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "log.cleanup.policy=compact,delete", "--override", "broker.id=2")))
    assertEquals(2, config4.brokerId)
    assertEquals(util.Arrays.asList("compact","delete"), config4.logCleanupPolicy)
  }

  @Test(expected = classOf[FatalExitError])
  def testGetKafkaConfigFromArgsNonArgsAtTheEnd(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "broker.id=1", "broker.id=2")))
  }

  @Test(expected = classOf[FatalExitError])
  def testGetKafkaConfigFromArgsNonArgsOnly(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "broker.id=2")))
  }

  @Test(expected = classOf[FatalExitError])
  def testGetKafkaConfigFromArgsNonArgsAtTheBegging(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "--override", "broker.id=2")))
  }

  @Test
  def testKafkaSslPasswords(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "ssl.keystore.password=keystore_password",
                                                                                    "--override", "ssl.key.password=key_password",
                                                                                    "--override", "ssl.truststore.password=truststore_password")))
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeyPasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeystorePasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslTruststorePasswordProp).toString)

    assertEquals("key_password", config.getPassword(KafkaConfig.SslKeyPasswordProp).value)
    assertEquals("keystore_password", config.getPassword(KafkaConfig.SslKeystorePasswordProp).value)
    assertEquals("truststore_password", config.getPassword(KafkaConfig.SslTruststorePasswordProp).value)
  }

  @Test
  def testKafkaSslPasswordsWithSymbols(): Unit = {
    val password = "=!#-+!?*/\"\'^%$=\\.,@:;="
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile,
      "--override", "ssl.keystore.password=" + password,
      "--override", "ssl.key.password=" + password,
      "--override", "ssl.truststore.password=" + password)))
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeyPasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeystorePasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslTruststorePasswordProp).toString)

    assertEquals(password, config.getPassword(KafkaConfig.SslKeystorePasswordProp).value)
    assertEquals(password, config.getPassword(KafkaConfig.SslKeyPasswordProp).value)
    assertEquals(password, config.getPassword(KafkaConfig.SslTruststorePasswordProp).value)
  }

  @Test
  def testConnectionsMaxReauthMsDefault(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertEquals(0L, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS).asInstanceOf[Long])
  }

  @Test
  def testConnectionsMaxReauthMsExplicit(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val expected = 3600000
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"sasl_ssl.oauthbearer.connections.max.reauth.ms=${expected}")))
    assertEquals(expected, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS).asInstanceOf[Long])
  }

  def prepareDefaultConfig(): String = {
    prepareConfig(Array("broker.id=1", "zookeeper.connect=somewhere"))
  }

  def prepareConfig(lines : Array[String]): String = {
    val file = File.createTempFile("kafkatest", ".properties")
    file.deleteOnExit()

    val writer = Files.newOutputStream(file.toPath)
    try {
      lines.foreach { l =>
        writer.write(l.getBytes)
        writer.write("\n".getBytes)
      }
      file.getAbsolutePath
    } finally writer.close()
  }
}
