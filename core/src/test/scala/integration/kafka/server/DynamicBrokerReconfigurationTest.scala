/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.api.SaslSetup
import kafka.log.LogConfig
import kafka.coordinator.group.OffsetConfig
import kafka.message.ProducerCompressionCodec
import kafka.utils.{ShutdownableThread, TestUtils}
import kafka.utils.Implicits._
import kafka.zk.{ConfigEntityChangeNotificationZNode, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.{AuthenticationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object DynamicBrokerReconfigurationTest {
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"
}

class DynamicBrokerReconfigurationTest extends ZooKeeperTestHarness with SaslSetup {

  import DynamicBrokerReconfigurationTest._

  private var servers = new ArrayBuffer[KafkaServer]
  private val numServers = 3
  private val producers = new ArrayBuffer[KafkaProducer[String, String]]
  private val consumers = new ArrayBuffer[KafkaConsumer[String, String]]
  private val adminClients = new ArrayBuffer[AdminClient]()
  private val clientThreads = new ArrayBuffer[ShutdownableThread]()
  private val topic = "testtopic"

  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  private val clientSaslProps = kafkaClientSaslProperties(kafkaClientSaslMechanism, dynamicJaasConfig = true)

  private val trustStoreFile1 = File.createTempFile("truststore", ".jks")
  private val trustStoreFile2 = File.createTempFile("truststore", ".jks")
  private val sslProperties1 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile1), "kafka")
  private val sslProperties2 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile2), "kafka")
  private val invalidSslProperties = invalidSslConfigs

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism)))
    super.setUp()

    (0 until numServers).foreach { brokerId =>

      val props = TestUtils.createBrokerConfig(brokerId, zkConnect, trustStoreFile = Some(trustStoreFile1))
      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
      props.put(KafkaConfig.ListenersProp, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
      props.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"$SecureInternal:SSL, $SecureExternal:SASL_SSL")
      props.put(KafkaConfig.InterBrokerListenerNameProp, SecureInternal)
      props.put(KafkaConfig.ZkEnableSecureAclsProp, "true")
      props.put(KafkaConfig.SaslEnabledMechanismsProp, kafkaServerSaslMechanisms.mkString(","))
      props.put(KafkaConfig.LogSegmentBytesProp, "2000")

      props ++= sslProperties1
      addKeystoreWithListenerPrefix(sslProperties1, props, SecureInternal)

      // Set invalid static properties to ensure that dynamic config is used
      props ++= invalidSslProperties
      addKeystoreWithListenerPrefix(invalidSslProperties, props, SecureExternal)

      val kafkaConfig = KafkaConfig.fromProps(props)
      configureDynamicKeystoreInZooKeeper(kafkaConfig, Seq(brokerId), sslProperties1)

      servers += TestUtils.createServer(kafkaConfig)
    }

    TestUtils.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, OffsetConfig.DefaultOffsetsTopicNumPartitions,
      replicationFactor = numServers, servers, servers.head.groupCoordinator.offsetsTopicConfigs)

    TestUtils.createTopic(zkClient, topic, numPartitions = 10, replicationFactor = numServers, servers)
    createAdminClient(SecurityProtocol.SSL, SecureInternal)
  }

  @After
  override def tearDown() {
    clientThreads.foreach(_.interrupt())
    clientThreads.foreach(_.initiateShutdown())
    clientThreads.foreach(_.join(5 * 1000))
    producers.foreach(_.close())
    consumers.foreach(_.close())
    adminClients.foreach(_.close())
    TestUtils.shutdownServers(servers)
    super.tearDown()
    closeSasl()
  }

  @Test
  def testKeystoreUpdate(): Unit = {
    val producer = createProducer(trustStoreFile1, retries = 0)
    val consumer = createConsumer("group1", trustStoreFile1)
    verifyProduceConsume(producer, consumer, 10)

    // Producer with new truststore should fail to connect before keystore update
    val producer2 = createProducer(trustStoreFile2, retries = 0)
    verifyAuthenticationFailure(producer2)

    // Update broker keystore
    configureDynamicKeystoreInZooKeeper(servers.head.config, servers.map(_.config.brokerId), sslProperties2)
    waitForKeystore(sslProperties2)

    // New producer with old truststore should fail to connect
    val producer1 = createProducer(trustStoreFile1, retries = 0)
    verifyAuthenticationFailure(producer1)

    // New producer with new truststore should work
    val producer3 = createProducer(trustStoreFile2, retries = 0)
    verifyProduceConsume(producer3, consumer, 10)

    // Old producer with old truststore should continue to work (with their old connections)
    verifyProduceConsume(producer, consumer, 10)
  }

  @Test
  def testKeyStoreDescribeUsingAdminClient(): Unit = {

    def verifyConfig(configName: String, configEntry: ConfigEntry, isSensitive: Boolean, expectedProps: Properties): Unit = {
      if (isSensitive) {
        assertTrue(s"Value is sensitive: $configName", configEntry.isSensitive)
        assertNull(s"Sensitive value returned for $configName", configEntry.value)
      } else {
        assertFalse(s"Config is not sensitive: $configName", configEntry.isSensitive)
        assertEquals(expectedProps.getProperty(configName), configEntry.value)
      }
    }

    def verifySynonym(configName: String, synonym: ConfigSynonym, isSensitive: Boolean,
                      expectedPrefix: String, expectedSource: ConfigSource, expectedProps: Properties): Unit = {
      if (isSensitive)
        assertNull(s"Sensitive value returned for $configName", synonym.value)
      else
        assertEquals(expectedProps.getProperty(configName), synonym.value)
      assertTrue(s"Expected listener config, got $synonym", synonym.name.startsWith(expectedPrefix))
      assertEquals(expectedSource, synonym.source)
    }

    def verifySynonyms(configName: String, synonyms: util.List[ConfigSynonym], isSensitive: Boolean,
                       prefix: String, defaultValue: Option[String]): Unit = {

      val overrideCount = if (prefix.isEmpty) 0 else 2
      assertEquals(s"Wrong synonyms for $configName: $synonyms", 1 + overrideCount + defaultValue.size, synonyms.size)
      if (overrideCount > 0) {
        val listenerPrefix = "listener.name.external.ssl."
        verifySynonym(configName, synonyms.get(0), isSensitive, listenerPrefix, ConfigSource.DYNAMIC_BROKER_CONFIG, sslProperties1)
        verifySynonym(configName, synonyms.get(1), isSensitive, listenerPrefix, ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties)
      }
      verifySynonym(configName, synonyms.get(overrideCount), isSensitive, "ssl.", ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties)
      defaultValue.foreach { value =>
        val defaultProps = new Properties
        defaultProps.setProperty(configName, value)
        verifySynonym(configName, synonyms.get(overrideCount + 1), isSensitive, "ssl.", ConfigSource.DEFAULT_CONFIG, defaultProps)
      }
    }

    def verifySslConfig(prefix: String, expectedProps: Properties, configDesc: Config): Unit = {
      Seq(SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_TYPE_CONFIG, SSL_KEYSTORE_PASSWORD_CONFIG, SSL_KEY_PASSWORD_CONFIG).foreach { configName =>
        val desc = configEntry(configDesc, s"$prefix$configName")
        val isSensitive = configName.contains("password")
        verifyConfig(configName, desc, isSensitive, if (prefix.isEmpty) invalidSslProperties else sslProperties1)
        val defaultValue = if (configName == SSL_KEYSTORE_TYPE_CONFIG) Some("JKS") else None
        verifySynonyms(configName, desc.synonyms, isSensitive, prefix, defaultValue)
      }
    }

    val adminClient = adminClients.head

    val configDesc = describeConfig(adminClient)
    verifySslConfig("listener.name.external.", sslProperties1, configDesc)
    verifySslConfig("", invalidSslProperties, configDesc)
  }

  @Test
  def testKeyStoreAlterUsingAdminClient(): Unit = {
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions = 10, replicationFactor = numServers, servers)

    // Start a producer and consumer that work with the current truststore.
    // This should continue working while changes are made
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)
    TestUtils.waitUntilTrue(() => consumerThread.received >= 10, "Messages not received")

    // Update broker keystore for external listener
    val adminClient = adminClients.head
    alterSslKeystore(adminClient, sslProperties2, SecureExternal)

    // Produce/consume should work with new truststore
    val producer = createProducer(trustStoreFile2, retries = 0)
    val consumer = createConsumer("group1", trustStoreFile2, topic2)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should fail without update
    alterSslKeystore(adminClient, sslProperties2, SecureInternal, expectFailure = true)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should succeed
    val sslPropertiesCopy = sslProperties1.clone().asInstanceOf[Properties]
    val oldFile = new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
    val newFile = File.createTempFile("keystore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    sslPropertiesCopy.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, newFile.getPath)
    alterSslKeystore(adminClient, sslPropertiesCopy, SecureInternal)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify that all messages sent with retries=0 while keystores were being altered were consumed
    stopAndVerifyProduceConsume(producerThread, consumerThread, mayFailRequests = false)
  }

  @Test
  def testLogCleanerConfig(): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(0)

    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 1)

    val props = new Properties
    props.put(KafkaConfig.LogCleanerThreadsProp, "2")
    props.put(KafkaConfig.LogCleanerDedupeBufferSizeProp, "20000000")
    props.put(KafkaConfig.LogCleanerDedupeBufferLoadFactorProp, "0.8")
    props.put(KafkaConfig.LogCleanerIoBufferSizeProp, "300000")
    props.put(KafkaConfig.MessageMaxBytesProp, "40000")
    props.put(KafkaConfig.LogCleanerIoMaxBytesPerSecondProp, "50000000")
    props.put(KafkaConfig.LogCleanerBackoffMsProp, "6000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogCleanerThreadsProp, "2"))

    // Verify cleaner config was updated
    val newCleanerConfig = servers.head.logManager.cleaner.currentConfig
    assertEquals(2, newCleanerConfig.numThreads)
    assertEquals(20000000, newCleanerConfig.dedupeBufferSize)
    assertEquals(0.8, newCleanerConfig.dedupeBufferLoadFactor, 0.001)
    assertEquals(300000, newCleanerConfig.ioBufferSize)
    assertEquals(40000, newCleanerConfig.maxMessageSize)
    assertEquals(50000000, newCleanerConfig.maxIoBytesPerSecond, 50000000)
    assertEquals(6000, newCleanerConfig.backOffMs)

    // Verify thread count
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)

    // Stop a couple of threads and verify they are recreated if any config is updated
    def cleanerThreads = Thread.getAllStackTraces.keySet.asScala.filter(_.getName.startsWith("kafka-log-cleaner-thread-"))
    cleanerThreads.take(2).foreach(_.interrupt())
    TestUtils.waitUntilTrue(() => cleanerThreads.size == (2 * numServers) - 2, "Threads did not exit")
    props.put(KafkaConfig.LogCleanerBackoffMsProp, "8000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogCleanerBackoffMsProp, "8000"))
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread, mayFailRequests = false)
  }

  @Test
  def testDefaultTopicConfig(): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)

    val props = new Properties
    props.put(KafkaConfig.LogSegmentBytesProp, "10000")
    props.put(KafkaConfig.LogRollTimeMillisProp, TimeUnit.HOURS.toMillis(2).toString)
    props.put(KafkaConfig.LogRollTimeJitterMillisProp, TimeUnit.HOURS.toMillis(1).toString)
    props.put(KafkaConfig.LogIndexSizeMaxBytesProp, "100000")
    props.put(KafkaConfig.LogFlushIntervalMessagesProp, "1000")
    props.put(KafkaConfig.LogFlushIntervalMsProp, "60000")
    props.put(KafkaConfig.LogRetentionBytesProp, "10000000")
    props.put(KafkaConfig.LogRetentionTimeMillisProp, TimeUnit.DAYS.toMillis(1).toString)
    props.put(KafkaConfig.MessageMaxBytesProp, "100000")
    props.put(KafkaConfig.LogIndexIntervalBytesProp, "10000")
    props.put(KafkaConfig.LogCleanerDeleteRetentionMsProp, TimeUnit.DAYS.toMillis(1).toString)
    props.put(KafkaConfig.LogCleanerMinCompactionLagMsProp, "60000")
    props.put(KafkaConfig.LogDeleteDelayMsProp, "60000")
    props.put(KafkaConfig.LogCleanerMinCleanRatioProp, "0.3")
    props.put(KafkaConfig.LogCleanupPolicyProp, "delete")
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, "false")
    props.put(KafkaConfig.MinInSyncReplicasProp, "2")
    props.put(KafkaConfig.CompressionTypeProp, "gzip")
    props.put(KafkaConfig.LogPreAllocateProp, true.toString)
    props.put(KafkaConfig.LogMessageTimestampTypeProp, TimestampType.LOG_APPEND_TIME.toString)
    props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, "1000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogSegmentBytesProp, "10000"))

    // Verify that all broker defaults have been updated
    servers.foreach { server =>
      props.asScala.foreach { case (k, v) =>
        assertEquals(s"Not reconfigured $k", server.config.originals.get(k).toString, v)
      }
    }

    // Verify that configs of existing logs have been updated
    val newLogConfig = LogConfig(KafkaServer.copyKafkaConfigToLog(servers.head.config))
    assertEquals(newLogConfig, servers.head.logManager.currentDefaultConfig)
    val log = servers.head.logManager.getLog(new TopicPartition(topic, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    TestUtils.waitUntilTrue(() => log.config.segmentSize == 10000, "Existing topic config using defaults not updated")
    props.asScala.foreach { case (k, v) =>
      val logConfigName = DynamicLogConfig.KafkaConfigToLogConfigName(k)
      val expectedValue = if (k == KafkaConfig.LogCleanupPolicyProp) s"[$v]" else v
      assertEquals(s"Not reconfigured $logConfigName for existing log", expectedValue,
        log.config.originals.get(logConfigName).toString)
    }
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.LOG_APPEND_TIME)

    // Verify that the new config is actually used for new segments of existing logs
    TestUtils.waitUntilTrue(() => log.logSegments.exists(_.size > 9000), "Log segment size increase not applied")

    // Verify that overridden topic configs are not updated when broker default is updated
    val log2 = servers.head.logManager.getLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))
      .getOrElse(throw new IllegalStateException("Log not found"))
    assertFalse("Overridden clean up policy should not be updated", log2.config.delete)
    assertEquals(ProducerCompressionCodec.name, log2.config.compressionType)

    // Verify that we can alter subset of log configs
    props.clear()
    props.put(KafkaConfig.LogMessageTimestampTypeProp, TimestampType.CREATE_TIME.toString)
    props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, "1000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogMessageTimestampTypeProp, TimestampType.CREATE_TIME.toString))
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.CREATE_TIME)

    // Verify that even though broker defaults can be defined at default cluster level for consistent
    // configuration across brokers, they can also be defined at per-broker level for testing
    props.clear()
    props.put(KafkaConfig.LogIndexSizeMaxBytesProp, "500000")
    alterConfigsOnServer(servers.head, props)
    assertEquals(500000, servers.head.config.values.get(KafkaConfig.LogIndexSizeMaxBytesProp))
    servers.tail.foreach { server => assertEquals(Defaults.LogIndexSizeMaxBytes, server.config.values.get(KafkaConfig.LogIndexSizeMaxBytesProp)) }

    // Verify that invalid configs are not applied
    val invalidProps = Map(
      KafkaConfig.LogMessageTimestampDifferenceMaxMsProp -> "abc", // Invalid type
      KafkaConfig.LogMessageTimestampTypeProp -> "invalid", // Invalid value
      KafkaConfig.LogRollTimeMillisProp -> "0" // Fails KafkaConfig validation
    )
    invalidProps.foreach { case (k, v) =>
      val newProps = new Properties
      newProps ++= props
      props.put(k, v)
      reconfigureServers(props, perBrokerConfig = false, (k, props.getProperty(k)), expectFailure = true)
    }

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread, mayFailRequests = false)
  }

  private def createProducer(trustStore: File, retries: Int,
                             clientId: String = "test-producer"): KafkaProducer[String, String] = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(SecureExternal))
    val propsOverride = new Properties
    propsOverride.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    val producer = TestUtils.createNewProducer(
      bootstrapServers,
      acks = -1,
      retries = retries,
      securityProtocol = SecurityProtocol.SASL_SSL,
      trustStoreFile = Some(trustStore),
      saslProperties = Some(clientSaslProps),
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      props = Some(propsOverride))
    producers += producer
    producer
  }

  private def createConsumer(groupId: String, trustStore: File, topic: String = topic):KafkaConsumer[String, String] = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(SecureExternal))
    val consumer = TestUtils.createNewConsumer(
      bootstrapServers,
      groupId,
      securityProtocol = SecurityProtocol.SASL_SSL,
      trustStoreFile = Some(trustStore),
      saslProperties = Some(clientSaslProps),
      keyDeserializer = new StringDeserializer,
      valueDeserializer = new StringDeserializer)
    consumer.subscribe(Collections.singleton(topic))
    consumers += consumer
    consumer
  }

  private def createAdminClient(securityProtocol: SecurityProtocol, listenerName: String): AdminClient = {
    val config = new util.HashMap[String, Object]
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(listenerName))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, Some(trustStoreFile1), Some(clientSaslProps))
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    val adminClient = AdminClient.create(config)
    adminClients += adminClient
    adminClient
  }

  private def verifyProduceConsume(producer: KafkaProducer[String, String],
                                   consumer: KafkaConsumer[String, String],
                                   numRecords: Int,
                                   topic: String = topic): Unit = {
    val producerRecords = (1 to numRecords).map(i => new ProducerRecord(topic, s"key$i", s"value$i"))
    producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))

    val records = new ArrayBuffer[ConsumerRecord[String, String]]
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50).asScala
      records.size == numRecords
    }, s"Consumed ${records.size} records until timeout instead of the expected $numRecords records")
  }

  private def verifyAuthenticationFailure(producer: KafkaProducer[_, _]): Unit = {
    try {
      producer.partitionsFor(topic)
      fail("Producer connection did not fail with invalid keystore")
    } catch {
      case _:AuthenticationException => // expected exception
    }
  }

  private def describeConfig(adminClient: AdminClient): Config = {
    val configResources = servers.map { server =>
      new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
    }
    val describeOptions = new DescribeConfigsOptions().includeSynonyms(true)
    val describeResult = adminClient.describeConfigs(configResources.asJava, describeOptions).all.get
    assertEquals(servers.size, describeResult.values.size)
    val configDescription = describeResult.values.iterator.next
    assertFalse("Configs are empty", configDescription.entries.isEmpty)
    configDescription
  }

  private def alterSslKeystore(adminClient: AdminClient, props: Properties, listener: String, expectFailure: Boolean  = false): Unit = {
    val newProps = new Properties
    val configPrefix = new ListenerName(listener).configPrefix
    val keystoreLocation = props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)
    newProps.setProperty(s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", keystoreLocation)
    newProps.setProperty(s"$configPrefix$SSL_KEYSTORE_TYPE_CONFIG", props.getProperty(SSL_KEYSTORE_TYPE_CONFIG))
    newProps.setProperty(s"$configPrefix$SSL_KEYSTORE_PASSWORD_CONFIG", props.get(SSL_KEYSTORE_PASSWORD_CONFIG).asInstanceOf[Password].value)
    newProps.setProperty(s"$configPrefix$SSL_KEY_PASSWORD_CONFIG", props.get(SSL_KEY_PASSWORD_CONFIG).asInstanceOf[Password].value)
    reconfigureServers(newProps, perBrokerConfig = true, (s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", keystoreLocation), expectFailure)
  }

  private def alterConfigs(adminClient: AdminClient, props: Properties, perBrokerConfig: Boolean): AlterConfigsResult = {
    val configEntries = props.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = if (perBrokerConfig) {
      servers.map { server =>
        val resource = new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
        (resource, newConfig)
      }.toMap.asJava
    } else {
      Map(new ConfigResource(ConfigResource.Type.BROKER, "") -> newConfig).asJava
    }
    adminClient.alterConfigs(configs)
  }

  private def alterConfigsOnServer(server: KafkaServer, props: Properties): Unit = {
    val configEntries = props.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = Map(new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString) -> newConfig).asJava
    adminClients.head.alterConfigs(configs).all.get
    props.asScala.foreach { case (k, v) => waitForConfigOnServer(server, k, v) }
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String), expectFailure: Boolean = false): Unit = {
    val alterResult = alterConfigs(adminClients.head, newProps, perBrokerConfig)
    if (expectFailure) {
      val oldProps = servers.head.config.values.asScala.filterKeys(newProps.containsKey)
      val brokerResources = if (perBrokerConfig)
        servers.map(server => new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString))
      else
        Seq(new ConfigResource(ConfigResource.Type.BROKER, ""))
      brokerResources.foreach { brokerResource =>
        val exception = intercept[ExecutionException](alterResult.values.get(brokerResource).get)
        assertTrue(exception.getCause.isInstanceOf[InvalidRequestException])
      }
      assertEquals(oldProps, servers.head.config.values.asScala.filterKeys(newProps.containsKey))
    } else {
      alterResult.all.get
      waitForConfig(aPropToVerify._1, aPropToVerify._2)
    }
  }

  private def configEntry(configDesc: Config, configName: String): ConfigEntry = {
    configDesc.entries.asScala.find(cfg => cfg.name == configName)
      .getOrElse(throw new IllegalStateException(s"Config not found $configName"))
  }

  private def addKeystoreWithListenerPrefix(srcProps: Properties, destProps: Properties, listener: String): Unit = {
    val listenerPrefix = new ListenerName(listener).configPrefix
    destProps.put(listenerPrefix + SSL_KEYSTORE_TYPE_CONFIG, srcProps.get(SSL_KEYSTORE_TYPE_CONFIG))
    destProps.put(listenerPrefix + SSL_KEYSTORE_LOCATION_CONFIG, srcProps.get(SSL_KEYSTORE_LOCATION_CONFIG))
    destProps.put(listenerPrefix + SSL_KEYSTORE_PASSWORD_CONFIG, srcProps.get(SSL_KEYSTORE_PASSWORD_CONFIG).asInstanceOf[Password].value)
    destProps.put(listenerPrefix + SSL_KEY_PASSWORD_CONFIG, srcProps.get(SSL_KEY_PASSWORD_CONFIG).asInstanceOf[Password].value)
  }

  private def configureDynamicKeystoreInZooKeeper(kafkaConfig: KafkaConfig, brokers: Seq[Int], sslProperties: Properties): Unit = {
    val keystoreProps = new Properties
    addKeystoreWithListenerPrefix(sslProperties, keystoreProps, SecureExternal)
    kafkaConfig.dynamicConfig.toPersistentProps(keystoreProps, perBrokerConfig = true)
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    adminZkClient.changeBrokerConfig(brokers, keystoreProps)
  }

  private def waitForKeystore(sslProperties: Properties, maxWaitMs: Long = 10000): Unit = {
    waitForConfig(new ListenerName(SecureExternal).configPrefix + SSL_KEYSTORE_LOCATION_CONFIG,
      sslProperties.getProperty(SSL_KEYSTORE_LOCATION_CONFIG), maxWaitMs)

  }

  private def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server => waitForConfigOnServer(server, propName, propValue, maxWaitMs) }
  }

  private def waitForConfigOnServer(server: KafkaServer, propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
    }
  }

  private def invalidSslConfigs: Properties = {
    val props = new Properties
    props.put(SSL_KEYSTORE_LOCATION_CONFIG, "invalid/file/path")
    props.put(SSL_KEYSTORE_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEY_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
    props
  }

  private def currentThreads: List[String] = {
    Thread.getAllStackTraces.keySet.asScala.toList.map(_.getName)
  }

  private def verifyThreads(threadPrefix: String, countPerBroker: Int): Unit = {
    val expectedCount = countPerBroker * servers.size
    val (threads, resized) = TestUtils.computeUntilTrue(currentThreads.filter(_.startsWith(threadPrefix))) {
      _.size == expectedCount
    }
    assertTrue(s"Invalid threads: expected $expectedCount, got ${threads.size}: $threads", resized)
  }


  private def startProduceConsume(retries: Int): (ProducerThread, ConsumerThread) = {
    val producerThread = new ProducerThread(retries)
    clientThreads += producerThread
    val consumerThread = new ConsumerThread(producerThread)
    clientThreads += consumerThread
    consumerThread.start()
    producerThread.start()
    (producerThread, consumerThread)
  }

  private def stopAndVerifyProduceConsume(producerThread: ProducerThread, consumerThread: ConsumerThread,
                                                                                   mayFailRequests: Boolean): Unit = {
    producerThread.shutdown()
    consumerThread.initiateShutdown()
    consumerThread.awaitShutdown()
    if (!mayFailRequests)
      assertEquals(producerThread.sent, consumerThread.received)
    else {
      assertTrue(s"Some messages not received, sent=${producerThread.sent} received=${consumerThread.received}",
        consumerThread.received >= producerThread.sent)
    }
  }

  private class ProducerThread(retries: Int) extends ShutdownableThread("test-producer", isInterruptible = false) {
    private val producer = createProducer(trustStoreFile1, retries)
    @volatile var sent = 0
    override def doWork(): Unit = {
        try {
            while (isRunning) {
                sent += 1
                val record = new ProducerRecord(topic, s"key$sent", s"value$sent")
                producer.send(record).get(10, TimeUnit.SECONDS)
              }
          } finally {
            producer.close()
          }
      }
  }

  private class ConsumerThread(producerThread: ProducerThread) extends ShutdownableThread("test-consumer", isInterruptible = false) {
    private val consumer = createConsumer("group1", trustStoreFile1)
    @volatile var lastBatch: ConsumerRecords[String, String] = _
    @volatile private var endTimeMs = Long.MaxValue
    var received = 0
    override def doWork(): Unit = {
      try {
        while (isRunning || (received < producerThread.sent && System.currentTimeMillis < endTimeMs)) {
          val records = consumer.poll(50)
          received += records.count
          if (!records.isEmpty)
            lastBatch = records
        }
      } finally {
        consumer.close()
      }
    }

    override def initiateShutdown(): Boolean = {
      endTimeMs = System.currentTimeMillis + 10 * 1000
      super.initiateShutdown()
    }

    def waitForMatchingRecords(predicate: ConsumerRecord[String, String] => Boolean): Unit = {
      TestUtils.waitUntilTrue(() => {
        val records = lastBatch
        if (records == null || records.isEmpty)
          false
        else
          records.asScala.toList.exists(predicate)
      }, "Received records did not match")
    }
  }
}
