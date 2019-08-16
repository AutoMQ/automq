/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.util.{Locale, Properties}

import kafka.log.LogConfig
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{JaasTestUtils, TestUtils}
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Gauge, Histogram, Meter}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{Metric, MetricName, TopicPartition}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.scalatest.Assertions.fail

import scala.collection.JavaConverters._

class MetricsTest extends IntegrationTestHarness with SaslSetup {

  override val brokerCount = 1

  override protected def listenerName = new ListenerName("CLIENT")
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaServerJaasEntryName =
    s"${listenerName.value.toLowerCase(Locale.ROOT)}.${JaasTestUtils.KafkaServerContextName}"
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "false")
  this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableDoc, "false")
  this.producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10")
  // intentionally slow message down conversion via gzip compression to ensure we can measure the time it takes
  this.producerConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties =
    Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties =
    Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @Before
  override def setUp(): Unit = {
    verifyNoRequestMetrics("Request metrics not removed in a previous test")
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, kafkaServerJaasEntryName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
    verifyNoRequestMetrics("Request metrics not removed in this test")
  }

  /**
   * Verifies some of the metrics of producer, consumer as well as server.
   */
  @Test
  def testMetrics(): Unit = {
    val topic = "topicWithOldMessageFormat"
    val props = new Properties
    props.setProperty(LogConfig.MessageFormatVersionProp, "0.9.0")
    createTopic(topic, numPartitions = 1, replicationFactor = 1, props)
    val tp = new TopicPartition(topic, 0)

    // Produce and consume some records
    val numRecords = 10
    val recordSize = 100000
    val producer = createProducer()
    sendRecords(producer, numRecords, recordSize, tp)

    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    TestUtils.consumeRecords(consumer, numRecords)

    verifyKafkaRateMetricsHaveCumulativeCount(producer, consumer)
    verifyClientVersionMetrics(consumer.metrics, "Consumer")
    verifyClientVersionMetrics(producer.metrics, "Producer")

    val server = servers.head
    verifyBrokerMessageConversionMetrics(server, recordSize, tp)
    verifyBrokerErrorMetrics(servers.head)
    verifyBrokerZkMetrics(server, topic)

    generateAuthenticationFailure(tp)
    verifyBrokerAuthenticationMetrics(server)
  }

  private def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
      recordSize: Int, tp: TopicPartition) = {
    val bytes = new Array[Byte](recordSize)
    (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(tp.topic, tp.partition, i.toLong, s"key $i".getBytes, bytes))
    }
    producer.flush()
  }

  // Create a producer that fails authentication to verify authentication failure metrics
  private def generateAuthenticationFailure(tp: TopicPartition): Unit = {
    val saslProps = new Properties()
     // Temporary limit to reduce blocking before KIP-152 client-side changes are merged
    saslProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
    // Use acks=0 to verify error metric when connection is closed without a response
    val producer = TestUtils.createProducer(brokerList,
      acks = 0,
      requestTimeoutMs = 1000,
      maxBlockMs = 1000,
      securityProtocol = securityProtocol,
      trustStoreFile = trustStoreFile,
      saslProperties = Some(saslProps))

    try {
      producer.send(new ProducerRecord(tp.topic, tp.partition, "key".getBytes, "value".getBytes)).get
    } catch {
      case _: Exception => // expected exception
    } finally {
      producer.close()
    }
  }

  private def verifyKafkaRateMetricsHaveCumulativeCount(producer: KafkaProducer[Array[Byte], Array[Byte]],
                                                        consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {

    def exists(name: String, rateMetricName: MetricName, allMetricNames: Set[MetricName]): Boolean = {
      allMetricNames.contains(new MetricName(name, rateMetricName.group, "", rateMetricName.tags))
    }

    def verify(rateMetricName: MetricName, allMetricNames: Set[MetricName]): Unit = {
      val name = rateMetricName.name
      val totalExists = exists(name.replace("-rate", "-total"), rateMetricName, allMetricNames)
      val totalTimeExists = exists(name.replace("-rate", "-time"), rateMetricName, allMetricNames)
      assertTrue(s"No cumulative count/time metric for rate metric $rateMetricName",
          totalExists || totalTimeExists)
    }

    val consumerMetricNames = consumer.metrics.keySet.asScala.toSet
    consumerMetricNames.filter(_.name.endsWith("-rate"))
        .foreach(verify(_, consumerMetricNames))

    val producerMetricNames = producer.metrics.keySet.asScala.toSet
    val producerExclusions = Set("compression-rate") // compression-rate is an Average metric, not Rate
    producerMetricNames.filter(_.name.endsWith("-rate"))
        .filterNot(metricName => producerExclusions.contains(metricName.name))
        .foreach(verify(_, producerMetricNames))

    // Check a couple of metrics of consumer and producer to ensure that values are set
    verifyKafkaMetricRecorded("records-consumed-rate", consumer.metrics, "Consumer")
    verifyKafkaMetricRecorded("records-consumed-total", consumer.metrics, "Consumer")
    verifyKafkaMetricRecorded("record-send-rate", producer.metrics, "Producer")
    verifyKafkaMetricRecorded("record-send-total", producer.metrics, "Producer")
  }

  private def verifyClientVersionMetrics(metrics: java.util.Map[MetricName, _ <: Metric], entity: String): Unit = {
    Seq("commit-id", "version").foreach { name =>
      verifyKafkaMetric(name, metrics, entity) { matchingMetrics =>
        assertEquals(1, matchingMetrics.size)
        val metric = matchingMetrics.head
        val value = metric.metricValue
        assertNotNull(s"$entity metric not recorded $name", value)
        assertNotNull(s"$entity metric $name should be a non-empty String",
            value.isInstanceOf[String] && !value.asInstanceOf[String].isEmpty)
        assertTrue("Client-id not specified", metric.metricName.tags.containsKey("client-id"))
      }
    }
  }

  private def verifyBrokerAuthenticationMetrics(server: KafkaServer): Unit = {
    val metrics = server.metrics.metrics
    TestUtils.waitUntilTrue(() =>
      maxKafkaMetricValue("failed-authentication-total", metrics, "Broker", Some("socket-server-metrics")) > 0,
      "failed-authentication-total not updated")
    verifyKafkaMetricRecorded("successful-authentication-rate", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("successful-authentication-total", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("failed-authentication-rate", metrics, "Broker", Some("socket-server-metrics"))
    verifyKafkaMetricRecorded("failed-authentication-total", metrics, "Broker", Some("socket-server-metrics"))
  }

  private def verifyBrokerMessageConversionMetrics(server: KafkaServer, recordSize: Int, tp: TopicPartition): Unit = {
    val requestMetricsPrefix = "kafka.network:type=RequestMetrics"
    val requestBytes = verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=RequestBytes,request=Produce")
    val tempBytes = verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=TemporaryMemoryBytes,request=Produce")
    assertTrue(s"Unexpected temporary memory size requestBytes $requestBytes tempBytes $tempBytes",
        tempBytes >= recordSize)

    verifyYammerMetricRecorded(s"kafka.server:type=BrokerTopicMetrics,name=ProduceMessageConversionsPerSec")
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=MessageConversionsTimeMs,request=Produce", value => value > 0.0)
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=RequestBytes,request=Fetch")
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=TemporaryMemoryBytes,request=Fetch", value => value == 0.0)

     // request size recorded for all request types, check one
    verifyYammerMetricRecorded(s"$requestMetricsPrefix,name=RequestBytes,request=Metadata")
  }

  private def verifyBrokerZkMetrics(server: KafkaServer, topic: String): Unit = {
    val histogram = yammerHistogram("kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs")
    // Latency is rounded to milliseconds, so check the count instead
    val initialCount = histogram.count
    servers.head.zkClient.getLeaderForPartition(new TopicPartition(topic, 0))
    val newCount = histogram.count
    assertTrue("ZooKeeper latency not recorded",  newCount > initialCount)

    val min = histogram.min
    assertTrue(s"Min latency should not be negative: $min", min >= 0)

    assertEquals(s"Unexpected ZK state", "CONNECTED", yammerMetricValue("SessionState"))
  }

  private def verifyBrokerErrorMetrics(server: KafkaServer): Unit = {

    def errorMetricCount = Metrics.defaultRegistry.allMetrics.keySet.asScala.filter(_.getName == "ErrorsPerSec").size

    val startErrorMetricCount = errorMetricCount
    val errorMetricPrefix = "kafka.network:type=RequestMetrics,name=ErrorsPerSec"
    verifyYammerMetricRecorded(s"$errorMetricPrefix,request=Metadata,error=NONE")

    try {
      val consumer = createConsumer()
      consumer.partitionsFor("12{}!")
    } catch {
      case _: InvalidTopicException => // expected
    }
    verifyYammerMetricRecorded(s"$errorMetricPrefix,request=Metadata,error=INVALID_TOPIC_EXCEPTION")

    // Check that error metrics are registered dynamically
    val currentErrorMetricCount = errorMetricCount
    assertEquals(startErrorMetricCount + 1, currentErrorMetricCount)
    assertTrue(s"Too many error metrics $currentErrorMetricCount" , currentErrorMetricCount < 10)

    // Verify that error metric is updated with producer acks=0 when no response is sent
    val producer = createProducer()
    sendRecords(producer, numRecords = 1, recordSize = 100, new TopicPartition("non-existent", 0))
    verifyYammerMetricRecorded(s"$errorMetricPrefix,request=Metadata,error=LEADER_NOT_AVAILABLE")
  }

  private def verifyKafkaMetric[T](name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None)(verify: Iterable[Metric] => T) : T = {
    val matchingMetrics = metrics.asScala.filter {
      case (metricName, _) => metricName.name == name && group.forall(_ == metricName.group)
    }
    assertTrue(s"Metric not found $name", matchingMetrics.nonEmpty)
    verify(matchingMetrics.values)
  }

  private def maxKafkaMetricValue(name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String]): Double = {
    // Use max value of all matching metrics since Selector metrics are recorded for each Processor
    verifyKafkaMetric(name, metrics, entity, group) { matchingMetrics =>
      matchingMetrics.foldLeft(0.0)((max, metric) => Math.max(max, metric.metricValue.asInstanceOf[Double]))
    }
  }

  private def verifyKafkaMetricRecorded(name: String, metrics: java.util.Map[MetricName, _ <: Metric], entity: String,
      group: Option[String] = None): Unit = {
    val value = maxKafkaMetricValue(name, metrics, entity, group)
    assertTrue(s"$entity metric not recorded correctly for $name value $value", value > 0.0)
  }

  private def yammerMetricValue(name: String): Any = {
    val allMetrics = Metrics.defaultRegistry.allMetrics.asScala
    val (_, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}"))
    metric match {
      case m: Meter => m.count.toDouble
      case m: Histogram => m.max
      case m: Gauge[_] => m.value
      case m => fail(s"Unexpected broker metric of class ${m.getClass}")
    }
  }

  private def yammerHistogram(name: String): Histogram = {
    val allMetrics = Metrics.defaultRegistry.allMetrics.asScala
    val (_, metric) = allMetrics.find { case (n, _) => n.getMBeanName.endsWith(name) }
      .getOrElse(fail(s"Unable to find broker metric $name: allMetrics: ${allMetrics.keySet.map(_.getMBeanName)}"))
    metric match {
      case m: Histogram => m
      case m => fail(s"Unexpected broker metric of class ${m.getClass}")
    }
  }

  private def verifyYammerMetricRecorded(name: String, verify: Double => Boolean = d => d > 0): Double = {
    val metricValue = yammerMetricValue(name).asInstanceOf[Double]
    assertTrue(s"Broker metric not recorded correctly for $name value $metricValue", verify(metricValue))
    metricValue
  }

  private def verifyNoRequestMetrics(errorMessage: String): Unit = {
    val metrics = Metrics.defaultRegistry.allMetrics.asScala.filter { case (n, _) =>
      n.getMBeanName.startsWith("kafka.network:type=RequestMetrics")
    }
    assertTrue(s"$errorMessage: ${metrics.keys}", metrics.isEmpty)
  }
}
