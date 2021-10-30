/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import java.lang.management.ManagementFactory
import java.util.Properties

import javax.management.ObjectName
import com.yammer.metrics.core.MetricPredicate
import org.junit.jupiter.api.Assertions._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils._

import scala.collection._
import scala.jdk.CollectionConverters._
import kafka.log.LogConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

@Timeout(120)
class MetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numParts = 2

  val requiredKafkaServerPrefix = "kafka.server:type=KafkaServer,name"
  val overridingProps = new Properties
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)
  overridingProps.put(JmxReporter.EXCLUDE_CONFIG, s"$requiredKafkaServerPrefix=ClusterId")

  def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(numNodes, zkConnectOrNull, enableControlledShutdown = false).
      map(KafkaConfig.fromProps(_, overridingProps))

  val nMessages = 2

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testMetricsReporterAfterDeletingTopic(quorum: String): Unit = {
    val topic = "test-topic-metric"
    createTopic(topic, 1, 1)
    deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
    assertEquals(Set.empty, topicMetricGroups(topic), "Topic metrics exists after deleteTopic")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testBrokerTopicMetricsUnregisteredAfterDeletingTopic(quorum: String): Unit = {
    val topic = "test-broker-topic-metric"
    createTopic(topic, 2, 1)
    // Produce a few messages to create the metrics
    // Don't consume messages as it may cause metrics to be re-created causing the test to fail, see KAFKA-5238
    TestUtils.generateAndProduceMessages(brokers, topic, nMessages)
    assertTrue(topicMetricGroups(topic).nonEmpty, "Topic metrics don't exist")
    brokers.foreach(b => assertNotNull(b.brokerTopicStats.topicStats(topic)))
    deleteTopic(topic)
    TestUtils.verifyTopicDeletion(zkClientOrNull, topic, 1, brokers)
    assertEquals(Set.empty, topicMetricGroups(topic), "Topic metrics exists after deleteTopic")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testClusterIdMetric(quorum: String): Unit = {
    // Check if clusterId metric exists.
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == s"$requiredKafkaServerPrefix=ClusterId"), 1)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testBrokerStateMetric(quorum: String): Unit = {
    // Check if BrokerState metric exists.
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == s"$requiredKafkaServerPrefix=BrokerState"), 1)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testYammerMetricsCountMetric(quorum: String): Unit = {
    // Check if yammer-metrics-count metric exists.
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == s"$requiredKafkaServerPrefix=yammer-metrics-count"), 1)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testLinuxIoMetrics(quorum: String): Unit = {
    // Check if linux-disk-{read,write}-bytes metrics either do or do not exist depending on whether we are or are not
    // able to collect those metrics on the platform where this test is running.
    val usable = new LinuxIoMetricsCollector("/proc", Time.SYSTEM, logger.underlying).usable()
    val expectedCount = if (usable) 1 else 0
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    Set("linux-disk-read-bytes", "linux-disk-write-bytes").foreach(name =>
      assertEquals(metrics.keySet.asScala.count(_.getMBeanName == s"$requiredKafkaServerPrefix=$name"), expectedCount))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testJMXFilter(quorum: String): Unit = {
    // Check if cluster id metrics is not exposed in JMX
    assertTrue(ManagementFactory.getPlatformMBeanServer
                 .isRegistered(new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount")))
    assertFalse(ManagementFactory.getPlatformMBeanServer
                  .isRegistered(new ObjectName(s"$requiredKafkaServerPrefix=ClusterId")))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testUpdateJMXFilter(quorum: String): Unit = {
    // verify previously exposed metrics are removed and existing matching metrics are added
    brokers.foreach(broker => broker.kafkaYammerMetrics.reconfigure(
      Map(JmxReporter.EXCLUDE_CONFIG -> "kafka.controller:type=KafkaController,name=ActiveControllerCount").asJava
    ))
    assertFalse(ManagementFactory.getPlatformMBeanServer
                 .isRegistered(new ObjectName("kafka.controller:type=KafkaController,name=ActiveControllerCount")))
    assertTrue(ManagementFactory.getPlatformMBeanServer
                  .isRegistered(new ObjectName(s"$requiredKafkaServerPrefix=ClusterId")))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testGeneralBrokerTopicMetricsAreGreedilyRegistered(quorum: String): Unit = {
    val topic = "test-broker-topic-metric"
    createTopic(topic, 2, 1)

    // The broker metrics for all topics should be greedily registered
    assertTrue(topicMetrics(None).nonEmpty, "General topic metrics don't exist")
    assertEquals(brokers.head.brokerTopicStats.allTopicsStats.metricMap.size, topicMetrics(None).size)
    // topic metrics should be lazily registered
    assertTrue(topicMetricGroups(topic).isEmpty, "Topic metrics aren't lazily registered")
    TestUtils.generateAndProduceMessages(brokers, topic, nMessages)
    assertTrue(topicMetricGroups(topic).nonEmpty, "Topic metrics aren't registered")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testWindowsStyleTagNames(quorum: String): Unit = {
    val path = "C:\\windows-path\\kafka-logs"
    val tags = Map("dir" -> path)
    val expectedMBeanName = Set(tags.keySet.head, ObjectName.quote(path)).mkString("=")
    val metric = KafkaMetricsGroup.metricName("test-metric", tags)
    assert(metric.getMBeanName.endsWith(expectedMBeanName))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testBrokerTopicMetricsBytesInOut(quorum: String): Unit = {
    val topic = "test-bytes-in-out"
    val replicationBytesIn = BrokerTopicStats.ReplicationBytesInPerSec
    val replicationBytesOut = BrokerTopicStats.ReplicationBytesOutPerSec
    val bytesIn = s"${BrokerTopicStats.BytesInPerSec},topic=$topic"
    val bytesOut = s"${BrokerTopicStats.BytesOutPerSec},topic=$topic"

    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, "2")
    createTopic(topic, 1, numNodes, topicConfig)
    // Produce a few messages to create the metrics
    TestUtils.generateAndProduceMessages(brokers, topic, nMessages)

    // Check the log size for each broker so that we can distinguish between failures caused by replication issues
    // versus failures caused by the metrics
    val topicPartition = new TopicPartition(topic, 0)
    brokers.foreach { broker =>
      val log = broker.logManager.getLog(new TopicPartition(topic, 0))
      val brokerId = broker.config.brokerId
      val logSize = log.map(_.size)
      assertTrue(logSize.exists(_ > 0), s"Expected broker $brokerId to have a Log for $topicPartition with positive size, actual: $logSize")
    }

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(brokers, topic, nMessages)
    val initialReplicationBytesIn = TestUtils.meterCount(replicationBytesIn)
    val initialReplicationBytesOut = TestUtils.meterCount(replicationBytesOut)
    val initialBytesIn = TestUtils.meterCount(bytesIn)
    val initialBytesOut = TestUtils.meterCount(bytesOut)

    // BytesOut doesn't include replication, so it shouldn't have changed
    assertEquals(initialBytesOut, TestUtils.meterCount(bytesOut))

    // Produce a few messages to make the metrics tick
    TestUtils.generateAndProduceMessages(brokers, topic, nMessages)

    assertTrue(TestUtils.meterCount(replicationBytesIn) > initialReplicationBytesIn)
    assertTrue(TestUtils.meterCount(replicationBytesOut) > initialReplicationBytesOut)
    assertTrue(TestUtils.meterCount(bytesIn) > initialBytesIn)

    // Consume messages to make bytesOut tick
    TestUtils.consumeTopicRecords(brokers, topic, nMessages)

    assertTrue(TestUtils.meterCount(bytesOut) > initialBytesOut)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk"))
  def testZkControllerMetrics(quorum: String): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics

    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ActiveControllerCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=OfflinePartitionsCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=GlobalTopicCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=GlobalPartitionCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=TopicsToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ReplicasToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=TopicsIneligibleToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ReplicasIneligibleToDeleteCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=ActiveBrokerCount"), 1)
    assertEquals(metrics.keySet.asScala.count(_.getMBeanName == "kafka.controller:type=KafkaController,name=FencedBrokerCount"), 1)
  }

  /**
   * Test that the metrics are created with the right name, testZooKeeperStateChangeRateMetrics
   * and testZooKeeperSessionStateMetric in ZooKeeperClientTest test the metrics behaviour.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testSessionExpireListenerMetrics(quorum: String): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry.allMetrics
    val expectedNumMetrics = if (isKRaftTest()) 0 else 1
    assertEquals(expectedNumMetrics, metrics.keySet.asScala.
      count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=SessionState"))
    assertEquals(expectedNumMetrics, metrics.keySet.asScala.
      count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=ZooKeeperExpiresPerSec"))
    assertEquals(expectedNumMetrics, metrics.keySet.asScala.
      count(_.getMBeanName == "kafka.server:type=SessionExpireListener,name=ZooKeeperDisconnectsPerSec"))
  }

  private def topicMetrics(topic: Option[String]): Set[String] = {
    val metricNames = KafkaYammerMetrics.defaultRegistry.allMetrics().keySet.asScala.map(_.getMBeanName)
    filterByTopicMetricRegex(metricNames, topic)
  }

  private def topicMetricGroups(topic: String): Set[String] = {
    val metricGroups = KafkaYammerMetrics.defaultRegistry.groupedMetrics(MetricPredicate.ALL).keySet.asScala
    filterByTopicMetricRegex(metricGroups, Some(topic))
  }

  private def filterByTopicMetricRegex(metrics: Set[String], topic: Option[String]): Set[String] = {
    val pattern = (".*BrokerTopicMetrics.*" + topic.map(t => s"($t)$$").getOrElse("")).r.pattern
    metrics.filter(pattern.matcher(_).matches())
  }
}
