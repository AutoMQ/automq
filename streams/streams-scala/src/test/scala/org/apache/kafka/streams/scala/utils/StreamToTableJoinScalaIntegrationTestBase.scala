/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.streams._
import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.test.{IntegrationTest, TestUtils}
import org.junit._
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder

/**
 * Test suite base that prepares Kafka cluster for stream-table joins in Kafka Streams
 * <p>
 */
@Category(Array(classOf[IntegrationTest]))
class StreamToTableJoinScalaIntegrationTestBase extends StreamToTableJoinTestData {

  private val privateCluster: EmbeddedKafkaCluster = new EmbeddedKafkaCluster(1)

  @Rule def cluster: EmbeddedKafkaCluster = privateCluster

  final val alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000
  val mockTime: MockTime = cluster.time
  mockTime.setCurrentTimeMs(alignedTime)

  val tFolder: TemporaryFolder = new TemporaryFolder(TestUtils.tempDirectory())
  @Rule def testFolder: TemporaryFolder = tFolder

  @Before
  def startKafkaCluster(): Unit = {
    cluster.createTopic(userClicksTopic)
    cluster.createTopic(userRegionsTopic)
    cluster.createTopic(outputTopic)
    cluster.createTopic(userClicksTopicJ)
    cluster.createTopic(userRegionsTopicJ)
    cluster.createTopic(outputTopicJ)
  }

  def getStreamsConfiguration(): Properties = {
    val streamsConfiguration: Properties = new Properties()

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-scala-integration-test")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot.getPath)

    streamsConfiguration
  }

  private def getUserRegionsProducerConfig(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p
  }

  private def getUserClicksProducerConfig(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    p
  }

  private def getConsumerConfig(): Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "join-scala-integration-test-standard-consumer")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
    p
  }

  def produceNConsume(userClicksTopic: String,
                      userRegionsTopic: String,
                      outputTopic: String,
                      waitTillRecordsReceived: Boolean = true): java.util.List[KeyValue[String, Long]] = {

    import _root_.scala.jdk.CollectionConverters._

    // Publish user-region information.
    val userRegionsProducerConfig: Properties = getUserRegionsProducerConfig()
    IntegrationTestUtils.produceKeyValuesSynchronously(userRegionsTopic,
                                                       userRegions.asJava,
                                                       userRegionsProducerConfig,
                                                       mockTime,
                                                       false)

    // Publish user-click information.
    val userClicksProducerConfig: Properties = getUserClicksProducerConfig()
    IntegrationTestUtils.produceKeyValuesSynchronously(userClicksTopic,
                                                       userClicks.asJava,
                                                       userClicksProducerConfig,
                                                       mockTime,
                                                       false)

    if (waitTillRecordsReceived) {
      // consume and verify result
      val consumerConfig = getConsumerConfig()

      IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig,
                                                                 outputTopic,
                                                                 expectedClicksPerRegion.asJava)
    } else {
      java.util.Collections.emptyList()
    }
  }
}
