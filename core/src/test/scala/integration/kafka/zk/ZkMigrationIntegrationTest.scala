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
package kafka.zk

import kafka.server.{ConfigType, KafkaConfig}
import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.testkit.{KafkaClusterTestKit, TestKitNodes}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AlterClientQuotasResult, AlterConfigOp, AlterConfigsResult, ConfigEntry, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.raft.RaftConfig
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion, ProducerIdsBlock}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertTrue}
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.Seq
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ZkMigrationIntegrationTest {

  val log = LoggerFactory.getLogger(classOf[ZkMigrationIntegrationTest])

  class MetadataDeltaVerifier {
    val metadataDelta = new MetadataDelta(MetadataImage.EMPTY)
    var offset = 0
    def accept(batch: java.util.List[ApiMessageAndVersion]): Unit = {
      batch.forEach(message => {
        metadataDelta.replay(message.message())
        offset += 1
      })
    }

    def verify(verifier: MetadataImage => Unit): Unit = {
      val image = metadataDelta.apply(new MetadataProvenance(offset, 0, 0))
      verifier.apply(image)
    }
  }

  @ClusterTest(brokers = 3, clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_4_IV0)
  def testMigrate(clusterInstance: ClusterInstance): Unit = {
    val admin = clusterInstance.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic("test-topic-1", 2, 3.toShort)
      .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
    newTopics.add(new NewTopic("test-topic-2", 1, 3.toShort))
    newTopics.add(new NewTopic("test-topic-3", 10, 3.toShort))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(60, TimeUnit.SECONDS)

    val quotas = new util.ArrayList[ClientQuotaAlteration]()
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1", "client-id" -> "clientA").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("ip" -> "8.8.8.8").asJava),
      List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
    admin.alterClientQuotas(quotas)

    val zkClient = clusterInstance.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val migrationClient = new ZkMigrationClient(zkClient)
    var migrationState = migrationClient.getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState.EMPTY)
    migrationState = migrationState.withNewKRaftController(3000, 42)
    migrationState = migrationClient.claimControllerLeadership(migrationState)

    val brokers = new java.util.HashSet[Integer]()
    val verifier = new MetadataDeltaVerifier()
    migrationClient.readAllMetadata(batch => verifier.accept(batch), brokerId => brokers.add(brokerId))
    assertEquals(Seq(0, 1, 2), brokers.asScala.toSeq)

    verifier.verify { image =>
      assertNotNull(image.topics().getTopic("test-topic-1"))
      assertEquals(2, image.topics().getTopic("test-topic-1").partitions().size())

      assertNotNull(image.topics().getTopic("test-topic-2"))
      assertEquals(1, image.topics().getTopic("test-topic-2").partitions().size())

      assertNotNull(image.topics().getTopic("test-topic-3"))
      assertEquals(10, image.topics().getTopic("test-topic-3").partitions().size())

      val clientQuotas = image.clientQuotas().entities()
      assertEquals(3, clientQuotas.size())
    }

    migrationState = migrationClient.releaseControllerLeadership(migrationState)
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 3, metadataVersion = MetadataVersion.IBP_3_4_IV0, serverProperties = Array(
    new ClusterConfigProperty(key = "inter.broker.listener.name", value = "EXTERNAL"),
    new ClusterConfigProperty(key = "listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "advertised.listeners", value = "PLAINTEXT://localhost:0,EXTERNAL://localhost:0"),
    new ClusterConfigProperty(key = "listener.security.protocol.map", value = "EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT"),
  ))
  def testDualWrite(zkCluster: ClusterInstance): Unit = {
    // Create a topic in ZK mode
    var admin = zkCluster.createAdminClient()
    val newTopics = new util.ArrayList[NewTopic]()
    newTopics.add(new NewTopic("test", 2, 3.toShort)
      .configs(Map(TopicConfig.SEGMENT_BYTES_CONFIG -> "102400", TopicConfig.SEGMENT_MS_CONFIG -> "300000").asJava))
    val createTopicResult = admin.createTopics(newTopics)
    createTopicResult.all().get(60, TimeUnit.SECONDS)
    admin.close()

    // Verify the configs exist in ZK
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val propsBefore = zkClient.getEntityConfigs(ConfigType.Topic, "test")
    assertEquals("102400", propsBefore.getProperty(TopicConfig.SEGMENT_BYTES_CONFIG))
    assertEquals("300000", propsBefore.getProperty(TopicConfig.SEGMENT_MS_CONFIG))

    // Bootstrap the ZK cluster ID into KRaft
    val clusterId = zkCluster.clusterId()
    val kraftCluster = new KafkaClusterTestKit.Builder(
      new TestKitNodes.Builder().
        setBootstrapMetadataVersion(MetadataVersion.IBP_3_4_IV0).
        setClusterId(Uuid.fromString(clusterId)).
        setNumBrokerNodes(0).
        setNumControllerNodes(1).build())
      .setConfigProp(KafkaConfig.MigrationEnabledProp, "true")
      .setConfigProp(KafkaConfig.ZkConnectProp, zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect)
      .build()
    try {
      kraftCluster.format()
      kraftCluster.startup()
      val readyFuture = kraftCluster.controllers().values().asScala.head.controller.waitForReadyBrokers(3)

      // Allocate a transactional producer ID while in ZK mode
      allocateProducerId(zkCluster.bootstrapServers())
      val producerIdBlock = readProducerIdBlock(zkClient)

      // Enable migration configs and restart brokers
      log.info("Restart brokers in migration mode")
      val clientProps = kraftCluster.controllerClientProperties()
      val voters = clientProps.get(RaftConfig.QUORUM_VOTERS_CONFIG)
      zkCluster.config().serverProperties().put(KafkaConfig.MigrationEnabledProp, "true")
      zkCluster.config().serverProperties().put(RaftConfig.QUORUM_VOTERS_CONFIG, voters)
      zkCluster.config().serverProperties().put(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
      zkCluster.config().serverProperties().put(KafkaConfig.ListenerSecurityProtocolMapProp, "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT")
      zkCluster.rollingBrokerRestart()
      zkCluster.waitForReadyBrokers()
      readyFuture.get(30, TimeUnit.SECONDS)

      // Wait for migration to begin
      log.info("Waiting for ZK migration to begin")
      TestUtils.waitUntilTrue(() => zkClient.getControllerId.contains(3000), "Timed out waiting for KRaft controller to take over")

      // Alter the metadata
      log.info("Updating metadata with AdminClient")
      admin = zkCluster.createAdminClient()
      alterTopicConfig(admin).all().get(60, TimeUnit.SECONDS)
      alterClientQuotas(admin).all().get(60, TimeUnit.SECONDS)

      // Verify the changes made to KRaft are seen in ZK
      log.info("Verifying metadata changes with ZK")
      verifyTopicConfigs(zkClient)
      verifyClientQuotas(zkClient)
      allocateProducerId(zkCluster.bootstrapServers())
      verifyProducerId(producerIdBlock, zkClient)

    } finally {
      zkCluster.stop()
      kraftCluster.close()
    }
  }

  def allocateProducerId(bootstrapServers: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("transactional.id", "some-transaction-id")
    val producer = new KafkaProducer[String, String](props, new StringSerializer(), new StringSerializer())
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(new ProducerRecord[String, String]("test", "", "one"))
    producer.commitTransaction()
    producer.flush()
    producer.close()
  }

  def readProducerIdBlock(zkClient: KafkaZkClient): ProducerIdsBlock = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt.map(ProducerIdBlockZNode.parseProducerIdBlockData).get
  }

  def alterTopicConfig(admin: Admin): AlterConfigsResult = {
    val topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "test")
    val alterConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_BYTES_CONFIG, "204800"), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(TopicConfig.SEGMENT_MS_CONFIG, null), AlterConfigOp.OpType.DELETE)
    ).asJavaCollection
    admin.incrementalAlterConfigs(Map(topicResource -> alterConfigs).asJava)
  }

  def alterClientQuotas(admin: Admin): AlterClientQuotasResult = {
    val quotas = new util.ArrayList[ClientQuotaAlteration]()
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("user" -> "user1", "client-id" -> "clientA").asJava),
      List(new ClientQuotaAlteration.Op("consumer_byte_rate", 800.0), new ClientQuotaAlteration.Op("producer_byte_rate", 100.0)).asJava))
    quotas.add(new ClientQuotaAlteration(
      new ClientQuotaEntity(Map("ip" -> "8.8.8.8").asJava),
      List(new ClientQuotaAlteration.Op("connection_creation_rate", 10.0)).asJava))
    admin.alterClientQuotas(quotas)
  }

  def verifyTopicConfigs(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val propsAfter = zkClient.getEntityConfigs(ConfigType.Topic, "test")
      assertEquals("204800", propsAfter.getProperty(TopicConfig.SEGMENT_BYTES_CONFIG))
      assertFalse(propsAfter.containsKey(TopicConfig.SEGMENT_MS_CONFIG))
    }
  }

  def verifyClientQuotas(zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      assertEquals("1000.0", zkClient.getEntityConfigs(ConfigType.User, "user1").getProperty("consumer_byte_rate"))
      assertEquals("800.0", zkClient.getEntityConfigs("users/user1/clients", "clientA").getProperty("consumer_byte_rate"))
      assertEquals("100.0", zkClient.getEntityConfigs("users/user1/clients", "clientA").getProperty("producer_byte_rate"))
      assertEquals("10.0", zkClient.getEntityConfigs(ConfigType.Ip, "8.8.8.8").getProperty("connection_creation_rate"))
    }
  }

  def verifyProducerId(firstProducerIdBlock: ProducerIdsBlock, zkClient: KafkaZkClient): Unit = {
    TestUtils.retry(10000) {
      val producerIdBlock = readProducerIdBlock(zkClient)
      assertTrue(firstProducerIdBlock.firstProducerId() < producerIdBlock.firstProducerId())
    }
  }
}
