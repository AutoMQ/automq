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
package kafka.zk.migration

import kafka.utils.CoreUtils
import kafka.server.{ConfigType, KafkaConfig, ZkAdminManager}
import kafka.zk.{AdminZkClient, ZkMigrationClient}
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.metadata.ClientQuotaRecord
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata.ConfigRecord
import org.apache.kafka.common.metadata.UserScramCredentialRecord
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.SecurityUtils
import org.apache.kafka.image.{ClientQuotasDelta, ClientQuotasImage}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.RecordTestUtils
import org.apache.kafka.metadata.migration.KRaftMigrationZkWriter
import org.apache.kafka.metadata.migration.ZkMigrationLeadershipState
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.util.MockRandom
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util
import java.util.Properties
import scala.collection.Map
import scala.jdk.CollectionConverters._

class ZkConfigMigrationClientTest extends ZkMigrationTestHarness {
  def randomBuffer(random: MockRandom, length: Int): Array[Byte] = {
    val buf = new Array[Byte](length)
    random.nextBytes(buf)
    buf
  }

  @Test
  def testMigrationBrokerConfigs(): Unit = {
    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()

    // Create some configs and persist in Zk.
    val props = new Properties()
    props.put(KafkaConfig.DefaultReplicationFactorProp, "1") // normal config
    props.put(KafkaConfig.SslKeystorePasswordProp, encoder.encode(new Password(SECRET))) // sensitive config
    zkClient.setOrCreateEntityConfigs(ConfigType.Broker, "1", props)

    val defaultProps = new Properties()
    defaultProps.put(KafkaConfig.DefaultReplicationFactorProp, "3") // normal config
    zkClient.setOrCreateEntityConfigs(ConfigType.Broker, "<default>", defaultProps)

    migrationClient.migrateBrokerConfigs(batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(1, brokers.size())
    assertEquals(2, batches.size())
    assertEquals(2, batches.get(0).size)
    assertEquals(1, batches.get(1).size)

    batches.get(0).forEach(record => {
      val message = record.message().asInstanceOf[ConfigRecord]
      val name = message.name
      val value = message.value

      assertTrue(props.containsKey(name))
      // If the config is sensitive, compare it to the decoded value.
      if (name == KafkaConfig.SslKeystorePasswordProp) {
        assertEquals(SECRET, value)
      } else {
        assertEquals(props.getProperty(name), value)
      }
    })

    val record = batches.get(1).get(0).message().asInstanceOf[ConfigRecord]
    assertEquals(ConfigResource.Type.BROKER.id(), record.resourceType())
    assertEquals("", record.resourceName())
    assertEquals(KafkaConfig.DefaultReplicationFactorProp, record.name())
    assertEquals("3", record.value())

    // Update the sensitive config value from the config client and check that the value
    // persisted in Zookeeper is encrypted.
    val newProps = new util.HashMap[String, String]()
    newProps.put(KafkaConfig.DefaultReplicationFactorProp, "2") // normal config
    newProps.put(KafkaConfig.SslKeystorePasswordProp, NEW_SECRET) // sensitive config
    migrationState = migrationClient.configClient().writeConfigs(
      new ConfigResource(ConfigResource.Type.BROKER, "1"), newProps, migrationState)
    val actualPropsInZk = zkClient.getEntityConfigs(ConfigType.Broker, "1")
    assertEquals(2, actualPropsInZk.size())
    actualPropsInZk.forEach { case (key, value) =>
      if (key == KafkaConfig.SslKeystorePasswordProp) {
        assertEquals(NEW_SECRET, encoder.decode(value.toString).value)
      } else {
        assertEquals(newProps.get(key), value)
      }
    }

    migrationState = migrationClient.configClient().deleteConfigs(
      new ConfigResource(ConfigResource.Type.BROKER, "1"), migrationState)
    assertEquals(0, zkClient.getEntityConfigs(ConfigType.Broker, "1").size())
  }

  @Test
  def testMigrateClientQuotas(): Unit = {
    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "100000")
    adminZkClient.changeConfigs(ConfigType.User, "<default>", props)
    adminZkClient.changeConfigs(ConfigType.User, "user1", props)
    adminZkClient.changeConfigs(ConfigType.User, "user1/clients/clientA", props)
    adminZkClient.changeConfigs(ConfigType.User, "<default>/clients/<default>", props)
    adminZkClient.changeConfigs(ConfigType.User, "<default>/clients/clientA", props)
    adminZkClient.changeConfigs(ConfigType.Client, "<default>", props)
    adminZkClient.changeConfigs(ConfigType.Client, "clientB", props)
    props.remove(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG)
    props.put(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "10")
    adminZkClient.changeConfigs(ConfigType.Ip, "1.1.1.1", props)
    adminZkClient.changeConfigs(ConfigType.Ip, "<default>", props)

    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()
    migrationClient.migrateClientQuotas(batch => batches.add(batch))

    assertEquals(9, batches.size())
    val delta = new ClientQuotasDelta(ClientQuotasImage.EMPTY)
    RecordTestUtils.replayAllBatches(delta, batches)
    val image = delta.apply()

    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("user" -> "").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("user" -> "user1").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("user" -> "user1", "client-id" -> "clientA").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("user" -> "", "client-id" -> "").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("user" -> "", "client-id" -> "clientA").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("client-id" -> "").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("client-id" -> "clientB").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("ip" -> "1.1.1.1").asJava)))
    assertTrue(image.entities().containsKey(new ClientQuotaEntity(Map("ip" -> "").asJava)))
  }

  @Test
  def testWriteExistingClientQuotas(): Unit = {
    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "100000")
    adminZkClient.changeConfigs(ConfigType.User, "user1", props)
    adminZkClient.changeConfigs(ConfigType.User, "user1/clients/clientA", props)

    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> "user1"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0),
      ConfigType.User, "user1")
    assertEquals(1, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> "user1"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 10000.0),
      ConfigType.User, "user1")
    assertEquals(2, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> "user1"),
      Map.empty,
      ConfigType.User, "user1")
    assertEquals(3, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> "user1"),
      Map(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 100.0),
      ConfigType.User, "user1")
    assertEquals(4, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> ""),
      Map(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 200.0),
      ConfigType.User, "<default>")
    assertEquals(5, migrationState.migrationZkVersion())
  }

  // Write Client Quotas using ZkMigrationClient and read them back using AdminZkClient
  private def writeClientQuotaAndVerify(
    migrationClient: ZkMigrationClient,
    adminZkClient: AdminZkClient,
    migrationState: ZkMigrationLeadershipState,
    entity: Map[String, String],
    quotas: Map[String, java.lang.Double],
    zkEntityType: String,
    zkEntityName: String
  ): ZkMigrationLeadershipState = {
    val nextMigrationState = migrationClient.configClient().writeClientQuotas(
      entity.asJava,
      quotas.asJava,
      Map.empty[String, String].asJava,
      migrationState)
    val newProps = ZkAdminManager.clientQuotaPropsToDoubleMap(
      adminZkClient.fetchEntityConfig(zkEntityType, zkEntityName).asScala)
    assertEquals(quotas, newProps)
    nextMigrationState
  }

  @Test
  def testWriteNewClientQuotas(): Unit = {
    assertEquals(0, migrationState.migrationZkVersion())
    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> "user2"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 20000.0, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 100.0),
      ConfigType.User, "user2")

    assertEquals(1, migrationState.migrationZkVersion())

    migrationState = writeClientQuotaAndVerify(migrationClient, adminZkClient, migrationState,
      Map(ClientQuotaEntity.USER -> "user2", ClientQuotaEntity.CLIENT_ID -> "clientA"),
      Map(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG -> 10000.0, QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG -> 200.0),
      ConfigType.User, "user2/clients/clientA")

    assertEquals(2, migrationState.migrationZkVersion())
  }

  @Test
  def testWriteNewTopicConfigs(): Unit = {
    migrationState = migrationClient.configClient().writeConfigs(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
      java.util.Collections.singletonMap(TopicConfig.SEGMENT_MS_CONFIG, "100000"), migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    val newProps = zkClient.getEntityConfigs(ConfigType.Topic, "test")
    assertEquals(1, newProps.size())
    assertEquals("100000", newProps.getProperty(TopicConfig.SEGMENT_MS_CONFIG))
  }

  @Test
  def testWriteExistingTopicConfigs(): Unit = {
    val props = new Properties()
    props.put(TopicConfig.FLUSH_MS_CONFIG, "60000")
    props.put(TopicConfig.RETENTION_MS_CONFIG, "300000")
    zkClient.setOrCreateEntityConfigs(ConfigType.Topic, "test", props)

    migrationState = migrationClient.configClient().writeConfigs(new ConfigResource(ConfigResource.Type.TOPIC, "test"),
      java.util.Collections.singletonMap(TopicConfig.SEGMENT_MS_CONFIG, "100000"), migrationState)
    assertEquals(1, migrationState.migrationZkVersion())

    val newProps = zkClient.getEntityConfigs(ConfigType.Topic, "test")
    assertEquals(1, newProps.size())
    assertEquals("100000", newProps.getProperty(TopicConfig.SEGMENT_MS_CONFIG))
  }

  @Test
  def testScram(): Unit = {
    val random = new MockRandom()

    val scramCredential = new ScramCredential(
      randomBuffer(random, 1024),
      randomBuffer(random, 1024),
      randomBuffer(random, 1024),
      4096)

    val props = new Properties()
    props.put("SCRAM-SHA-256", ScramCredentialUtils.credentialToString(scramCredential))
    adminZkClient.changeConfigs(ConfigType.User, "alice", props)

    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()

    migrationClient.readAllMetadata(batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(0, brokers.size())
    assertEquals(1, batches.size())
    assertEquals(1, batches.get(0).size)
  }

  @Test
  def testScramAndQuotaChangesInSnapshot(): Unit = {
    val random = new MockRandom()

    val props = new Properties()
    props.put(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, "100000")
    adminZkClient.changeConfigs(ConfigType.User, "user1", props)

    // Create SCRAM records in Zookeeper.
    val aliceScramCredential = new ScramCredential(
      randomBuffer(random, 1024),
      randomBuffer(random, 1024),
      randomBuffer(random, 1024),
      4096)

    val alicePropsInit = new Properties()
    alicePropsInit.put("SCRAM-SHA-256", ScramCredentialUtils.credentialToString(aliceScramCredential))
    adminZkClient.changeConfigs(ConfigType.User, "alice", alicePropsInit)

    val delta = new MetadataDelta(MetadataImage.EMPTY)

    // Create a new Quota for user2
    val entityData = new EntityData().setEntityType("user").setEntityName("user2")
    val clientQuotaRecord = new ClientQuotaRecord()
      .setEntity(List(entityData).asJava)
      .setKey("request_percentage")
      .setValue(58.58)
      .setRemove(false)
    delta.replay(clientQuotaRecord)

    // Create a new SCRAM credential for george
    val scramCredentialRecord = new UserScramCredentialRecord()
      .setName("george")
      .setMechanism(ScramMechanism.SCRAM_SHA_256.`type`)
      .setSalt(randomBuffer(random, 1024))
      .setStoredKey(randomBuffer(random, 1024))
      .setServerKey(randomBuffer(random, 1024))
      .setIterations(8192)
    delta.replay(scramCredentialRecord)

    // Add Quota record for user2 but not user1 to delete user1
    // Add SCRAM record for george but not for alice to delete alice
    val image = delta.apply(MetadataProvenance.EMPTY)

    // load snapshot to Zookeeper.
    val kraftMigrationZkWriter = new KRaftMigrationZkWriter(migrationClient)
    kraftMigrationZkWriter.handleSnapshot(image, (_, _, operation) => {
      migrationState = operation.apply(migrationState)
    })

    val user1Props = zkClient.getEntityConfigs(ConfigType.User, "user1")
    assertEquals(0, user1Props.size())
    val user2Props = zkClient.getEntityConfigs(ConfigType.User, "user2")
    assertEquals(1, user2Props.size())

    val georgeProps = zkClient.getEntityConfigs(ConfigType.User, "george")
    assertEquals(1, georgeProps.size())
    val aliceProps = zkClient.getEntityConfigs(ConfigType.User, "alice")
    assertEquals(0, aliceProps.size())
  }

  @Test
  def testDelegationTokens(): Unit = {
    val uuid = CoreUtils.generateUuidAsBase64()
    val owner = SecurityUtils.parseKafkaPrincipal("User:alice")

    val tokenInfo = new TokenInformation(uuid, owner, owner, List(owner).asJava, 0, 100, 1000)

    val hmac: Array[Byte] = Array(1.toByte, 2.toByte, 3.toByte, 4.toByte)
    val token = new DelegationToken(tokenInfo, hmac)

    zkClient.createDelegationTokenPaths()
    zkClient.setOrCreateDelegationToken(token)

    val brokers = new java.util.ArrayList[Integer]()
    val batches = new java.util.ArrayList[java.util.List[ApiMessageAndVersion]]()

    migrationClient.readAllMetadata(batch => batches.add(batch), brokerId => brokers.add(brokerId))
    assertEquals(0, brokers.size())
    assertEquals(1, batches.size())
    assertEquals(1, batches.get(0).size)
  }
}
