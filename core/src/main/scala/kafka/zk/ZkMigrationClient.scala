/*
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

<<<<<<< HEAD
import kafka.api.LeaderAndIsr
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.server.{ConfigEntityName, ConfigType, ZkAdminManager}
import kafka.utils.Logging
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zookeeper._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.quota.ClientQuotaEntity
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.kafka.metadata.migration.{MigrationClient, ZkMigrationLeadershipState}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion, ProducerIdsBlock}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.{CreateMode, KeeperException}

import java.util
import java.util.Properties
import java.util.function.Consumer
import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * Migration client in KRaft controller responsible for handling communication to Zookeeper and
 * the ZkBrokers present in the cluster.
 */
class ZkMigrationClient(zkClient: KafkaZkClient) extends MigrationClient with Logging {

  override def getOrCreateMigrationRecoveryState(initialState: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    zkClient.createTopLevelPaths()
    zkClient.getOrCreateMigrationState(initialState)
  }

  override def setMigrationRecoveryState(state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    zkClient.updateMigrationState(state)
  }

  override def claimControllerLeadership(state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
=======
import kafka.utils.Logging
import kafka.zk.ZkMigrationClient.wrapZkException
import kafka.zk.migration.{ZkAclMigrationClient, ZkConfigMigrationClient, ZkDelegationTokenMigrationClient, ZkTopicMigrationClient}
import kafka.zookeeper._
import org.apache.kafka.clients.admin.ScramMechanism
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.metadata._
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.metadata.DelegationTokenData
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.metadata.migration.ConfigMigrationClient.ClientQuotaVisitor
import org.apache.kafka.metadata.migration.TopicMigrationClient.{TopicVisitor, TopicVisitorInterest}
import org.apache.kafka.metadata.migration._
import org.apache.kafka.security.PasswordEncoder
import org.apache.kafka.server.common.{ApiMessageAndVersion, ProducerIdsBlock}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.{AuthFailedException, NoAuthException, SessionClosedRequireAuthException}

import java.{lang, util}
import java.util.function.Consumer
import scala.collection.Seq
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

object ZkMigrationClient {

  private val MaxBatchSize = 100

  def apply(
    zkClient: KafkaZkClient,
    zkConfigEncoder: PasswordEncoder
  ): ZkMigrationClient = {
    val topicClient = new ZkTopicMigrationClient(zkClient)
    val configClient = new ZkConfigMigrationClient(zkClient, zkConfigEncoder)
    val aclClient = new ZkAclMigrationClient(zkClient)
    val delegationTokenClient = new ZkDelegationTokenMigrationClient(zkClient)
    new ZkMigrationClient(zkClient, topicClient, configClient, aclClient, delegationTokenClient)
  }

  /**
   * Wrap a function such that any KeeperExceptions is captured and converted to a MigrationClientException.
   * Any authentication related exception is converted to a MigrationClientAuthException which may be treated
   * differently by the caller.
   */
  @throws(classOf[MigrationClientException])
  def wrapZkException[T](fn: => T): T = {
    try {
      fn
    } catch {
      case e @ (_: MigrationClientException | _: MigrationClientAuthException) => throw e
      case e @ (_: AuthFailedException | _: NoAuthException | _: SessionClosedRequireAuthException) =>
        // We don't expect authentication errors to be recoverable, so treat them differently
        throw new MigrationClientAuthException(e)
      case e: KeeperException => throw new MigrationClientException(e)
    }
  }

  @throws(classOf[MigrationClientException])
  def logAndRethrow[T](logger: Logging, msg: String)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: Throwable =>
        logger.error(msg, e)
        throw e
    }
  }
}


/**
 * Migration client in KRaft controller responsible for handling communication to Zookeeper and
 * the ZkBrokers present in the cluster. Methods that directly use KafkaZkClient should use the wrapZkException
 * wrapper function in order to translate KeeperExceptions into something usable by the caller.
 */
class ZkMigrationClient(
  zkClient: KafkaZkClient,
  topicClient: TopicMigrationClient,
  configClient: ConfigMigrationClient,
  aclClient: AclMigrationClient,
  delegationTokenClient: DelegationTokenMigrationClient
) extends MigrationClient with Logging {

  override def getOrCreateMigrationRecoveryState(
    initialState: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
      zkClient.createTopLevelPaths()
      zkClient.getOrCreateMigrationState(initialState)
    }

  override def setMigrationRecoveryState(
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    zkClient.updateMigrationState(state)
  }

  override def claimControllerLeadership(
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
>>>>>>> trunk
    zkClient.tryRegisterKRaftControllerAsActiveController(state.kraftControllerId(), state.kraftControllerEpoch()) match {
      case SuccessfulRegistrationResult(controllerEpoch, controllerEpochZkVersion) =>
        state.withZkController(controllerEpoch, controllerEpochZkVersion)
      case FailedRegistrationResult() => state.withUnknownZkController()
    }
  }

<<<<<<< HEAD
  override def releaseControllerLeadership(state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
=======
  override def releaseControllerLeadership(
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
>>>>>>> trunk
    try {
      zkClient.deleteController(state.zkControllerEpochZkVersion())
      state.withUnknownZkController()
    } catch {
      case _: ControllerMovedException =>
        // If the controller moved, no need to release
        state.withUnknownZkController()
      case t: Throwable =>
<<<<<<< HEAD
        throw new RuntimeException("Could not release controller leadership due to underlying error", t)
    }
  }

  def migrateTopics(metadataVersion: MetadataVersion,
                    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
                    brokerIdConsumer: Consumer[Integer]): Unit = {
    val topics = zkClient.getAllTopicsInCluster()
    val topicConfigs = zkClient.getEntitiesConfigs(ConfigType.Topic, topics)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, partitionAssignments) =>
      val partitions = partitionAssignments.keys.toSeq
      val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
      val topicBatch = new util.ArrayList[ApiMessageAndVersion]()
      topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
        .setName(topic)
        .setTopicId(topicIdOpt.get), TopicRecord.HIGHEST_SUPPORTED_VERSION))

      partitionAssignments.foreach { case (topicPartition, replicaAssignment) =>
        replicaAssignment.replicas.foreach(brokerIdConsumer.accept(_))
        replicaAssignment.addingReplicas.foreach(brokerIdConsumer.accept(_))
        val replicaList = replicaAssignment.replicas.map(Integer.valueOf).asJava
        val record = new PartitionRecord()
          .setTopicId(topicIdOpt.get)
          .setPartitionId(topicPartition.partition)
          .setReplicas(replicaList)
          .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
          .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        leaderIsrAndControllerEpochs.get(topicPartition) match {
          case Some(leaderIsrAndEpoch) => record
              .setIsr(leaderIsrAndEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava)
              .setLeader(leaderIsrAndEpoch.leaderAndIsr.leader)
              .setLeaderEpoch(leaderIsrAndEpoch.leaderAndIsr.leaderEpoch)
              .setPartitionEpoch(leaderIsrAndEpoch.leaderAndIsr.partitionEpoch)
              .setLeaderRecoveryState(leaderIsrAndEpoch.leaderAndIsr.leaderRecoveryState.value())
          case None =>
            warn(s"Could not find partition state in ZK for $topicPartition. Initializing this partition " +
              s"with ISR={$replicaList} and leaderEpoch=0.")
            record
              .setIsr(replicaList)
              .setLeader(replicaList.get(0))
              .setLeaderEpoch(0)
              .setPartitionEpoch(0)
              .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
        }
        topicBatch.add(new ApiMessageAndVersion(record, PartitionRecord.HIGHEST_SUPPORTED_VERSION))
      }

      val props = topicConfigs(topic)
      props.forEach { case (key: Object, value: Object) =>
        topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.TOPIC.id)
          .setResourceName(topic)
          .setName(key.toString)
          .setValue(value.toString), ConfigRecord.HIGHEST_SUPPORTED_VERSION))
      }
=======
        throw new MigrationClientException("Could not release controller leadership due to underlying error", t)
    }
  }

  def migrateTopics(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = wrapZkException {
    var topicBatch = new util.ArrayList[ApiMessageAndVersion]()
    topicClient.iterateTopics(
      util.EnumSet.allOf(classOf[TopicVisitorInterest]),
      new TopicVisitor() {
        override def visitTopic(topicName: String, topicId: Uuid, assignments: util.Map[Integer, util.List[Integer]]): Unit = {
          if (!topicBatch.isEmpty) {
            recordConsumer.accept(topicBatch)
            topicBatch = new util.ArrayList[ApiMessageAndVersion]()
          }

          topicBatch.add(new ApiMessageAndVersion(new TopicRecord()
            .setName(topicName)
            .setTopicId(topicId), 0.toShort))

          // This breaks the abstraction a bit, but the topic configs belong in the topic batch
          // when migrating topics and the logic for reading configs lives elsewhere
          configClient.readTopicConfigs(topicName, (topicConfigs: util.Map[String, String]) => {
            topicConfigs.forEach((key: Any, value: Any) => {
              topicBatch.add(new ApiMessageAndVersion(new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id)
                .setResourceName(topicName)
                .setName(key.toString)
                .setValue(value.toString), 0.toShort))
            })
          })
        }

        override def visitPartition(topicIdPartition: TopicIdPartition, partitionRegistration: PartitionRegistration): Unit = {
          val record = new PartitionRecord()
            .setTopicId(topicIdPartition.topicId())
            .setPartitionId(topicIdPartition.partition())
            .setReplicas(partitionRegistration.replicas.map(Integer.valueOf).toList.asJava)
            .setAddingReplicas(partitionRegistration.addingReplicas.map(Integer.valueOf).toList.asJava)
            .setRemovingReplicas(partitionRegistration.removingReplicas.map(Integer.valueOf).toList.asJava)
            .setIsr(partitionRegistration.isr.map(Integer.valueOf).toList.asJava)
            .setLeader(partitionRegistration.leader)
            .setLeaderEpoch(partitionRegistration.leaderEpoch)
            .setPartitionEpoch(partitionRegistration.partitionEpoch)
            .setLeaderRecoveryState(partitionRegistration.leaderRecoveryState.value())
          partitionRegistration.replicas.foreach(brokerIdConsumer.accept(_))
          partitionRegistration.addingReplicas.foreach(brokerIdConsumer.accept(_))
          topicBatch.add(new ApiMessageAndVersion(record, 0.toShort))
        }
      }
    )

    if (!topicBatch.isEmpty) {
>>>>>>> trunk
      recordConsumer.accept(topicBatch)
    }
  }

<<<<<<< HEAD
  def migrateBrokerConfigs(metadataVersion: MetadataVersion,
                           recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val brokerEntities = zkClient.getAllEntitiesWithConfig(ConfigType.Broker)
    val batch = new util.ArrayList[ApiMessageAndVersion]()
    zkClient.getEntitiesConfigs(ConfigType.Broker, brokerEntities.toSet).foreach { case (broker, props) =>
      val brokerResource = if (broker == ConfigEntityName.Default) {
        ""
      } else {
        broker
      }
      props.forEach { case (key: Object, value: Object) =>
        batch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.BROKER.id)
          .setResourceName(brokerResource)
          .setName(key.toString)
          .setValue(value.toString), ConfigRecord.HIGHEST_SUPPORTED_VERSION))
      }
    }
    if (!batch.isEmpty) {
      recordConsumer.accept(batch)
    }
  }

  def migrateClientQuotas(metadataVersion: MetadataVersion,
                          recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)

    def migrateEntityType(entityType: String): Unit = {
      adminZkClient.fetchAllEntityConfigs(entityType).foreach { case (name, props) =>
        val entity = new EntityData().setEntityType(entityType).setEntityName(name)
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
          batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
            .setEntity(List(entity).asJava)
            .setKey(key)
            .setValue(value), ClientQuotaRecord.HIGHEST_SUPPORTED_VERSION))
        }
        recordConsumer.accept(batch)
      }
    }

    migrateEntityType(ConfigType.User)
    migrateEntityType(ConfigType.Client)
    adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).foreach { case (name, props) =>
      // Taken from ZkAdminManager
      val components = name.split("/")
      if (components.size != 3 || components(1) != "clients")
        throw new IllegalArgumentException(s"Unexpected config path: ${name}")
      val entity = List(
        new EntityData().setEntityType(ConfigType.User).setEntityName(components(0)),
        new EntityData().setEntityType(ConfigType.Client).setEntityName(components(2))
      )

      val batch = new util.ArrayList[ApiMessageAndVersion]()
      ZkAdminManager.clientQuotaPropsToDoubleMap(props.asScala).foreach { case (key: String, value: Double) =>
        batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
          .setEntity(entity.asJava)
          .setKey(key)
          .setValue(value), ClientQuotaRecord.HIGHEST_SUPPORTED_VERSION))
      }
      recordConsumer.accept(batch)
    }

    migrateEntityType(ConfigType.Ip)
  }

  def migrateProducerId(metadataVersion: MetadataVersion,
                        recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
=======
  def migrateBrokerConfigs(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = wrapZkException {
    configClient.iterateBrokerConfigs((broker, props) => {
      if (broker.nonEmpty) {
        brokerIdConsumer.accept(Integer.valueOf(broker))
      }
      val batch = new util.ArrayList[ApiMessageAndVersion]()
      props.forEach((key, value) => {
        batch.add(new ApiMessageAndVersion(new ConfigRecord()
          .setResourceType(ConfigResource.Type.BROKER.id)
          .setResourceName(broker)
          .setName(key)
          .setValue(value), 0.toShort))
      })
      if (!batch.isEmpty) {
        recordConsumer.accept(batch)
      }
    })
  }

  def migrateClientQuotas(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    configClient.iterateClientQuotas(new ClientQuotaVisitor {
      override def visitClientQuota(
        entityDataList: util.List[ClientQuotaRecord.EntityData],
        quotas: util.Map[String, lang.Double]
      ): Unit = {
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        quotas.forEach((key, value) => {
          batch.add(new ApiMessageAndVersion(new ClientQuotaRecord()
            .setEntity(entityDataList)
            .setKey(key)
            .setValue(value), 0.toShort))
        })
        recordConsumer.accept(batch)
      }

      override def visitScramCredential(
        userName: String,
        scramMechanism: ScramMechanism,
        scramCredential: ScramCredential
      ): Unit = {
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        batch.add(new ApiMessageAndVersion(new UserScramCredentialRecord()
          .setName(userName)
          .setMechanism(scramMechanism.`type`)
          .setSalt(scramCredential.salt)
          .setStoredKey(scramCredential.storedKey)
          .setServerKey(scramCredential.serverKey)
          .setIterations(scramCredential.iterations), 0.toShort))
        recordConsumer.accept(batch)
      }
    })
  }

  def migrateProducerId(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
>>>>>>> trunk
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt match {
      case Some(data) =>
        val producerIdBlock = ProducerIdBlockZNode.parseProducerIdBlockData(data)
        recordConsumer.accept(List(new ApiMessageAndVersion(new ProducerIdsRecord()
          .setBrokerEpoch(-1)
          .setBrokerId(producerIdBlock.assignedBrokerId)
<<<<<<< HEAD
          .setNextProducerId(producerIdBlock.firstProducerId()), ProducerIdsRecord.HIGHEST_SUPPORTED_VERSION)).asJava)
=======
          .setNextProducerId(producerIdBlock.nextBlockFirstId()), 0.toShort)).asJava)
>>>>>>> trunk
      case None => // Nothing to migrate
    }
  }

<<<<<<< HEAD
  override def readAllMetadata(batchConsumer: Consumer[util.List[ApiMessageAndVersion]],
                               brokerIdConsumer: Consumer[Integer]): Unit = {
    migrateTopics(MetadataVersion.latest(), batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(MetadataVersion.latest(), batchConsumer)
    migrateClientQuotas(MetadataVersion.latest(), batchConsumer)
    migrateProducerId(MetadataVersion.latest(), batchConsumer)
  }

  override def readBrokerIds(): util.Set[Integer] = {
    zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava
  }

  override def readBrokerIdsFromTopicAssignments(): util.Set[Integer] = {
    val topics = zkClient.getAllTopicsInCluster()
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    val brokersWithAssignments = new util.HashSet[Integer]()
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(_, _, assignments) =>
      assignments.values.foreach { assignment =>
        assignment.replicas.foreach { brokerId => brokersWithAssignments.add(brokerId) }
      }
    }
    brokersWithAssignments
  }

  override def createTopic(topicName: String,
                           topicId: Uuid,
                           partitions: util.Map[Integer, PartitionRegistration],
                           state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val assignments = partitions.asScala.map { case (partitionId, partition) =>
      new TopicPartition(topicName, partitionId) ->
        ReplicaAssignment(partition.replicas, partition.addingReplicas, partition.removingReplicas)
    }

    val createTopicZNode = {
      val path = TopicZNode.path(topicName)
      CreateRequest(
        path,
        TopicZNode.encode(Some(topicId), assignments),
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }
    val createPartitionsZNode = {
      val path = TopicPartitionsZNode.path(topicName)
      CreateRequest(
        path,
        null,
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }

    val createPartitionZNodeReqs = partitions.asScala.flatMap { case (partitionId, partition) =>
      val topicPartition = new TopicPartition(topicName, partitionId)
      Seq(
        createTopicPartition(topicPartition),
        createTopicPartitionState(topicPartition, partition, state.kraftControllerEpoch())
      )
    }

    val requests = Seq(createTopicZNode, createPartitionsZNode) ++ createPartitionZNodeReqs
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes(TopicZNode.path(topicName)).equals(Code.NODEEXISTS)) {
      // topic already created, just return
      state
    } else if (resultCodes.forall { case (_, code) => code.equals(Code.OK) } ) {
      // ok
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      // not ok
      throw new RuntimeException(s"Failed to create or update topic $topicName. ZK operation had results $resultCodes")
    }
  }

  private def createTopicPartition(topicPartition: TopicPartition): CreateRequest = {
    val path = TopicPartitionZNode.path(topicPartition)
    CreateRequest(path, null, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def partitionStatePathAndData(topicPartition: TopicPartition,
                                        partitionRegistration: PartitionRegistration,
                                        controllerEpoch: Int): (String, Array[Byte]) = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(new LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    (path, data)
  }

  private def createTopicPartitionState(topicPartition: TopicPartition,
                                        partitionRegistration: PartitionRegistration,
                                        controllerEpoch: Int): CreateRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    CreateRequest(path, data, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def updateTopicPartitionState(topicPartition: TopicPartition,
                                        partitionRegistration: PartitionRegistration,
                                        controllerEpoch: Int): SetDataRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    SetDataRequest(path, data, ZkVersion.MatchAnyVersion, Some(topicPartition))
  }

  override def updateTopicPartitions(topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]],
                                     state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionRegistrations) =>
      partitionRegistrations.asScala.flatMap { case (partitionId, partitionRegistration) =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        Seq(updateTopicPartitionState(topicPartition, partitionRegistration, state.kraftControllerEpoch()))
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state)
      val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
      if (resultCodes.forall { case (_, code) => code.equals(Code.OK) } ) {
        state.withMigrationZkVersion(migrationZkVersion)
      } else {
        throw new RuntimeException(s"Failed to update partition states: $topicPartitions. ZK transaction had results $resultCodes")
      }
    }
  }

  // Try to update an entity config and the migration state. If NoNode is encountered, it probably means we
  // need to recursively create the parent ZNode. In this case, return None.
  def tryWriteEntityConfig(entityType: String,
                           path: String,
                           props: Properties,
                           create: Boolean,
                           state: ZkMigrationLeadershipState): Option[ZkMigrationLeadershipState] = {
    val configData = ConfigEntityZNode.encode(props)

    val requests = if (create) {
      Seq(CreateRequest(ConfigEntityZNode.path(entityType, path), configData, zkClient.defaultAcls(path), CreateMode.PERSISTENT))
    } else {
      Seq(SetDataRequest(ConfigEntityZNode.path(entityType, path), configData, ZkVersion.MatchAnyVersion))
    }
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    if (!create && responses.head.resultCode.equals(Code.NONODE)) {
      // Not fatal. Just means we need to Create this node instead of SetData
      None
    } else if (responses.head.resultCode.equals(Code.OK)) {
      Some(state.withMigrationZkVersion(migrationZkVersion))
    } else {
      throw KeeperException.create(responses.head.resultCode, path)
    }
  }

  override def writeClientQuotas(
    entity: util.Map[String, String],
    quotas: util.Map[String, java.lang.Double],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = {

    val entityMap = entity.asScala
    val hasUser = entityMap.contains(ClientQuotaEntity.USER)
    val hasClient = entityMap.contains(ClientQuotaEntity.CLIENT_ID)
    val hasIp = entityMap.contains(ClientQuotaEntity.IP)
    val props = new Properties()
    // We store client quota values as strings in the ZK JSON
    quotas.forEach { case (key, value) => props.put(key, value.toString) }
    val (configType, path) = if (hasUser && !hasClient) {
      (Some(ConfigType.User), Some(entityMap(ClientQuotaEntity.USER)))
    } else if (hasUser && hasClient) {
      (Some(ConfigType.User), Some(s"${entityMap(ClientQuotaEntity.USER)}/clients/${entityMap(ClientQuotaEntity.CLIENT_ID)}"))
    } else if (hasClient) {
      (Some(ConfigType.Client), Some(entityMap(ClientQuotaEntity.CLIENT_ID)))
    } else if (hasIp) {
      (Some(ConfigType.Ip), Some(entityMap(ClientQuotaEntity.IP)))
    } else {
      (None, None)
    }

    if (path.isEmpty) {
      error(s"Skipping unknown client quota entity $entity")
      return state
    }

    // Try to write the client quota configs once with create=false, and again with create=true if the first operation fails
    tryWriteEntityConfig(configType.get, path.get, props, create=false, state) match {
      case Some(newState) =>
        newState
      case None =>
        // If we didn't update the migration state, we failed to write the client quota. Try again
        // after recursively create its parent znodes
        val createPath = if (hasUser && hasClient) {
          s"${ConfigEntityTypeZNode.path(configType.get)}/${entityMap(ClientQuotaEntity.USER)}/clients"
        } else {
          ConfigEntityTypeZNode.path(configType.get)
        }
        zkClient.createRecursive(createPath, throwIfPathExists=false)
        debug(s"Recursively creating ZNode $createPath and attempting to write $entity quotas a second time.")

        tryWriteEntityConfig(configType.get, path.get, props, create=true, state) match {
          case Some(newStateSecondTry) => newStateSecondTry
          case None => throw new RuntimeException(
            s"Could not write client quotas for $entity on second attempt when using Create instead of SetData")
        }
    }
  }

  override def writeProducerId(nextProducerId: Long, state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
=======
  def migrateAcls(recordConsumer: Consumer[util.List[ApiMessageAndVersion]]): Unit = {
    aclClient.iterateAcls(new util.function.BiConsumer[ResourcePattern, util.Set[AccessControlEntry]]() {
      override def accept(resourcePattern: ResourcePattern, acls: util.Set[AccessControlEntry]): Unit = {
        val batch = new util.ArrayList[ApiMessageAndVersion]()
        acls.asScala.foreach { entry =>
          batch.add(new ApiMessageAndVersion(new AccessControlEntryRecord()
            .setId(Uuid.randomUuid())
            .setResourceType(resourcePattern.resourceType().code())
            .setResourceName(resourcePattern.name())
            .setPatternType(resourcePattern.patternType().code())
            .setPrincipal(entry.principal())
            .setHost(entry.host())
            .setOperation(entry.operation().code())
            .setPermissionType(entry.permissionType().code()), AccessControlEntryRecord.HIGHEST_SUPPORTED_VERSION))
          if (batch.size() == ZkMigrationClient.MaxBatchSize) {
            recordConsumer.accept(batch)
            batch.clear()
          }
        }
        if (!batch.isEmpty) {
          recordConsumer.accept(batch)
        }
      }
    })
  }

  private def migrateDelegationTokens(
    recordConsumer: Consumer[util.List[ApiMessageAndVersion]]
  ): Unit = wrapZkException {
    val batch = new util.ArrayList[ApiMessageAndVersion]()
    val tokens = zkClient.getChildren(DelegationTokensZNode.path)
    for (tokenId <- tokens) {
      zkClient.getDelegationTokenInfo(tokenId) match {
        case Some(tokenInformation) =>
          val newDelegationTokenData = new DelegationTokenData(tokenInformation)
          batch.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), 0.toShort))
        case None =>
      }
    }
    if (!batch.isEmpty) {
      recordConsumer.accept(batch)
    }
  }

  override def readAllMetadata(
    batchConsumer: Consumer[util.List[ApiMessageAndVersion]],
    brokerIdConsumer: Consumer[Integer]
  ): Unit = {
    migrateTopics(batchConsumer, brokerIdConsumer)
    migrateBrokerConfigs(batchConsumer, brokerIdConsumer)
    migrateClientQuotas(batchConsumer)
    migrateProducerId(batchConsumer)
    migrateAcls(batchConsumer)
    migrateDelegationTokens(batchConsumer)
  }

  override def readBrokerIds(): util.Set[Integer] = wrapZkException {
    new util.HashSet[Integer](zkClient.getSortedBrokerList.map(Integer.valueOf).toSet.asJava)
  }

  override def readProducerId(): util.Optional[ProducerIdsBlock] = {
    val (dataOpt, _) = zkClient.getDataAndVersion(ProducerIdBlockZNode.path)
    dataOpt.map(ProducerIdBlockZNode.parseProducerIdBlockData).asJava
  }

  override def writeProducerId(
    nextProducerId: Long,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
>>>>>>> trunk
    val newProducerIdBlockData = ProducerIdBlockZNode.generateProducerIdBlockJson(
      new ProducerIdsBlock(-1, nextProducerId, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE))

    val request = SetDataRequest(ProducerIdBlockZNode.path, newProducerIdBlockData, ZkVersion.MatchAnyVersion)
    val (migrationZkVersion, _) = zkClient.retryMigrationRequestsUntilConnected(Seq(request), state)
    state.withMigrationZkVersion(migrationZkVersion)
  }

<<<<<<< HEAD
  override def writeConfigs(resource: ConfigResource,
                            configs: util.Map[String, String],
                            state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    val configType = resource.`type`() match {
      case ConfigResource.Type.BROKER => Some(ConfigType.Broker)
      case ConfigResource.Type.TOPIC => Some(ConfigType.Topic)
      case _ => None
    }

    val configName = resource.name()
    if (configType.isDefined) {
      val props = new Properties()
      configs.forEach { case (key, value) => props.put(key, value) }
      tryWriteEntityConfig(configType.get, configName, props, create=false, state) match {
        case Some(newState) =>
          newState
        case None =>
          val createPath = ConfigEntityTypeZNode.path(configType.get)
          debug(s"Recursively creating ZNode $createPath and attempting to write $resource configs a second time.")
          zkClient.createRecursive(createPath, throwIfPathExists=false)

          tryWriteEntityConfig(configType.get, configName, props, create=true, state) match {
            case Some(newStateSecondTry) => newStateSecondTry
            case None => throw new RuntimeException(
              s"Could not write ${configType.get} configs on second attempt when using Create instead of SetData.")
          }
      }
    } else {
      debug(s"Not updating ZK for $resource since it is not a Broker or Topic entity.")
      state
    }
  }

  override def writeMetadataDeltaToZookeeper(delta: MetadataDelta,
                                             image: MetadataImage,
                                             state: ZkMigrationLeadershipState): ZkMigrationLeadershipState = {
    state
  }
=======
  override def topicClient(): TopicMigrationClient = topicClient

  override def configClient(): ConfigMigrationClient = configClient

  override def aclClient(): AclMigrationClient = aclClient

  override def delegationTokenClient(): DelegationTokenMigrationClient = delegationTokenClient
>>>>>>> trunk
}
