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
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.{KafkaZkClient, TopicPartitionStateZNode}
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zookeeper.{CreateResponse, GetDataResponse, ResponseMetadata, ZooKeeperClientException}
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.data.Stat
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite

import scala.collection.mutable

class PartitionStateMachineTest extends JUnitSuite {
  private var controllerContext: ControllerContext = null
  private var mockZkClient: KafkaZkClient = null
  private var mockControllerBrokerRequestBatch: ControllerBrokerRequestBatch = null
  private var mockTopicDeletionManager: TopicDeletionManager = null
  private var partitionState: mutable.Map[TopicPartition, PartitionState] = null
  private var partitionStateMachine: PartitionStateMachine = null

  private val brokerId = 5
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val controllerEpoch = 50
  private val partition = new TopicPartition("t", 0)
  private val partitions = Seq(partition)

  @Before
  def setUp(): Unit = {
    controllerContext = new ControllerContext
    controllerContext.epoch = controllerEpoch
    mockZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    mockControllerBrokerRequestBatch = EasyMock.createMock(classOf[ControllerBrokerRequestBatch])
    mockTopicDeletionManager = EasyMock.createMock(classOf[TopicDeletionManager])
    partitionState = mutable.Map.empty[TopicPartition, PartitionState]
    partitionStateMachine = new PartitionStateMachine(config, new StateChangeLogger(brokerId, true, None), controllerContext, mockTopicDeletionManager,
      mockZkClient, partitionState, mockControllerBrokerRequestBatch)
  }

  @Test
  def testNonexistentPartitionToNewPartitionTransition(): Unit = {
    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testInvalidNonexistentPartitionToOnlinePartitionTransition(): Unit = {
    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    assertEquals(NonExistentPartition, partitionState(partition))
  }

  @Test
  def testInvalidNonexistentPartitionToOfflinePartitionTransition(): Unit = {
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(NonExistentPartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOnlinePartitionTransition(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch)))
      .andReturn(Seq(CreateResponse(Code.OK, null, Some(partition), null, ResponseMetadata(0, 0))))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, leaderIsrAndControllerEpoch, Seq(brokerId), isNew = true))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOnlinePartitionTransitionZkUtilsExceptionFromCreateStates(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch)))
      .andThrow(new ZooKeeperClientException("test"))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOnlinePartitionTransitionErrorCodeFromCreateStates(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, NewPartition)
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(LeaderAndIsr(brokerId, List(brokerId)), controllerEpoch)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.createTopicPartitionStatesRaw(Map(partition -> leaderIsrAndControllerEpoch)))
      .andReturn(Seq(CreateResponse(Code.NODEEXISTS, null, Some(partition), null, ResponseMetadata(0, 0))))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)
    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testNewPartitionToOfflinePartitionTransition(): Unit = {
    partitionState.put(partition, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testInvalidNewPartitionToNonexistentPartitionTransition(): Unit = {
    partitionState.put(partition, NewPartition)
    partitionStateMachine.handleStateChanges(partitions, NonExistentPartition)
    assertEquals(NewPartition, partitionState(partition))
  }

  @Test
  def testOnlinePartitionToOnlineTransition(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, OnlinePartition)
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    val leaderAndIsrAfterElection = leaderAndIsr.newLeader(brokerId)
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> updatedLeaderAndIsr), Seq.empty, Map.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(PreferredReplicaPartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOnlinePartitionToOnlineTransitionForControlledShutdown(): Unit = {
    val otherBrokerId = brokerId + 1
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0), TestUtils.createBroker(otherBrokerId, "host", 0))
    controllerContext.shuttingDownBrokerIds.add(brokerId)
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId, otherBrokerId))
    partitionState.put(partition, OnlinePartition)
    val leaderAndIsr = LeaderAndIsr(brokerId, List(brokerId, otherBrokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    val leaderAndIsrAfterElection = leaderAndIsr.newLeaderAndIsr(otherBrokerId, List(otherBrokerId))
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> updatedLeaderAndIsr), Seq.empty, Map.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(otherBrokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), Seq(brokerId, otherBrokerId),
      isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(ControlledShutdownPartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOnlinePartitionToOfflineTransition(): Unit = {
    partitionState.put(partition, OnlinePartition)
    partitionStateMachine.handleStateChanges(partitions, OfflinePartition)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testInvalidOnlinePartitionToNonexistentPartitionTransition(): Unit = {
    partitionState.put(partition, OnlinePartition)
    partitionStateMachine.handleStateChanges(partitions, NonExistentPartition)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testInvalidOnlinePartitionToNewPartitionTransition(): Unit = {
    partitionState.put(partition, OnlinePartition)
    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransition(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.OK, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    EasyMock.expect(mockZkClient.getLogConfigs(Seq.empty, config.originals()))
      .andReturn((Map(partition.topic -> LogConfig()), Map.empty))
    val leaderAndIsrAfterElection = leaderAndIsr.newLeader(brokerId)
    val updatedLeaderAndIsr = leaderAndIsrAfterElection.withZkVersion(2)
    EasyMock.expect(mockZkClient.updateLeaderAndIsr(Map(partition -> leaderAndIsrAfterElection), controllerEpoch))
      .andReturn(UpdateLeaderAndIsrResult(Map(partition -> updatedLeaderAndIsr), Seq.empty, Map.empty))
    EasyMock.expect(mockControllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(brokerId),
      partition, LeaderIsrAndControllerEpoch(updatedLeaderAndIsr, controllerEpoch), Seq(brokerId), isNew = false))
    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OnlinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransitionZkUtilsExceptionFromStateLookup(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andThrow(new ZooKeeperClientException(""))

    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToOnlinePartitionTransitionErrorCodeFromStateLookup(): Unit = {
    controllerContext.liveBrokers = Set(TestUtils.createBroker(brokerId, "host", 0))
    controllerContext.updatePartitionReplicaAssignment(partition, Seq(brokerId))
    partitionState.put(partition, OfflinePartition)
    val leaderAndIsr = LeaderAndIsr(LeaderAndIsr.NoLeader, List(brokerId))
    val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
    controllerContext.partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)

    val stat = new Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    EasyMock.expect(mockControllerBrokerRequestBatch.newBatch())
    EasyMock.expect(mockZkClient.getTopicPartitionStatesRaw(partitions))
      .andReturn(Seq(GetDataResponse(Code.NONODE, null, Some(partition),
        TopicPartitionStateZNode.encode(leaderIsrAndControllerEpoch), stat, ResponseMetadata(0, 0))))

    EasyMock.expect(mockControllerBrokerRequestBatch.sendRequestsToBrokers(controllerEpoch))
    EasyMock.replay(mockZkClient, mockControllerBrokerRequestBatch)

    partitionStateMachine.handleStateChanges(partitions, OnlinePartition, Option(OfflinePartitionLeaderElectionStrategy))
    EasyMock.verify(mockZkClient, mockControllerBrokerRequestBatch)
    assertEquals(OfflinePartition, partitionState(partition))
  }

  @Test
  def testOfflinePartitionToNonexistentPartitionTransition(): Unit = {
    partitionState.put(partition, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitions, NonExistentPartition)
    assertEquals(NonExistentPartition, partitionState(partition))
  }

  @Test
  def testInvalidOfflinePartitionToNewPartitionTransition(): Unit = {
    partitionState.put(partition, OfflinePartition)
    partitionStateMachine.handleStateChanges(partitions, NewPartition)
    assertEquals(OfflinePartition, partitionState(partition))
  }

}
