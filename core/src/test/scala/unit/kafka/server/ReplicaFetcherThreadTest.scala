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
package kafka.server

import kafka.api.{ApiVersion, KAFKA_2_6_IV0}
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.log.{LogAppendInfo, LogManager, UnifiedLog}
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.epoch.util.ReplicaFetcherMockBlockingSend
import kafka.server.metadata.ZkMetadataCache
import kafka.utils.TestUtils
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.{FetchResponseData, UpdateMetadataRequestData}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, UpdateMetadataRequest}
import org.apache.kafka.common.utils.SystemTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong}
import org.mockito.Mockito.{mock, never, times, verify, when}

import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Optional}
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

class ReplicaFetcherThreadTest {

  private val t1p0 = new TopicPartition("topic1", 0)
  private val t1p1 = new TopicPartition("topic1", 1)
  private val t2p1 = new TopicPartition("topic2", 1)

  private val topicId1 = Uuid.randomUuid()
  private val topicId2 = Uuid.randomUuid()

  private val topicIds = Map("topic1" -> topicId1, "topic2" -> topicId2)

  private val brokerEndPoint = new BrokerEndPoint(0, "localhost", 1000)
  private val failedPartitions = new FailedPartitions

  private val partitionStates = List(
    new UpdateMetadataRequestData.UpdateMetadataPartitionState()
      .setTopicName("topic1")
      .setPartitionIndex(0)
      .setControllerEpoch(0)
      .setLeader(0)
      .setLeaderEpoch(0),
    new UpdateMetadataRequestData.UpdateMetadataPartitionState()
      .setTopicName("topic2")
      .setPartitionIndex(0)
      .setControllerEpoch(0)
      .setLeader(0)
      .setLeaderEpoch(0),
  ).asJava

  private val updateMetadataRequest = new UpdateMetadataRequest.Builder(ApiKeys.UPDATE_METADATA.latestVersion(),
    0, 0, 0, partitionStates, Collections.emptyList(), topicIds.asJava).build()
  // TODO: support raft code?
  private val metadataCache = new ZkMetadataCache(0)
  metadataCache.updateMetadata(0, updateMetadataRequest)

  private def initialFetchState(topicId: Option[Uuid], fetchOffset: Long, leaderEpoch: Int = 1): InitialFetchState = {
    InitialFetchState(topicId = topicId, leader = new BrokerEndPoint(0, "localhost", 9092),
      initOffset = fetchOffset, currentLeaderEpoch = leaderEpoch)
  }

  @AfterEach
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test
  def shouldSendLatestRequestVersionsByDefault(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions: FailedPartitions,
      replicaMgr = replicaManager,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = UnboundedQuota,
      leaderEndpointBlockingSend = None)
    assertEquals(ApiKeys.FETCH.latestVersion, thread.fetchRequestVersion)
    assertEquals(ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion, thread.offsetForLeaderEpochRequestVersion)
    assertEquals(ApiKeys.LIST_OFFSETS.latestVersion, thread.listOffsetRequestVersion)
  }

  @Test
  def testFetchLeaderEpochRequestIfLastEpochDefinedForSomePartitions(): Unit = {
    val config = kafkaConfigNoTruncateOnFetch

    //Setup all dependencies
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 5

    //Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.logEndOffset).thenReturn(0)
    when(log.highWatermark).thenReturn(0)
    when(log.latestEpoch)
      .thenReturn(Some(leaderEpoch))
      .thenReturn(Some(leaderEpoch))
      .thenReturn(None)  // t2p1 doesn't support epochs
    when(log.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Some(OffsetAndEpoch(0, leaderEpoch)))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, leaderEpoch, 1),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, leaderEpoch, 1)).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())

    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    // topic 1 supports epoch, t2 doesn't.
    thread.addPartitions(Map(
      t1p0 -> initialFetchState(Some(topicId1), 0L),
      t1p1 -> initialFetchState(Some(topicId2), 0L),
      t2p1 -> initialFetchState(Some(topicId2), 0L)))

    assertPartitionStates(thread, shouldBeReadyForFetch = false, shouldBeTruncatingLog = true, shouldBeDelayed = false)
    //Loop 1
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)

    assertPartitionStates(thread, shouldBeReadyForFetch = true, shouldBeTruncatingLog = false, shouldBeDelayed = false)

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    assertPartitionStates(thread, shouldBeReadyForFetch = true, shouldBeTruncatingLog = false, shouldBeDelayed = false)

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)

    assertPartitionStates(thread, shouldBeReadyForFetch = true, shouldBeTruncatingLog = false, shouldBeDelayed = false)

    //Assert that truncate to is called exactly once (despite two loops)
    verify(partition, times(3)).truncateTo(anyLong(), anyBoolean())
  }

  /**
    * Assert that all partitions' states are as expected
    *
    */
  def assertPartitionStates(fetcher: AbstractFetcherThread,
                            shouldBeReadyForFetch: Boolean,
                            shouldBeTruncatingLog: Boolean,
                            shouldBeDelayed: Boolean): Unit = {
    for (tp <- List(t1p0, t1p1, t2p1)) {
      assertTrue(fetcher.fetchState(tp).isDefined)
      val fetchState = fetcher.fetchState(tp).get

      assertEquals(shouldBeReadyForFetch, fetchState.isReadyForFetch,
        s"Partition $tp should${if (!shouldBeReadyForFetch) " NOT" else ""} be ready for fetching")

      assertEquals(shouldBeTruncatingLog, fetchState.isTruncating,
        s"Partition $tp should${if (!shouldBeTruncatingLog) " NOT" else ""} be truncating its log")

      assertEquals(shouldBeDelayed, fetchState.isDelayed,
        s"Partition $tp should${if (!shouldBeDelayed) " NOT" else ""} be delayed")
    }
  }

  @Test
  def shouldHandleExceptionFromBlockingSend(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])

    when(mockBlockingSend.sendRequest(any())).thenThrow(new NullPointerException)
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions: FailedPartitions,
      replicaMgr = replicaManager,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(mockBlockingSend))

    val result = thread.fetchEpochEndOffsets(Map(
      t1p0 -> new OffsetForLeaderPartition()
        .setPartition(t1p0.partition)
        .setLeaderEpoch(0),
      t1p1 -> new OffsetForLeaderPartition()
        .setPartition(t1p1.partition)
        .setLeaderEpoch(0)))

    val expected = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, Errors.UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
    )

    assertEquals(expected, result, "results from leader epoch request should have undefined offset")
    verify(mockBlockingSend).sendRequest(any())
  }

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnlyIfLeaderEpochKnownToBothIbp26(): Unit = {
    verifyFetchLeaderEpochOnFirstFetch(KAFKA_2_6_IV0)
  }

  @Test
  def shouldNotFetchLeaderEpochOnFirstFetchWithTruncateOnFetch(): Unit = {
    verifyFetchLeaderEpochOnFirstFetch(ApiVersion.latestVersion, epochFetchCount = 0)
  }

  private def verifyFetchLeaderEpochOnFirstFetch(ibp: ApiVersion, epochFetchCount: Int = 1): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, ibp.version)
    val config = KafkaConfig.fromProps(props)

    //Setup all dependencies
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 5

    //Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(0)
    when(log.latestEpoch).thenReturn(Some(leaderEpoch))
    when(log.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Some(OffsetAndEpoch(0, leaderEpoch)))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, leaderEpoch, 1),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, leaderEpoch, 1)).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager,
      new Metrics, new SystemTime, UnboundedQuota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    //Loop 1
    thread.doWork()
    assertEquals(epochFetchCount, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(epochFetchCount, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(epochFetchCount, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)
  }

  @Test
  def shouldTruncateToOffsetSpecifiedInEpochOffsetResponse(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = kafkaConfigNoTruncateOnFetch
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 5
    val initialLEO = 200

    //Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(initialLEO - 1)
    when(log.latestEpoch).thenReturn(Some(leaderEpoch))
    when(log.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Some(OffsetAndEpoch(initialLEO, leaderEpoch)))
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, leaderEpoch, 156),
      t2p1 -> newOffsetForLeaderPartitionResult(t2p1, leaderEpoch, 172)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager,
      new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t2p1 -> initialFetchState(Some(topicId2), 0L)))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(156),
               "Expected " + t1p0 + " to truncate to offset 156 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    assertTrue(truncateToCapture.getAllValues.asScala.contains(172),
               "Expected " + t2p1 + " to truncate to offset 172 (truncation offsets: " + truncateToCapture.getAllValues + ")")
  }

  @Test
  def shouldTruncateToOffsetSpecifiedInEpochOffsetResponseIfFollowerHasNoMoreEpochs(): Unit = {
    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = kafkaConfigNoTruncateOnFetch
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpochAtFollower = 5
    val leaderEpochAtLeader = 4
    val initialLEO = 200

    //Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(initialLEO - 3)
    when(log.latestEpoch).thenReturn(Some(leaderEpochAtFollower))
    when(log.endOffsetForEpoch(leaderEpochAtLeader)).thenReturn(None)
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, leaderEpochAtLeader, 156),
      t2p1 -> newOffsetForLeaderPartitionResult(t2p1, leaderEpochAtLeader, 202)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions,
      replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t2p1 -> initialFetchState(Some(topicId2), 0L)))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(156),
               "Expected " + t1p0 + " to truncate to offset 156 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    assertTrue(truncateToCapture.getAllValues.asScala.contains(initialLEO),
               "Expected " + t2p1 + " to truncate to offset " + initialLEO +
               " (truncation offsets: " + truncateToCapture.getAllValues + ")")
  }

  @Test
  def shouldFetchLeaderEpochSecondTimeIfLeaderRepliesWithEpochNotKnownToFollower(): Unit = {
    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    val config = kafkaConfigNoTruncateOnFetch

    // Setup all dependencies
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val initialLEO = 200

    // Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(initialLEO - 2)
    when(log.latestEpoch).thenReturn(Some(5))
    when(log.endOffsetForEpoch(4)).thenReturn(
      Some(OffsetAndEpoch(120, 3)))
    when(log.endOffsetForEpoch(3)).thenReturn(
      Some(OffsetAndEpoch(120, 3)))
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    // Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, 4, 155),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, 4, 143)).asJava

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    // Loop 1 -- both topic partitions will need to fetch another leader epoch
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(0, mockNetwork.fetchCount)

    // Loop 2 should do the second fetch for both topic partitions because the leader replied with
    // epoch 4 while follower knows only about epoch 3
    val nextOffsets = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, 3, 101),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, 3, 102)).asJava
    mockNetwork.setOffsetsForNextResponse(nextOffsets)

    thread.doWork()
    assertEquals(2, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)
    assertTrue(mockNetwork.lastUsedOffsetForLeaderEpochVersion >= 3,
      "OffsetsForLeaderEpochRequest version.")

    //Loop 3 we should not fetch epochs
    thread.doWork()
    assertEquals(2, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    verify(partition, times(4)).truncateTo(truncateToCapture.capture(), anyBoolean())
    //We should have truncated to the offsets in the second response
    assertTrue(truncateToCapture.getAllValues.asScala.contains(102),
               "Expected " + t1p1 + " to truncate to offset 102 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    assertTrue(truncateToCapture.getAllValues.asScala.contains(101),
               "Expected " + t1p0 + " to truncate to offset 101 (truncation offsets: " + truncateToCapture.getAllValues + ")")
  }

  @Test
  def shouldTruncateIfLeaderRepliesWithDivergingEpochNotKnownToFollower(): Unit = {

    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))

    // Setup all dependencies
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val initialLEO = 200
    var latestLogEpoch: Option[Int] = Some(5)

    // Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(115)
    when(log.latestEpoch).thenAnswer(_ => latestLogEpoch)
    when(log.endOffsetForEpoch(4)).thenReturn(Some(OffsetAndEpoch(149, 4)))
    when(log.endOffsetForEpoch(3)).thenReturn(Some(OffsetAndEpoch(129, 2)))
    when(log.endOffsetForEpoch(2)).thenReturn(Some(OffsetAndEpoch(119, 1)))
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(Collections.emptyMap(), brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork)) {
      override def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: FetchData): Option[LogAppendInfo] = None
    }
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), initialLEO), t1p1 -> initialFetchState(Some(topicId1), initialLEO)))
    val partitions = Set(t1p0, t1p1)

    // Loop 1 -- both topic partitions skip epoch fetch and send fetch request since we can truncate
    // later based on diverging epochs in fetch response.
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }

    def partitionData(partition: Int, divergingEpoch: FetchResponseData.EpochEndOffset): FetchResponseData.PartitionData = {
      new FetchResponseData.PartitionData()
        .setPartitionIndex(partition)
        .setLastStableOffset(0)
        .setLogStartOffset(0)
        .setDivergingEpoch(divergingEpoch)
    }

    // Loop 2 should truncate based on diverging epoch and continue to send fetch requests.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> partitionData(t1p0.partition, new FetchResponseData.EpochEndOffset().setEpoch(4).setEndOffset(140)),
      t1p1 -> partitionData(t1p1.partition, new FetchResponseData.EpochEndOffset().setEpoch(4).setEndOffset(141))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)
    latestLogEpoch = Some(4)
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(140),
      "Expected " + t1p0 + " to truncate to offset 140 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    assertTrue(truncateToCapture.getAllValues.asScala.contains(141),
      "Expected " + t1p1 + " to truncate to offset 141 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }

    // Loop 3 should truncate because of diverging epoch. Offset truncation is not complete
    // because divergent epoch is not known to follower. We truncate and stay in Fetching state.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> partitionData(t1p0.partition, new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(130)),
      t1p1 -> partitionData(t1p1.partition, new FetchResponseData.EpochEndOffset().setEpoch(3).setEndOffset(131))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(3, mockNetwork.fetchCount)
    verify(partition, times(4)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(129),
      "Expected to truncate to offset 129 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }

    // Loop 4 should truncate because of diverging epoch. Offset truncation is not complete
    // because divergent epoch is not known to follower. Last fetched epoch cannot be determined
    // from the log. We truncate and stay in Fetching state.
    mockNetwork.setFetchPartitionDataForNextResponse(Map(
      t1p0 -> partitionData(t1p0.partition, new FetchResponseData.EpochEndOffset().setEpoch(2).setEndOffset(120)),
      t1p1 -> partitionData(t1p1.partition, new FetchResponseData.EpochEndOffset().setEpoch(2).setEndOffset(121))
    ))
    mockNetwork.setIdsForNextResponse(topicIds)
    latestLogEpoch = None
    thread.doWork()
    assertEquals(0, mockNetwork.epochFetchCount)
    assertEquals(4, mockNetwork.fetchCount)
    verify(partition, times(6)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(119),
      "Expected to truncate to offset 119 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    partitions.foreach { tp => assertEquals(Fetching, thread.fetchState(tp).get.state) }
  }

  @Test
  def shouldUseLeaderEndOffsetIfInterBrokerVersionBelow20(): Unit = {

    // Create a capture to track what partitions/offsets are truncated
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.11.0")
    val config = KafkaConfig.fromProps(props)

    // Setup all dependencies
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val initialLEO = 200

    // Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(initialLEO - 2)
    when(log.latestEpoch).thenReturn(Some(5))
    when(log.endOffsetForEpoch(4)).thenReturn(
      Some(OffsetAndEpoch(120, 3)))
    when(log.endOffsetForEpoch(3)).thenReturn(
      Some(OffsetAndEpoch(120, 3)))
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    // Define the offsets for the OffsetsForLeaderEpochResponse with undefined epoch to simulate
    // older protocol version
    val offsets = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, UNDEFINED_EPOCH, 155),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, UNDEFINED_EPOCH, 143)).asJava

    // Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    // Loop 1 -- both topic partitions will truncate to leader offset even though they don't know
    // about leader epoch
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(1, mockNetwork.fetchCount)
    assertEquals(0, mockNetwork.lastUsedOffsetForLeaderEpochVersion, "OffsetsForLeaderEpochRequest version.")

    //Loop 2 we should not fetch epochs
    thread.doWork()
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)

    //We should have truncated to the offsets in the first response
    verify(partition, times(2)).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertTrue(truncateToCapture.getAllValues.asScala.contains(155),
               "Expected " + t1p0 + " to truncate to offset 155 (truncation offsets: " + truncateToCapture.getAllValues + ")")
    assertTrue(truncateToCapture.getAllValues.asScala.contains(143),
               "Expected " + t1p1 + " to truncate to offset 143 (truncation offsets: " + truncateToCapture.getAllValues + ")")
  }

  @Test
  def shouldTruncateToInitialFetchOffsetIfLeaderReturnsUndefinedOffset(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = kafkaConfigNoTruncateOnFetch
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val initialFetchOffset = 100

    //Stubs
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(initialFetchOffset)
    when(log.latestEpoch).thenReturn(Some(5))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), initialFetchOffset)))

    //Run it
    thread.doWork()

    //We should have truncated to initial fetch offset
    verify(partition).truncateTo(truncated.capture(), anyBoolean())
    assertEquals(initialFetchOffset, truncated.getValue)
  }

  @Test
  def shouldPollIndefinitelyIfLeaderReturnsAnyException(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])

    // Setup all the dependencies
    val config = kafkaConfigNoTruncateOnFetch
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 5
    val highWaterMark = 100
    val initialLeo = 300

    //Stubs
    when(log.highWatermark).thenReturn(highWaterMark)
    when(partition.localLogOrException).thenReturn(log)
    when(log.latestEpoch).thenReturn(Some(leaderEpoch))
    // this is for the last reply with EpochEndOffset(5, 156)
    when(log.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Some(OffsetAndEpoch(initialLeo, leaderEpoch)))
    when(log.logEndOffset).thenReturn(initialLeo)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = mutable.Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, NOT_LEADER_OR_FOLLOWER, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, UNKNOWN_SERVER_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
    ).asJava

    //Create the thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    //Run thread 3 times
    (0 to 3).foreach { _ =>
      thread.doWork()
    }

    //Then should loop continuously while there is no leader
    verify(partition, never()).truncateTo(anyLong(), anyBoolean())

    //New leader elected and replies
    offsetsReply.put(t1p0, newOffsetForLeaderPartitionResult(t1p0, leaderEpoch, 156))

    thread.doWork()

    //Now the final call should have actually done a truncation (to offset 156)
    verify(partition).truncateTo(truncated.capture(), anyBoolean())
    assertEquals(156, truncated.getValue)
  }

  @Test
  def shouldMovePartitionsOutOfTruncatingLogState(): Unit = {
    val config = kafkaConfigNoTruncateOnFetch

    //Setup all stubs
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    val leaderEpoch = 4

    //Stub return values
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(0)
    when(log.latestEpoch).thenReturn(Some(leaderEpoch))
    when(log.endOffsetForEpoch(leaderEpoch)).thenReturn(
      Some(OffsetAndEpoch(0, leaderEpoch)))
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, leaderEpoch, 1),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, leaderEpoch, 1)
    ).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    //Then all partitions should start in an TruncatingLog state
    assertEquals(Option(Truncating), thread.fetchState(t1p0).map(_.state))
    assertEquals(Option(Truncating), thread.fetchState(t1p1).map(_.state))

    //When
    thread.doWork()

    //Then none should be TruncatingLog anymore
    assertEquals(Option(Fetching), thread.fetchState(t1p0).map(_.state))
    assertEquals(Option(Fetching), thread.fetchState(t1p1).map(_.state))
    verify(partition, times(2)).truncateTo(0L, false)
  }

  @Test
  def shouldFilterPartitionsMadeLeaderDuringLeaderEpochRequest(): Unit ={
    val config = kafkaConfigNoTruncateOnFetch
    val truncateToCapture: ArgumentCaptor[Long] = ArgumentCaptor.forClass(classOf[Long])
    val initialLEO = 100

    //Setup all stubs
    val quota: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
    val logManager: LogManager = mock(classOf[LogManager])
    val replicaAlterLogDirsManager: ReplicaAlterLogDirsManager = mock(classOf[ReplicaAlterLogDirsManager])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val partition: Partition = mock(classOf[Partition])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])

    //Stub return values
    when(partition.localLogOrException).thenReturn(log)
    when(log.highWatermark).thenReturn(initialLEO - 2)
    when(log.latestEpoch).thenReturn(Some(5))
    when(log.endOffsetForEpoch(5)).thenReturn(Some(OffsetAndEpoch(initialLEO, 5)))
    when(log.logEndOffset).thenReturn(initialLEO)
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaManager.logManager).thenReturn(logManager)
    when(replicaManager.replicaAlterLogDirsManager).thenReturn(replicaAlterLogDirsManager)
    stub(partition, replicaManager, log)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsetsReply = Map(
      t1p0 -> newOffsetForLeaderPartitionResult(t1p0, 5, 52),
      t1p1 -> newOffsetForLeaderPartitionResult(t1p1, 5, 49)
    ).asJava

    //Create the fetcher thread
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, brokerEndPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions, replicaManager, new Metrics(),
      new SystemTime(), quota, Some(mockNetwork))

    //When
    thread.addPartitions(Map(t1p0 -> initialFetchState(Some(topicId1), 0L), t1p1 -> initialFetchState(Some(topicId1), 0L)))

    //When the epoch request is outstanding, remove one of the partitions to simulate a leader change. We do this via a callback passed to the mock thread
    val partitionThatBecameLeader = t1p0
    mockNetwork.setEpochRequestCallback(() => {
      thread.removePartitions(Set(partitionThatBecameLeader))
    })

    //When
    thread.doWork()

    //Then we should not have truncated the partition that became leader. Exactly one partition should be truncated.
    verify(partition).truncateTo(truncateToCapture.capture(), anyBoolean())
    assertEquals(49, truncateToCapture.getValue)
  }

  @Test
  def shouldCatchExceptionFromBlockingSendWhenShuttingDownReplicaFetcherThread(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])

    when(mockBlockingSend.initiateClose()).thenThrow(new IllegalArgumentException())
    when(mockBlockingSend.close()).thenThrow(new IllegalStateException())
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = null,
      leaderEndpointBlockingSend = Some(mockBlockingSend))
    thread.start()

    // Verify that:
    //   1) IllegalArgumentException thrown by BlockingSend#initiateClose() during `initiateShutdown` is not propagated
    //   2) BlockingSend.close() is invoked even if BlockingSend#initiateClose() fails
    //   3) IllegalStateException thrown by BlockingSend.close() during `awaitShutdown` is not propagated
    thread.initiateShutdown()
    thread.awaitShutdown()
    verify(mockBlockingSend).initiateClose()
    verify(mockBlockingSend).close()
  }

  @Test
  def shouldUpdateReassignmentBytesInMetrics(): Unit = {
    assertProcessPartitionDataWhen(isReassigning = true)
  }

  @Test
  def shouldNotUpdateReassignmentBytesInMetricsWhenNoReassignmentsInProgress(): Unit = {
    assertProcessPartitionDataWhen(isReassigning = false)
  }

  @Test
  def testBuildFetch(): Unit = {
    val tid1p0 = new TopicIdPartition(topicId1, t1p0)
    val tid1p1 = new TopicIdPartition(topicId1, t1p1)
    val tid2p1 = new TopicIdPartition(topicId2, t2p1)

    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])
    val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])
    val log: UnifiedLog = mock(classOf[UnifiedLog])

    when(replicaManager.brokerTopicStats).thenReturn(mock(classOf[BrokerTopicStats]))
    when(replicaManager.localLogOrException(any[TopicPartition])).thenReturn(log)
    when(replicaQuota.isThrottled(any[TopicPartition])).thenReturn(false)
    when(log.logStartOffset).thenReturn(0)

    val thread = new ReplicaFetcherThread("bob", 0, brokerEndPoint, config, failedPartitions,
      replicaManager, new Metrics(), new SystemTime(), replicaQuota, Some(mockBlockingSend))

    val leaderEpoch = 1

    val partitionMap = Map(
        t1p0 -> PartitionFetchState(Some(topicId1), 150, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
        t1p1 -> PartitionFetchState(Some(topicId1), 155, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
        t2p1 -> PartitionFetchState(Some(topicId2), 160, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None))

    val ResultWithPartitions(fetchRequestOpt, _) = thread.buildFetch(partitionMap)

    assertTrue(fetchRequestOpt.isDefined)
    val fetchRequestBuilder = fetchRequestOpt.get.fetchRequest

    val partitionDataMap = partitionMap.map { case (tp, state) =>
      (tp, new FetchRequest.PartitionData(state.topicId.get, state.fetchOffset, 0L,
        config.replicaFetchMaxBytes, Optional.of(state.currentLeaderEpoch), Optional.empty()))
    }

    assertEquals(partitionDataMap.asJava, fetchRequestBuilder.fetchData())
    assertEquals(0, fetchRequestBuilder.replaced().size)
    assertEquals(0, fetchRequestBuilder.removed().size)

    val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    responseData.put(tid1p0, new FetchResponseData.PartitionData())
    responseData.put(tid1p1, new FetchResponseData.PartitionData())
    responseData.put(tid2p1, new FetchResponseData.PartitionData())
    val fetchResponse = FetchResponse.of(Errors.NONE, 0, 123, responseData)

    thread.fetchSessionHandler.handleResponse(fetchResponse, ApiKeys.FETCH.latestVersion())

    // Remove t1p0, change the ID for t2p1, and keep t1p1 the same
    val newTopicId = Uuid.randomUuid()
    val partitionMap2 = Map(
      t1p1 -> PartitionFetchState(Some(topicId1), 155, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None),
      t2p1 -> PartitionFetchState(Some(newTopicId), 160, None, leaderEpoch, None, state = Fetching, lastFetchedEpoch = None))
    val ResultWithPartitions(fetchRequestOpt2, _) = thread.buildFetch(partitionMap2)

    // Since t1p1 didn't change, we drop that one
    val partitionDataMap2 = partitionMap2.drop(1).map { case (tp, state) =>
      (tp, new FetchRequest.PartitionData(state.topicId.get, state.fetchOffset, 0L,
        config.replicaFetchMaxBytes, Optional.of(state.currentLeaderEpoch), Optional.empty()))
    }

    assertTrue(fetchRequestOpt2.isDefined)
    val fetchRequestBuilder2 = fetchRequestOpt2.get.fetchRequest
    assertEquals(partitionDataMap2.asJava, fetchRequestBuilder2.fetchData())
    assertEquals(Collections.singletonList(tid2p1), fetchRequestBuilder2.replaced())
    assertEquals(Collections.singletonList(tid1p0), fetchRequestBuilder2.removed())
  }

  private def newOffsetForLeaderPartitionResult(
   tp: TopicPartition,
   leaderEpoch: Int,
   endOffset: Long
 ): EpochEndOffset = {
    newOffsetForLeaderPartitionResult(tp, Errors.NONE, leaderEpoch, endOffset)
  }

  private def newOffsetForLeaderPartitionResult(
    tp: TopicPartition,
    error: Errors,
    leaderEpoch: Int,
    endOffset: Long
  ): EpochEndOffset = {
    new EpochEndOffset()
      .setPartition(tp.partition)
      .setErrorCode(error.code)
      .setLeaderEpoch(leaderEpoch)
      .setEndOffset(endOffset)
  }

  private def assertProcessPartitionDataWhen(isReassigning: Boolean): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val config = KafkaConfig.fromProps(props)

    val mockBlockingSend: BlockingSend = mock(classOf[BlockingSend])

    val log: UnifiedLog = mock(classOf[UnifiedLog])

    val partition: Partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(partition.isReassigning).thenReturn(isReassigning)
    when(partition.isAddingLocalReplica).thenReturn(isReassigning)

    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getPartitionOrException(any[TopicPartition])).thenReturn(partition)
    val brokerTopicStats = new BrokerTopicStats
    when(replicaManager.brokerTopicStats).thenReturn(brokerTopicStats)

    val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

    val thread = new ReplicaFetcherThread(
      name = "bob",
      fetcherId = 0,
      sourceBroker = brokerEndPoint,
      brokerConfig = config,
      failedPartitions = failedPartitions,
      replicaMgr = replicaManager,
      metrics =  new Metrics(),
      time = new SystemTime(),
      quota = replicaQuota,
      leaderEndpointBlockingSend = Some(mockBlockingSend))

    val records = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    val partitionData: thread.FetchData = new FetchResponseData.PartitionData()
      .setPartitionIndex(t1p0.partition)
      .setLastStableOffset(0)
      .setLogStartOffset(0)
      .setRecords(records)
    thread.processPartitionData(t1p0, 0, partitionData)

    if (isReassigning)
      assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.reassignmentBytesInPerSec.get.count())
    else
      assertEquals(0, brokerTopicStats.allTopicsStats.reassignmentBytesInPerSec.get.count())

    assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.replicationBytesInRate.get.count())
  }

  def stub(partition: Partition, replicaManager: ReplicaManager, log: UnifiedLog): Unit = {
    when(replicaManager.localLogOrException(t1p0)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t1p0)).thenReturn(partition)
    when(replicaManager.localLogOrException(t1p1)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t1p1)).thenReturn(partition)
    when(replicaManager.localLogOrException(t2p1)).thenReturn(log)
    when(replicaManager.getPartitionOrException(t2p1)).thenReturn(partition)
  }

  private def kafkaConfigNoTruncateOnFetch: KafkaConfig = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, KAFKA_2_6_IV0.version)
    KafkaConfig.fromProps(props)
  }
}
