/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import kafka.api.Request
import kafka.common.UnexpectedAppendOffsetException
import kafka.log.{LogConfig, LogManager, CleanerConfig}
import kafka.server._
import kafka.utils.{MockTime, TestUtils, MockScheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ReplicaNotAvailableException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.scalatest.Assertions.assertThrows
import org.easymock.EasyMock

import scala.collection.JavaConverters._

class PartitionTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val metrics = new Metrics

  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = _
  var logConfig: LogConfig = _

  @Before
  def setup(): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, CleanerConfig(enableCleaner = false), time)
    logManager.startup()

    val brokerProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    brokerProps.put(KafkaConfig.LogDirsProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    val brokerConfig = KafkaConfig.fromProps(brokerProps)
    val kafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    replicaManager = new ReplicaManager(
      config = brokerConfig, metrics, time, zkClient = kafkaZkClient, new MockScheduler(time),
      logManager, new AtomicBoolean(false), QuotaFactory.instantiate(brokerConfig, metrics, time, ""),
      brokerTopicStats, new MetadataCache(brokerId), new LogDirFailureChannel(brokerConfig.logDirs.size))

    EasyMock.expect(kafkaZkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(logProps).anyTimes()
    EasyMock.expect(kafkaZkClient.conditionalUpdatePath(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
      .andReturn((true, 0)).anyTimes()
    EasyMock.replay(kafkaZkClient)
  }

  @After
  def tearDown(): Unit = {
    brokerTopicStats.close()
    metrics.close()

    logManager.shutdown()
    Utils.delete(tmpDir)
    logManager.liveLogDirs.foreach(Utils.delete)
    replicaManager.shutdown(checkpointHW = false)
  }

  @Test
  def testMakeLeaderUpdatesEpochCache(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower = brokerId + 1
    val controllerId = brokerId + 3
    val replicas = List[Integer](leader, follower).asJava
    val isr = List[Integer](leader, follower).asJava
    val leaderEpoch = 8

    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)
    ), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 5,
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)
    assertTrue("Expected makeLeader to succeed",
      partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, leader, leaderEpoch,
        isr, 1, replicas, true), 0))

    assertEquals(Some(4), partition.leaderReplicaIfLocal.map(_.logEndOffset.messageOffset))

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(leaderEpoch)
    assertEquals(4, epochEndOffset.endOffset)
    assertEquals(leaderEpoch, epochEndOffset.leaderEpoch)
  }

  @Test
  // Verify that partition.removeFutureLocalReplica() and partition.maybeReplaceCurrentWithFutureReplica() can run concurrently
  def testMaybeReplaceCurrentWithFutureReplica(): Unit = {
    val latch = new CountDownLatch(1)

    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    val log1 = logManager.getOrCreateLog(topicPartition, logConfig)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    val log2 = logManager.getOrCreateLog(topicPartition, logConfig, isFuture = true)
    val currentReplica = new Replica(brokerId, topicPartition, time, log = Some(log1))
    val futureReplica = new Replica(Request.FutureLocalReplicaId, topicPartition, time, log = Some(log2))
    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)

    partition.addReplicaIfNotExists(futureReplica)
    partition.addReplicaIfNotExists(currentReplica)
    assertEquals(Some(currentReplica), partition.getReplica(brokerId))
    assertEquals(Some(futureReplica), partition.getReplica(Request.FutureLocalReplicaId))

    val thread1 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.removeFutureLocalReplica()
      }
    }

    val thread2 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.maybeReplaceCurrentWithFutureReplica()
      }
    }

    thread1.start()
    thread2.start()

    latch.countDown()
    thread1.join()
    thread2.join()
    assertEquals(None, partition.getReplica(Request.FutureLocalReplicaId))
  }


  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)
    partition.addReplicaIfNotExists(replica)
    assertEquals(Some(replica), partition.getReplica(replica.brokerId))

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(s"Log end offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, replica.logStartOffset)

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows[UnexpectedAppendOffsetException] {
      // append one record with offset = 3
      partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L), isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", initialLogStartOffset, replica.logEndOffset.messageOffset)

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
    assertEquals(s"Log end offset after append of 3 records with base offset $newLogStartOffset:", 7L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset after append of 3 records with base offset $newLogStartOffset:", newLogStartOffset, replica.logStartOffset)

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 7:", 8L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, replica.logStartOffset)

    // but we cannot append to offset < log start if the log is not empty
    assertThrows[UnexpectedAppendOffsetException] {
      val records2 = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                        new SimpleRecord("k2".getBytes, "v2".getBytes)),
                                   baseOffset = 3L)
      partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", 8L, replica.logEndOffset.messageOffset)

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 8:", 9L, replica.logEndOffset.messageOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, replica.logStartOffset)
  }

  @Test
  def testGetReplica(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    val replica = new Replica(brokerId, topicPartition, time, log = Some(log))
    val partition = new
        Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)

    assertEquals(None, partition.getReplica(brokerId))
    assertThrows[ReplicaNotAvailableException] {
      partition.getReplicaOrException(brokerId)
    }

    partition.addReplicaIfNotExists(replica)
    assertEquals(replica, partition.getReplicaOrException(brokerId))
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)
    assertThrows[ReplicaNotAvailableException] {
      partition.appendRecordsToFollowerOrFutureReplica(
           createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L), isFuture = false)
    }
  }

  @Test
  def testMakeFollowerWithNoLeaderIdChange(): Unit = {
    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)

    // Start off as follower
    var partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 1, List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    partition.makeFollower(0, partitionStateInfo, 0)

    // Request with same leader and epoch increases by more than 1, perform become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 3, List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    assertTrue(partition.makeFollower(0, partitionStateInfo, 1))

    // Request with same leader and epoch increases by only 1, skip become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 4, List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    assertFalse(partition.makeFollower(0, partitionStateInfo, 2))

    // Request with same leader and same epoch, skip become-follower steps
    partitionStateInfo = new LeaderAndIsrRequest.PartitionState(0, 1, 4, List[Integer](0, 1, 2).asJava, 1, List[Integer](0, 1, 2).asJava, false)
    assertFalse(partition.makeFollower(0, partitionStateInfo, 2))
  }

  @Test
  def testFollowerDoesNotJoinISRUntilCaughtUpToOffsetWithinCurrentLeaderEpoch(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val controllerId = brokerId + 3
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k4".getBytes, "v2".getBytes),
                                                  new SimpleRecord("k5".getBytes, "v3".getBytes)))
    val batch3 = TestUtils.records(records = List(new SimpleRecord("k6".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k7".getBytes, "v2".getBytes)))

    val partition = new Partition(topicPartition.topic, topicPartition.partition, time, replicaManager)
    assertTrue("Expected first makeLeader() to return 'leader changed'",
               partition.makeLeader(controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, leader, leaderEpoch, isr, 1, replicas, true), 0))
    assertEquals("Current leader epoch", leaderEpoch, partition.getLeaderEpoch)
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicas.map(_.brokerId))

    // after makeLeader(() call, partition should know about all the replicas
    val leaderReplica = partition.getReplica(leader).get
    val follower1Replica = partition.getReplica(follower1).get
    val follower2Replica = partition.getReplica(follower2).get

    // append records with initial leader epoch
    val lastOffsetOfFirstBatch = partition.appendRecordsToLeader(batch1, isFromClient = true).lastOffset
    partition.appendRecordsToLeader(batch2, isFromClient = true)
    assertEquals("Expected leader's HW not move", leaderReplica.logStartOffset, leaderReplica.highWatermark.messageOffset)

    // let the follower in ISR move leader's HW to move further but below LEO
    def readResult(fetchInfo: FetchDataInfo, leaderReplica: Replica): LogReadResult = {
      LogReadResult(info = fetchInfo,
                    highWatermark = leaderReplica.highWatermark.messageOffset,
                    leaderLogStartOffset = leaderReplica.logStartOffset,
                    leaderLogEndOffset = leaderReplica.logEndOffset.messageOffset,
                    followerLogStartOffset = 0,
                    fetchTimeMs = time.milliseconds,
                    readSize = 10240,
                    lastStableOffset = None)
    }
    partition.updateReplicaLogReadResult(
      follower2Replica, readResult(FetchDataInfo(LogOffsetMetadata(0), batch1), leaderReplica))
    partition.updateReplicaLogReadResult(
      follower2Replica, readResult(FetchDataInfo(LogOffsetMetadata(lastOffsetOfFirstBatch), batch2), leaderReplica))
    assertEquals("Expected leader's HW", lastOffsetOfFirstBatch, leaderReplica.highWatermark.messageOffset)

    // current leader becomes follower and then leader again (without any new records appended)
    partition.makeFollower(
      controllerId, new LeaderAndIsrRequest.PartitionState(controllerEpoch, follower2, leaderEpoch + 1, isr, 1, replicas, false), 1)
    assertTrue("Expected makeLeader() to return 'leader changed' after makeFollower()",
               partition.makeLeader(controllerEpoch, new LeaderAndIsrRequest.PartitionState(
                 controllerEpoch, leader, leaderEpoch + 2, isr, 1, replicas, false), 2))
    val currentLeaderEpochStartOffset = leaderReplica.logEndOffset.messageOffset

    // append records with the latest leader epoch
    partition.appendRecordsToLeader(batch3, isFromClient = true)

    // fetch from follower not in ISR from log start offset should not add this follower to ISR
    partition.updateReplicaLogReadResult(follower1Replica,
                                         readResult(FetchDataInfo(LogOffsetMetadata(0), batch1), leaderReplica))
    partition.updateReplicaLogReadResult(follower1Replica,
                                         readResult(FetchDataInfo(LogOffsetMetadata(lastOffsetOfFirstBatch), batch2), leaderReplica))
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicas.map(_.brokerId))

    // fetch from the follower not in ISR from start offset of the current leader epoch should
    // add this follower to ISR
    partition.updateReplicaLogReadResult(follower1Replica,
                                         readResult(FetchDataInfo(LogOffsetMetadata(currentLeaderEpochStartOffset), batch3), leaderReplica))
    assertEquals("ISR", Set[Integer](leader, follower1, follower2), partition.inSyncReplicas.map(_.brokerId))
 }

  def createRecords(records: Iterable[SimpleRecord], baseOffset: Long, partitionLeaderEpoch: Int = 0): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(
      buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.LOG_APPEND_TIME,
      baseOffset, time.milliseconds, partitionLeaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

}
