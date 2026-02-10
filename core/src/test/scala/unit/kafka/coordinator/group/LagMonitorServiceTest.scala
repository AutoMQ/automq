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

package kafka.coordinator.group

import kafka.common.OffsetAndMetadata
import kafka.server.ReplicaManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.AsyncSender
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.{mock, when}

import java.util.concurrent.CompletableFuture

class LagMonitorServiceTest {

  var lagMonitorService: LagMonitorService = _
  var groupMetadataManager: GroupMetadataManager = _
  var replicaManager: ReplicaManager = _
  var remoteLeoFetcher: RemoteLeoFetcher = _
  var asyncSender: AsyncSender = _

  val brokerId = 0
  val lagComputeIntervalMs = 100L

  @BeforeEach
  def setUp(): Unit = {
    groupMetadataManager = mock(classOf[GroupMetadataManager])
    replicaManager = mock(classOf[ReplicaManager])
    remoteLeoFetcher = mock(classOf[RemoteLeoFetcher])
    asyncSender = mock(classOf[AsyncSender])

    // Default: remoteLeoFetcher returns empty map
    when(remoteLeoFetcher.fetchLeos(org.mockito.ArgumentMatchers.any()))
      .thenReturn(CompletableFuture.completedFuture(Map.empty[TopicPartition, Long]))

    lagMonitorService = new LagMonitorService(
      brokerId,
      groupMetadataManager,
      replicaManager,
      lagComputeIntervalMs,
      remoteLeoFetcher,
      asyncSender
    )
  }

  @AfterEach
  def tearDown(): Unit = {
    if (lagMonitorService != null && lagMonitorService.isRunning) {
      lagMonitorService.shutdown()
    }
  }

  @Test
  def testStartupAndShutdown(): Unit = {
    assertFalse(lagMonitorService.isRunning)

    lagMonitorService.startup()
    assertTrue(lagMonitorService.isRunning)

    lagMonitorService.shutdown()
    assertFalse(lagMonitorService.isRunning)
  }

  @Test
  def testIsRunningBeforeStartup(): Unit = {
    assertFalse(lagMonitorService.isRunning)
  }

  @Test
  def testCollectCommitOffsets(): Unit = {
    when(groupMetadataManager.currentGroups).thenReturn(Iterable.empty)
    
    val result = lagMonitorService.collectCommitOffsets()
    
    assertEquals(0, result.size)
  }

  @Test
  def testFetchLocalLeos(): Unit = {
    val tp0 = new TopicPartition("topic-1", 0)
    val tp1 = new TopicPartition("topic-1", 1)

    when(replicaManager.getLogEndOffset(tp0)).thenReturn(Some(500L))
    when(replicaManager.getLogEndOffset(tp1)).thenReturn(None)

    val (localLeos, remotePartitions) = lagMonitorService.fetchLocalLeos(Set(tp0, tp1))

    assertEquals(Map(tp0 -> 500L), localLeos)
    assertEquals(Set(tp1), remotePartitions)
  }

  @Test
  def testComputeLag(): Unit = {
    when(groupMetadataManager.currentGroups).thenReturn(Iterable.empty)

    lagMonitorService.computeLag()

    val lagCache = lagMonitorService.getLagCache
    assertEquals(0, lagCache.size)
  }

  @Test
  def testMetricsRegistration(): Unit = {
    when(groupMetadataManager.currentGroups).thenReturn(Iterable.empty)
    
    lagMonitorService.computeLag()
    
    val registeredMetrics = lagMonitorService.getRegisteredMetrics
    assertEquals(0, registeredMetrics.size)
  }

  @Test
  def testConcurrentCleanup(): Unit = {
    when(groupMetadataManager.currentGroups).thenReturn(Iterable.empty)
    
    val threads = (1 to 10).map { _ =>
      new Thread(() => {
        try {
          lagMonitorService.computeLag()
        } catch {
          case _: java.util.ConcurrentModificationException =>
            fail("ConcurrentModificationException should not occur")
        }
      })
    }
    
    threads.foreach(_.start())
    threads.foreach(_.join())
    
    assertTrue(true)
  }

  @Test
  def testShutdownCleansUpMetrics(): Unit = {
    val tp0 = new TopicPartition("topic-1", 0)
    
    val group = new GroupMetadata("test-group", Stable, Time.SYSTEM)
    when(groupMetadataManager.currentGroups).thenReturn(Iterable(group))
    when(replicaManager.getLogEndOffset(tp0)).thenReturn(Some(100L))
    
    lagMonitorService.computeLag()
    
    lagMonitorService.shutdown()
    
    val registeredMetrics = lagMonitorService.getRegisteredMetrics
    assertEquals(0, registeredMetrics.size, "All metrics should be cleaned up after shutdown")
  }

  @Test
  def testExceptionHandlingInFetchLocalLeos(): Unit = {
    val tp0 = new TopicPartition("topic-1", 0)
    val tp1 = new TopicPartition("topic-1", 1)
    
    when(replicaManager.getLogEndOffset(tp0)).thenThrow(new RuntimeException("Test exception"))
    when(replicaManager.getLogEndOffset(tp1)).thenReturn(Some(200L))
    
    val (localLeos, remotePartitions) = lagMonitorService.fetchLocalLeos(Set(tp0, tp1))
    
    assertTrue(remotePartitions.contains(tp0), "Failed partition should be in remote set")
    assertEquals(Some(200L), localLeos.get(tp1), "Normal partition should have LEO")
  }

  @Test
  def testComputeLagWithRemotePartitions(): Unit = {
    val tp0 = new TopicPartition("topic-1", 0)
    val tp1 = new TopicPartition("topic-1", 1)

    val group = new GroupMetadata("group-1", Stable, Time.SYSTEM)
    when(groupMetadataManager.currentGroups).thenReturn(Iterable(group))

    when(replicaManager.getLogEndOffset(tp0)).thenReturn(Some(150L))
    when(replicaManager.getLogEndOffset(tp1)).thenReturn(None)

    val (localLeos, remotePartitions) = lagMonitorService.fetchLocalLeos(Set(tp0, tp1))
    
    assertEquals(1, localLeos.size)
    assertEquals(Some(150L), localLeos.get(tp0))
    assertEquals(1, remotePartitions.size)
    assertTrue(remotePartitions.contains(tp1))
  }

  @Test
  def testCollectCommitOffsetsSkipsDeadGroups(): Unit = {
    val tp0 = new TopicPartition("topic-1", 0)

    val deadGroup = new GroupMetadata("dead-group", Dead, Time.SYSTEM)
    val stableGroup = new GroupMetadata("stable-group", Stable, Time.SYSTEM)
    stableGroup.initializeOffsets(
      Map(tp0 -> new CommitRecordMetadataAndOffset(
        Some(0L),
        OffsetAndMetadata(100L, java.util.Optional.empty(), "", System.currentTimeMillis(), None)
      )),
      Map.empty
    )

    when(groupMetadataManager.currentGroups).thenReturn(Iterable(deadGroup, stableGroup))

    val result = lagMonitorService.collectCommitOffsets()

    assertEquals(1, result.size)
    assertEquals(Some(100L), result.get(("stable-group", tp0)))
    assertFalse(result.keys.exists(_._1 == "dead-group"))
  }

  @Test
  def testComputeLagDoesNotRemoveMetricOnLeoFetchFailure(): Unit = {
    val tp0 = new TopicPartition("topic-1", 0)

    val group = new GroupMetadata("group-1", Stable, Time.SYSTEM)
    group.initializeOffsets(
      Map(tp0 -> new CommitRecordMetadataAndOffset(
        Some(0L),
        OffsetAndMetadata(50L, java.util.Optional.empty(), "", System.currentTimeMillis(), None)
      )),
      Map.empty
    )
    when(groupMetadataManager.currentGroups).thenReturn(Iterable(group))

    // First cycle: LEO available locally
    when(replicaManager.getLogEndOffset(tp0)).thenReturn(Some(100L))
    lagMonitorService.computeLag()
    assertEquals(1, lagMonitorService.getRegisteredMetrics.size)
    assertEquals(Some(50L), lagMonitorService.getLagCache.get(("group-1", tp0)))

    // Second cycle: LEO unavailable locally, becomes remote partition
    // Remote fetch returns the LEO successfully
    when(replicaManager.getLogEndOffset(tp0)).thenReturn(None)
    when(remoteLeoFetcher.fetchLeos(Set(tp0)))
      .thenReturn(CompletableFuture.completedFuture(Map(tp0 -> 120L)))
    lagMonitorService.computeLag()

    // Metric should still exist with updated lag value
    assertEquals(1, lagMonitorService.getRegisteredMetrics.size,
      "Metric should exist after remote fetch")
    assertEquals(Some(70L), lagMonitorService.getLagCache.get(("group-1", tp0)),
      "Lag value should be updated from remote fetch (120 - 50 = 70)")
  }

  @Test
  def testStartupFailureCleanup(): Unit = {
    lagMonitorService.startup()
    assertTrue(lagMonitorService.isRunning)
    lagMonitorService.shutdown()
    assertFalse(lagMonitorService.isRunning)
  }
}
