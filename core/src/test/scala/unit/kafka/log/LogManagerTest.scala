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

package kafka.log

import java.io._
import java.util.{Collections, Properties}

import kafka.server.{FetchDataInfo, FetchLogEnd}
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils._
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, spy}

import scala.collection.mutable
import scala.util.{Failure, Try}

class LogManagerTest {

  val time = new MockTime()
  val maxRollInterval = 100
  val maxLogAgeMs = 10 * 60 * 1000
  val logProps = new Properties()
  logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
  logProps.put(LogConfig.SegmentIndexBytesProp, 4096: java.lang.Integer)
  logProps.put(LogConfig.RetentionMsProp, maxLogAgeMs: java.lang.Integer)
  logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)
  val logConfig = LogConfig(logProps)
  var logDir: File = null
  var logManager: LogManager = null
  val name = "kafka"
  val veryLargeLogFlushInterval = 10000000L

  @Before
  def setUp(): Unit = {
    logDir = TestUtils.tempDir()
    logManager = createLogManager()
    logManager.startup()
  }

  @After
  def tearDown(): Unit = {
    if (logManager != null)
      logManager.shutdown()
    Utils.delete(logDir)
    // Some tests assign a new LogManager
    logManager.liveLogDirs.foreach(Utils.delete)
  }

  /**
   * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
   */
  @Test
  def testCreateLog(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => logConfig)
    assertEquals(1, logManager.liveLogDirs.size)

    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
   * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
   * The LogManager is configured with one invalid log directory which should be marked as offline.
   */
  @Test
  def testCreateLogWithInvalidLogDir(): Unit = {
    // Configure the log dir with the Nul character as the path, which causes dir.getCanonicalPath() to throw an
    // IOException. This simulates the scenario where the disk is not properly mounted (which is hard to achieve in
    // a unit test)
    val dirs = Seq(logDir, new File("\u0000"))

    logManager.shutdown()
    logManager = createLogManager(dirs)
    logManager.startup()

    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => logConfig, isNew = true)
    val logFile = new File(logDir, name + "-0")
    assertTrue(logFile.exists)
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  @Test
  def testCreateLogWithLogDirFallback(): Unit = {
    // Configure a number of directories one level deeper in logDir,
    // so they all get cleaned up in tearDown().
    val dirs = (0 to 4)
      .map(_.toString)
      .map(logDir.toPath.resolve(_).toFile)

    // Create a new LogManager with the configured directories and an overridden createLogDirectory.
    logManager.shutdown()
    logManager = spy(createLogManager(dirs))
    val brokenDirs = mutable.Set[File]()
    doAnswer { invocation =>
      // The first half of directories tried will fail, the rest goes through.
      val logDir = invocation.getArgument[File](0)
      if (brokenDirs.contains(logDir) || brokenDirs.size < dirs.length / 2) {
        brokenDirs.add(logDir)
        Failure(new Throwable("broken dir"))
      } else {
        invocation.callRealMethod().asInstanceOf[Try[File]]
      }
    }.when(logManager).createLogDirectory(any(), any())
    logManager.startup()

    // Request creating a new log.
    // LogManager should try using all configured log directories until one succeeds.
    logManager.getOrCreateLog(new TopicPartition(name, 0), () => logConfig, isNew = true)

    // Verify that half the directories were considered broken,
    assertEquals(dirs.length / 2, brokenDirs.size)

    // and that exactly one log file was created,
    val containsLogFile: File => Boolean = dir => new File(dir, name + "-0").exists()
    assertEquals("More than one log file created", 1, dirs.count(containsLogFile))

    // and that it wasn't created in one of the broken directories.
    assertFalse(brokenDirs.exists(containsLogFile))
  }

  /**
   * Test that get on a non-existent returns None and no log is created.
   */
  @Test
  def testGetNonExistentLog(): Unit = {
    val log = logManager.getLog(new TopicPartition(name, 0))
    assertEquals("No log should be found.", None, log)
    val logFile = new File(logDir, name + "-0")
    assertTrue(!logFile.exists)
  }

  /**
   * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
   */
  @Test
  def testCleanupExpiredSegments(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => logConfig)
    var offset = 0L
    for(_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
    }
    assertTrue("There should be more than one segment now.", log.numberOfSegments > 1)
    log.updateHighWatermark(log.logEndOffset)

    log.logSegments.foreach(_.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals("Now there should only be only one segment in the index.", 1, log.numberOfSegments)
    time.sleep(log.config.fileDeleteDelayMs + 1)

    log.logSegments.foreach(s => {
      s.lazyOffsetIndex.get
      s.lazyTimeIndex.get
    })

    // there should be a log file, two indexes, one producer snapshot, and the leader epoch checkpoint
    assertEquals("Files should have been deleted", log.numberOfSegments * 4 + 1, log.dir.list.length)
    assertEquals("Should get empty fetch off new log.", 0, readLog(log, offset + 1).records.sizeInBytes)

    try {
      readLog(log, 0)
      fail("Should get exception from fetching earlier.")
    } catch {
      case _: OffsetOutOfRangeException => // This is good.
    }
    // log should still be appendable
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
   * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
   */
  @Test
  def testCleanupSegmentsToMaintainSize(): Unit = {
    val setSize = TestUtils.singletonRecords("test".getBytes()).sizeInBytes
    logManager.shutdown()
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 10 * setSize: java.lang.Integer)
    logProps.put(LogConfig.RetentionBytesProp, 5L * 10L * setSize + 10L: java.lang.Long)
    val config = LogConfig.fromProps(logConfig.originals, logProps)

    logManager = createLogManager()
    logManager.startup()

    // create a log
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => config)
    var offset = 0L

    // add a bunch of messages that should be larger than the retentionSize
    val numMessages = 200
    for (_ <- 0 until numMessages) {
      val set = TestUtils.singletonRecords("test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.firstOffset.get
    }

    log.updateHighWatermark(log.logEndOffset)
    assertEquals("Check we have the expected number of segments.", numMessages * setSize / config.segmentSize, log.numberOfSegments)

    // this cleanup shouldn't find any expired segments but should delete some to reduce size
    time.sleep(logManager.InitialTaskDelayMs)
    assertEquals("Now there should be exactly 6 segments", 6, log.numberOfSegments)
    time.sleep(log.config.fileDeleteDelayMs + 1)

    // there should be a log file, two indexes (the txn index is created lazily),
    // the leader epoch checkpoint and two producer snapshot files (one for the active and previous segments)
    assertEquals("Files should have been deleted", log.numberOfSegments * 3 + 3, log.dir.list.length)
    assertEquals("Should get empty fetch off new log.", 0, readLog(log, offset + 1).records.sizeInBytes)
    try {
      readLog(log, 0)
      fail("Should get exception from fetching earlier.")
    } catch {
      case _: OffsetOutOfRangeException => // This is good.
    }
    // log should still be appendable
    log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)
  }

  /**
    * Ensures that LogManager doesn't run on logs with cleanup.policy=compact,delete
    * LogCleaner.CleanerThread handles all logs where compaction is enabled.
    */
  @Test
  def testDoesntCleanLogsWithCompactDeletePolicy(): Unit = {
    testDoesntCleanLogs(LogConfig.Compact + "," + LogConfig.Delete)
  }

  /**
    * Ensures that LogManager doesn't run on logs with cleanup.policy=compact
    * LogCleaner.CleanerThread handles all logs where compaction is enabled.
    */
  @Test
  def testDoesntCleanLogsWithCompactPolicy(): Unit = {
    testDoesntCleanLogs(LogConfig.Compact)
  }

  private def testDoesntCleanLogs(policy: String): Unit = {
    val logProps = new Properties()
    logProps.put(LogConfig.CleanupPolicyProp, policy)
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => LogConfig.fromProps(logConfig.originals, logProps))
    var offset = 0L
    for (_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes(), key="test".getBytes())
      val info = log.appendAsLeader(set, leaderEpoch = 0)
      offset = info.lastOffset
    }

    val numSegments = log.numberOfSegments
    assertTrue("There should be more than one segment now.", log.numberOfSegments > 1)

    log.logSegments.foreach(_.log.file.setLastModified(time.milliseconds))

    time.sleep(maxLogAgeMs + 1)
    assertEquals("number of segments shouldn't have changed", numSegments, log.numberOfSegments)
  }

  /**
   * Test that flush is invoked by the background scheduler thread.
   */
  @Test
  def testTimeBasedFlush(): Unit = {
    logManager.shutdown()
    val logProps = new Properties()
    logProps.put(LogConfig.FlushMsProp, 1000: java.lang.Integer)
    val config = LogConfig.fromProps(logConfig.originals, logProps)

    logManager = createLogManager()
    logManager.startup()
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => config)
    val lastFlush = log.lastFlushTime
    for (_ <- 0 until 200) {
      val set = TestUtils.singletonRecords("test".getBytes())
      log.appendAsLeader(set, leaderEpoch = 0)
    }
    time.sleep(logManager.InitialTaskDelayMs)
    assertTrue("Time based flush should have been triggered", lastFlush != log.lastFlushTime)
  }

  /**
   * Test that new logs that are created are assigned to the least loaded log directory
   */
  @Test
  def testLeastLoadedAssignment(): Unit = {
    // create a log manager with multiple data directories
    val dirs = Seq(TestUtils.tempDir(),
                     TestUtils.tempDir(),
                     TestUtils.tempDir())
    logManager.shutdown()
    logManager = createLogManager(dirs)

    // verify that logs are always assigned to the least loaded partition
    for(partition <- 0 until 20) {
      logManager.getOrCreateLog(new TopicPartition("test", partition), () => logConfig)
      assertEquals("We should have created the right number of logs", partition + 1, logManager.allLogs.size)
      val counts = logManager.allLogs.groupBy(_.dir.getParent).values.map(_.size)
      assertTrue("Load should balance evenly", counts.max <= counts.min + 1)
    }
  }

  /**
   * Test that it is not possible to open two log managers using the same data directory
   */
  @Test
  def testTwoLogManagersUsingSameDirFails(): Unit = {
    try {
      createLogManager()
      fail("Should not be able to create a second log manager instance with the same data directory")
    } catch {
      case _: KafkaException => // this is good
    }
  }

  /**
   * Test that recovery points are correctly written out to disk
   */
  @Test
  def testCheckpointRecoveryPoints(): Unit = {
    verifyCheckpointRecovery(Seq(new TopicPartition("test-a", 1), new TopicPartition("test-b", 1)), logManager, logDir)
  }

  /**
   * Test that recovery points directory checking works with trailing slash
   */
  @Test
  def testRecoveryDirectoryMappingWithTrailingSlash(): Unit = {
    logManager.shutdown()
    logManager = TestUtils.createLogManager(logDirs = Seq(new File(TestUtils.tempDir().getAbsolutePath + File.separator)))
    logManager.startup()
    verifyCheckpointRecovery(Seq(new TopicPartition("test-a", 1)), logManager, logManager.liveLogDirs.head)
  }

  /**
   * Test that recovery points directory checking works with relative directory
   */
  @Test
  def testRecoveryDirectoryMappingWithRelativeDirectory(): Unit = {
    logManager.shutdown()
    logManager = createLogManager(Seq(new File("data", logDir.getName).getAbsoluteFile))
    logManager.startup()
    verifyCheckpointRecovery(Seq(new TopicPartition("test-a", 1)), logManager, logManager.liveLogDirs.head)
  }

  private def verifyCheckpointRecovery(topicPartitions: Seq[TopicPartition], logManager: LogManager, logDir: File): Unit = {
    val logs = topicPartitions.map(logManager.getOrCreateLog(_, () => logConfig))
    logs.foreach { log =>
      for (_ <- 0 until 50)
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes()), leaderEpoch = 0)

      log.flush()
    }

    logManager.checkpointLogRecoveryOffsets()
    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile)).read()

    topicPartitions.zip(logs).foreach { case (tp, log) =>
      assertEquals("Recovery point should equal checkpoint", checkpoints(tp), log.recoveryPoint)
      assertEquals(Some(log.minSnapshotsOffsetToRetain), log.oldestProducerSnapshotOffset)
    }
  }

  private def createLogManager(logDirs: Seq[File] = Seq(this.logDir)): LogManager = {
    TestUtils.createLogManager(
      defaultConfig = logConfig,
      logDirs = logDirs,
      time = this.time)
  }

  @Test
  def testFileReferencesAfterAsyncDelete(): Unit = {
    val log = logManager.getOrCreateLog(new TopicPartition(name, 0), () => logConfig)
    val activeSegment = log.activeSegment
    val logName = activeSegment.log.file.getName
    val indexName = activeSegment.offsetIndex.file.getName
    val timeIndexName = activeSegment.timeIndex.file.getName
    val txnIndexName = activeSegment.txnIndex.file.getName
    val indexFilesOnDiskBeforeDelete = activeSegment.log.file.getParentFile.listFiles.filter(_.getName.endsWith("index"))

    val removedLog = logManager.asyncDelete(new TopicPartition(name, 0))
    val removedSegment = removedLog.activeSegment
    val indexFilesAfterDelete = Seq(removedSegment.lazyOffsetIndex.file, removedSegment.lazyTimeIndex.file,
      removedSegment.txnIndex.file)

    assertEquals(new File(removedLog.dir, logName), removedSegment.log.file)
    assertEquals(new File(removedLog.dir, indexName), removedSegment.lazyOffsetIndex.file)
    assertEquals(new File(removedLog.dir, timeIndexName), removedSegment.lazyTimeIndex.file)
    assertEquals(new File(removedLog.dir, txnIndexName), removedSegment.txnIndex.file)

    // Try to detect the case where a new index type was added and we forgot to update the pointer
    // This will only catch cases where the index file is created eagerly instead of lazily
    indexFilesOnDiskBeforeDelete.foreach { fileBeforeDelete =>
      val fileInIndex = indexFilesAfterDelete.find(_.getName == fileBeforeDelete.getName)
      assertEquals(s"Could not find index file ${fileBeforeDelete.getName} in indexFilesAfterDelete",
        Some(fileBeforeDelete.getName), fileInIndex.map(_.getName))
      assertNotEquals("File reference was not updated in index", fileBeforeDelete.getAbsolutePath,
        fileInIndex.get.getAbsolutePath)
    }

    time.sleep(logManager.InitialTaskDelayMs)
    assertTrue("Logs deleted too early", logManager.hasLogsToBeDeleted)
    time.sleep(logManager.currentDefaultConfig.fileDeleteDelayMs - logManager.InitialTaskDelayMs)
    assertFalse("Logs not deleted", logManager.hasLogsToBeDeleted)
  }

  @Test
  def testCreateAndDeleteOverlyLongTopic(): Unit = {
    val invalidTopicName = String.join("", Collections.nCopies(253, "x"))
    logManager.getOrCreateLog(new TopicPartition(invalidTopicName, 0), () => logConfig)
    logManager.asyncDelete(new TopicPartition(invalidTopicName, 0))
  }

  @Test
  def testCheckpointForOnlyAffectedLogs(): Unit = {
    val tps = Seq(
      new TopicPartition("test-a", 0),
      new TopicPartition("test-a", 1),
      new TopicPartition("test-a", 2),
      new TopicPartition("test-b", 0),
      new TopicPartition("test-b", 1))

    val allLogs = tps.map(logManager.getOrCreateLog(_, () => logConfig))
    allLogs.foreach { log =>
      for (_ <- 0 until 50)
        log.appendAsLeader(TestUtils.singletonRecords("test".getBytes), leaderEpoch = 0)
      log.flush()
    }

    logManager.checkpointRecoveryOffsetsAndCleanSnapshot(this.logDir, allLogs.filter(_.dir.getName.contains("test-a")))

    val checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RecoveryPointCheckpointFile)).read()

    tps.zip(allLogs).foreach { case (tp, log) =>
      assertEquals("Recovery point should equal checkpoint", checkpoints(tp), log.recoveryPoint)
      if (tp.topic.equals("test-a")) // should only cleanup old producer snapshots for topic 'test-a'
        assertEquals(Some(log.minSnapshotsOffsetToRetain), log.oldestProducerSnapshotOffset)
      else
        assertNotEquals(Some(log.minSnapshotsOffsetToRetain), log.oldestProducerSnapshotOffset)
    }
  }

  private def readLog(log: Log, offset: Long, maxLength: Int = 1024): FetchDataInfo = {
    log.read(offset, maxLength, isolation = FetchLogEnd, minOneMessage = true)
  }

  /**
   * Test when a configuration of a topic is updated while its log is getting initialized,
   * the config is refreshed when log initialization is finished.
   */
  @Test
  def testTopicConfigChangeUpdatesLogConfig(): Unit = {
    val testTopicOne = "test-topic-one"
    val testTopicTwo = "test-topic-two"
    val testTopicOnePartition: TopicPartition = new TopicPartition(testTopicOne, 1)
    val testTopicTwoPartition: TopicPartition = new TopicPartition(testTopicTwo, 1)
    val mockLog: Log = EasyMock.mock(classOf[Log])

    logManager.initializingLog(testTopicOnePartition)
    logManager.initializingLog(testTopicTwoPartition)

    logManager.topicConfigUpdated(testTopicOne)

    val logConfig: LogConfig = null
    var configUpdated = false
    logManager.finishedInitializingLog(testTopicOnePartition, Some(mockLog), () => {
      configUpdated = true
      logConfig
    })
    assertTrue(configUpdated)

    var configNotUpdated = true
    logManager.finishedInitializingLog(testTopicTwoPartition, Some(mockLog), () => {
      configNotUpdated = false
      logConfig
    })
    assertTrue(configNotUpdated)
  }

  /**
   * Test if an error occurs when creating log, log manager removes corresponding
   * topic partition from the list of initializing partitions.
   */
  @Test
  def testConfigChangeGetsCleanedUp(): Unit = {
    val testTopicPartition: TopicPartition = new TopicPartition("test-topic", 1)

    logManager.initializingLog(testTopicPartition)

    val logConfig: LogConfig = null
    var configUpdateNotCalled = true
    logManager.finishedInitializingLog(testTopicPartition, None, () => {
      configUpdateNotCalled = false
      logConfig
    })

    assertTrue(logManager.partitionsInitializing.isEmpty)
    assertTrue(configUpdateNotCalled)
  }

  /**
   * Test when a broker configuration change happens all logs in process of initialization
   * pick up latest config when finished with initialization.
   */
  @Test
  def testBrokerConfigChangeDeliveredToAllLogs(): Unit = {
    val testTopicOne = "test-topic-one"
    val testTopicTwo = "test-topic-two"
    val testTopicOnePartition: TopicPartition = new TopicPartition(testTopicOne, 1)
    val testTopicTwoPartition: TopicPartition = new TopicPartition(testTopicTwo, 1)
    val mockLog: Log = EasyMock.mock(classOf[Log])

    logManager.initializingLog(testTopicOnePartition)
    logManager.initializingLog(testTopicTwoPartition)

    logManager.brokerConfigUpdated()

    val logConfig: LogConfig = null
    var totalChanges = 0
    logManager.finishedInitializingLog(testTopicOnePartition, Some(mockLog), () => {
      totalChanges += 1
      logConfig
    })
    logManager.finishedInitializingLog(testTopicTwoPartition, Some(mockLog), () => {
      totalChanges += 1
      logConfig
    })

    assertEquals(2, totalChanges)
  }

  /**
   * Test even if no log is getting initialized, if config change events are delivered
   * things continue to work correctly. This test should not throw.
   *
   * This makes sure that events can be delivered even when no log is getting initialized.
   */
  @Test
  def testConfigChangesWithNoLogGettingInitialized(): Unit = {
    logManager.brokerConfigUpdated()
    logManager.topicConfigUpdated("test-topic")
    assertTrue(logManager.partitionsInitializing.isEmpty)
  }
}
