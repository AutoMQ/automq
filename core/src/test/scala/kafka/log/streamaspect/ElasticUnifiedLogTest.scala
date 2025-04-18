/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.streamaspect

import com.automq.stream.api.Client
import kafka.log._
import kafka.log.remote.RemoteLogManager
import kafka.log.streamaspect.client.Context
import kafka.server.{BrokerTopicStats, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log._
import org.apache.kafka.storage.internals.utils.Throttler
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Tag, Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.File
import java.util.Optional
import scala.jdk.CollectionConverters.IterableHasAsScala

@Timeout(60)
@Tag("S3Unit")
class ElasticUnifiedLogTest extends UnifiedLogTest {
    var client: Client = _

    @BeforeEach
    override def setUp(): Unit = {
        ReadHint.markReadAll()
        val props = TestUtils.createSimpleEsBrokerConfig()
        config = KafkaConfig.fromProps(props)
        Context.enableTestMode()
        client = new MemoryClient();
    }

    override def createLog(dir: File, config: LogConfig,
        brokerTopicStats: BrokerTopicStats, logStartOffset: Long, recoveryPoint: Long,
        scheduler: Scheduler, time: Time,
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        producerIdExpirationCheckIntervalMs: Int, lastShutdownClean: Boolean,
        topicId: Option[Uuid] = Some(Uuid.ZERO_UUID), keepPartitionMetadataFile: Boolean,
        remoteStorageSystemEnable: Boolean,
        remoteLogManager: Option[RemoteLogManager],
        logOffsetsListener: LogOffsetsListener): ElasticUnifiedLog = {
        ElasticUnifiedLog.apply(
            dir,
            config,
            scheduler,
            time,
            maxTransactionTimeoutMs,
            producerStateManagerConfig,
            brokerTopicStats,
            producerIdExpirationCheckIntervalMs: Int,
            new LogDirFailureChannel(10),
            topicId,
            0,
            logOffsetsListener,

            client,
            "test_namespace",
            OpenStreamChecker.NOOP,
        )
    }

    override def testSegmentDeletionEnabledBeforeUploadToRemoteTierWhenLogStartOffsetMovedAhead(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testLogRecoversTopicId(): Unit = {
        // AutoMQ run in Kraft mode
    }

    override def testTruncateBelowFirstUnstableOffset(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testTopicIdTransfersAfterDirectoryRename(): Unit = {
        // AutoMQ don't have local file, so rename is not needed
    }

    override def testSplitOnOffsetOverflow(): Unit = {
        // Won't happen in AutoMQ
    }

    override def testReadWithMinMessage(): Unit = {
        // AutoMQ only has leader partition, the data integrity is guaranteed by S3Stream
    }

    override def testTruncateFullyAndStartBelowFirstUnstableOffset(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testFetchOffsetByTimestampFromRemoteStorage(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testAsyncDelete(): Unit = {
        // AutoMQ don't have local file
    }

    override def testAppendToTransactionIndexFailure(): Unit = {
        // AutoMQ don't have local file
    }

    override def testLogFailsWhenInconsistentTopicIdSet(): Unit = {
        // AutoMQ run in Kraft mode
    }

    @Test
    override def testEnablingVerificationWhenRequestIsAtLogLayer(): Unit = super.testEnablingVerificationWhenRequestIsAtLogLayer()

    @Test
    override def testDeleteSnapshotsOnIncrementLogStartOffset(): Unit = {
        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5)
        val log = createLog(logDir, logConfig)
        val pid1 = 1L
        val pid2 = 2L
        val epoch = 0.toShort

        log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "a".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord(mockTime.milliseconds(), "b".getBytes)), producerId = pid2,
            producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
        log.roll()

        assertEquals(2, log.activeProducersWithLastSequence.size)
        assertEquals(2, log.asInstanceOf[ElasticUnifiedLog].listProducerSnapshots().size())

        log.updateHighWatermark(log.logEndOffset)
        log.maybeIncrementLogStartOffset(2L, LogStartOffsetIncrementReason.ClientRecordDeletion)
        log.deleteOldSegments() // force retention to kick in so that the snapshot files are cleaned up.
        mockTime.sleep(logConfig.fileDeleteDelayMs + 1000) // advance the clock so file deletion takes place

        // Deleting records should not remove producer state but should delete snapshots after the file deletion delay.
        assertEquals(2, log.activeProducersWithLastSequence.size)
        assertEquals(1, log.asInstanceOf[ElasticUnifiedLog].listProducerSnapshots().size())
        val retainedLastSeqOpt = log.activeProducersWithLastSequence.get(pid2)
        assertTrue(retainedLastSeqOpt.isDefined)
        assertEquals(0, retainedLastSeqOpt.get)
    }

    @Test
    override def testReadOutOfRange(): Unit = {
        // TODO: construct a test case with records range [1024, 1025)
        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024)
        val log = createLog(logDir, logConfig)
        log.appendAsLeader(TestUtils.singletonRecords(value = "42".getBytes), leaderEpoch = 0)

        assertEquals(0, LogTestUtils.readLog(log, 1, 1000).records.sizeInBytes,
            "Reading at the log end offset should produce 0 byte read.")

        assertThrows(classOf[OffsetOutOfRangeException], () => LogTestUtils.readLog(log, 2, 1000))
    }

    @Test
    override def testHighWatermarkMaintenance(): Unit = {
        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
        val log = createLog(logDir, logConfig)
        val leaderEpoch = 0

        def records(offset: Long): MemoryRecords = TestUtils.records(List(
            new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
            new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
            new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
        ), baseOffset = offset, partitionLeaderEpoch = leaderEpoch)

        def assertHighWatermark(offset: Long): Unit = {
            assertEquals(offset, log.highWatermark)
            assertValidLogOffsetMetadata(log, log.fetchOffsetSnapshot.highWatermark)
        }

        // High watermark initialized to 0
        assertHighWatermark(0L)

        // High watermark not changed by append
        log.appendAsLeader(records(0), leaderEpoch)
        assertHighWatermark(0L)

        // Update high watermark as leader
        log.maybeIncrementHighWatermark(new LogOffsetMetadata(1L))
        assertHighWatermark(1L)

        // Cannot update past the log end offset
        log.updateHighWatermark(5L)
        assertHighWatermark(3L)

        // Update high watermark as follower
        log.appendAsFollower(records(3L))
        log.updateHighWatermark(6L)
        assertHighWatermark(6L)

        // AutoMQ remove truncate case
    }

    override def testLogReinitializeAfterManualDelete(): Unit = {
        // AutoMQ only has leader partition, the Record from append will be continuous.
    }

    override def testReadAtLogGap(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testTopicIdFlushesBeforeDirectoryRename(): Unit = {
        // AutoMQ run in Kraft mode, the topicId is provided by initial log creation
    }

    override def testIndexResizingAtTruncation(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    @Test
    override def testLogOffsetsListener(): Unit = {
        def records(offset: Long): MemoryRecords = TestUtils.records(List(
            new SimpleRecord(mockTime.milliseconds, "a".getBytes, "value".getBytes),
            new SimpleRecord(mockTime.milliseconds, "b".getBytes, "value".getBytes),
            new SimpleRecord(mockTime.milliseconds, "c".getBytes, "value".getBytes)
        ), baseOffset = offset, partitionLeaderEpoch = 0)

        val listener = new MockLogOffsetsListener()
        listener.verify()

        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 1024 * 1024)
        val log = createLog(logDir, logConfig, logOffsetsListener = listener)

        listener.verify(expectedHighWatermark = 0)

        log.appendAsLeader(records(0), 0)
        log.appendAsLeader(records(0), 0)

        log.maybeIncrementHighWatermark(new LogOffsetMetadata(4))
        listener.verify(expectedHighWatermark = 4)

        // AutoMQ remove truncate case
    }

    override def testProducerIdMapTruncateToWithNoSnapshots(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testThatGarbageCollectingSegmentsDoesntChangeOffset(): Unit = {
        // TODO: support UnifiedLog#delete and maybe use another method name to avoid missing up to Kafka local replica deletion.
    }

    @Test
    override def testCompactionDeletesProducerStateSnapshots(): Unit = {
        // TODO: After adaptor log.Cleaner, unmask the test
        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, cleanupPolicy = TopicConfig.CLEANUP_POLICY_COMPACT, fileDeleteDelayMs = 0)
        val log = createLog(logDir, logConfig)
        val pid1 = 1L
        val epoch = 0.toShort
        val cleaner = new Cleaner(id = 0,
            offsetMap = new FakeOffsetMap(Int.MaxValue),
            ioBufferSize = 64 * 1024,
            maxIoBufferSize = 64 * 1024,
            dupBufferLoadFactor = 0.75,
            throttler = new Throttler(Double.MaxValue, Long.MaxValue, "throttler", "entries", mockTime),
            time = mockTime,
            checkDone = _ => {})

        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "a".getBytes())), producerId = pid1,
            producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "b".getBytes())), producerId = pid1,
            producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes, "c".getBytes())), producerId = pid1,
            producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
        log.updateHighWatermark(log.logEndOffset)
        // AutoMQ change the way to get snapshots
        assertEquals(log.logSegments.asScala.map(_.baseOffset).toSeq.sorted.drop(1), log.listProducerSnapshots().keySet().asScala.toList,
            "expected a snapshot file per segment base offset, except the first segment")
        assertEquals(2, log.listProducerSnapshots().size())

        // Clean segments, this should delete everything except the active segment since there only
        // exists the key "a".
        cleaner.clean(LogToClean(log.topicPartition, log, 0, log.logEndOffset))
        log.deleteOldSegments()
        // Sleep to breach the file delete delay and run scheduled file deletion tasks
        mockTime.sleep(1)
        assertEquals(log.logSegments.asScala.map(_.baseOffset).toSeq.sorted.drop(1), log.listProducerSnapshots().keySet().asScala.toList,
            "expected a snapshot file per segment base offset, excluding the first")
    }

    override def testProducerSnapshotAfterSegmentRollOnAppend(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testAppendEmptyLogBelowLogStartOffsetThrowsException(): Unit = {
        // AutoMQ don't have local file
    }

    override def testDegenerateSegmentSplitWithOutOfRangeBatchLastOffset(): Unit = {
        // AutoMQ won't overflow
    }

    override def testRebuildProducerStateWithEmptyCompactedBatch(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testRenamingDirWithoutReinitialization(): Unit = {
        // AutoMQ don't have local file
    }

    override def testFlushingEmptyActiveSegments(): Unit = {
        // AutoMQ don't have local file
    }

    override def shouldTruncateLeaderEpochCheckpointFileWhenTruncatingLog(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    @Test
    override def testLogStartOffsetMovementDeletesSnapshots(): Unit = {
        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = -1, fileDeleteDelayMs = 0)
        val log = createLog(logDir, logConfig)
        val pid1 = 1L
        val epoch = 0.toShort

        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
        log.updateHighWatermark(log.logEndOffset)
        assertEquals(2, log.asInstanceOf[ElasticUnifiedLog].listProducerSnapshots().size())

        // Increment the log start offset to exclude the first two segments.
        log.maybeIncrementLogStartOffset(log.logEndOffset - 1, LogStartOffsetIncrementReason.ClientRecordDeletion)
        log.deleteOldSegments()
        // Sleep to breach the file delete delay and run scheduled file deletion tasks
        mockTime.sleep(1)
        assertEquals(1, log.asInstanceOf[ElasticUnifiedLog].listProducerSnapshots().size(),
            "expect a single producer state snapshot remaining")
    }

    @Test
    override def testRollSegmentThatAlreadyExists(): Unit = {
        val logConfig = LogTestUtils.createLogConfig(segmentMs = 1 * 60 * 60L)

        // create a log
        val log = createLog(logDir, logConfig)
        assertEquals(1, log.numberOfSegments, "Log begins with a single empty segment.")

        // roll active segment with the same base offset of size zero should recreate the segment
        log.roll(Some(0L))
        assertEquals(1, log.numberOfSegments, "Expect 1 segment after roll() empty segment with base offset.")

        // should be able to append records to active segment
        val records = TestUtils.records(
            List(new SimpleRecord(mockTime.milliseconds, "k1".getBytes, "v1".getBytes)),
            baseOffset = 0L, partitionLeaderEpoch = 0)
        log.appendAsFollower(records)
        assertEquals(1, log.numberOfSegments, "Expect one segment.")
        assertEquals(0L, log.activeSegment.baseOffset)

        // make sure we can append more records
        val records2 = TestUtils.records(
            List(new SimpleRecord(mockTime.milliseconds + 10, "k2".getBytes, "v2".getBytes)),
            baseOffset = 1L, partitionLeaderEpoch = 0)
        log.appendAsFollower(records2)

        assertEquals(2, log.logEndOffset, "Expect two records in the log")
        assertEquals(0, LogTestUtils.readLog(log, 0, 1).records.batches.iterator.next().lastOffset)
        assertEquals(1, LogTestUtils.readLog(log, 1, 1).records.batches.iterator.next().lastOffset)

        // roll so that active segment is empty
        log.roll()
        assertEquals(2L, log.activeSegment.baseOffset, "Expect base offset of active segment to be LEO")
        assertEquals(2, log.numberOfSegments, "Expect two segments.")

        // manually resize offset index to force roll of an empty active segment on next append
        log.activeSegment.asInstanceOf[ElasticLogSegment].forceRoll()
        val records3 = TestUtils.records(
            List(new SimpleRecord(mockTime.milliseconds + 12, "k3".getBytes, "v3".getBytes)),
            baseOffset = 2L, partitionLeaderEpoch = 0)
        log.appendAsFollower(records3)
        // AutoMQ don't have isolated offset index file
        assertEquals(2, LogTestUtils.readLog(log, 2, 1).records.batches.iterator.next().lastOffset)
        assertEquals(2, log.numberOfSegments, "Expect two segments.")
    }

    override def testDegenerateSegmentSplit(): Unit = {
        // AutoMQ won't overflow
    }

    @Test
    override def testDeleteOldSegments(): Unit = {
        def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds - 1000)

        val logConfig = LogTestUtils.createLogConfig(segmentBytes = createRecords.sizeInBytes * 5, segmentIndexBytes = 1000, retentionMs = 999)
        val log = createLog(logDir, logConfig)

        // append some messages to create some segments
        for (_ <- 0 until 100)
            log.appendAsLeader(createRecords, leaderEpoch = 0)

        log.maybeAssignEpochStartOffset(0, 40)
        log.maybeAssignEpochStartOffset(1, 90)

        // segments are not eligible for deletion if no high watermark has been set
        val numSegments = log.numberOfSegments
        log.deleteOldSegments()
        assertEquals(numSegments, log.numberOfSegments)
        assertEquals(0L, log.logStartOffset)

        // only segments with offset before the current high watermark are eligible for deletion
        for (hw <- 25 to 30) {
            log.updateHighWatermark(hw)
            log.deleteOldSegments()
            assertTrue(log.logStartOffset <= hw)
            log.logSegments.forEach { segment =>
                val segmentFetchInfo = segment.read(segment.baseOffset, Int.MaxValue)
                val segmentLastOffsetOpt = segmentFetchInfo.records.records.asScala.lastOption.map(_.offset)
                segmentLastOffsetOpt.foreach { lastOffset =>
                    assertTrue(lastOffset >= hw)
                }
            }
        }

        // expire all segments
        log.updateHighWatermark(log.logEndOffset)
        log.deleteOldSegments()
        assertEquals(1, log.numberOfSegments, "The deleted segments should be gone.")
        assertEquals(1, epochCache(log).epochEntries.size, "Epoch entries should have gone.")
        assertEquals(new EpochEntry(1, 100), epochCache(log).epochEntries.get(0), "Epoch entry should be the latest epoch and the leo.")

        // append some messages to create some segments
        for (_ <- 0 until 100)
            log.appendAsLeader(createRecords, leaderEpoch = 0)
    }

    override def testProducerIdMapTruncateTo(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testTruncateFullyAndStart(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    @ParameterizedTest(name = "testRetentionDeletesProducerStateSnapshots with createEmptyActiveSegment: {0}")
    @ValueSource(booleans = Array(true, false))
    override def testRetentionDeletesProducerStateSnapshots(createEmptyActiveSegment: Boolean): Unit = {
        val logConfig = LogTestUtils.createLogConfig(segmentBytes = 2048 * 5, retentionBytes = 0, retentionMs = 1000 * 60, fileDeleteDelayMs = 0)
        val log = createLog(logDir, logConfig)
        val pid1 = 1L
        val epoch = 0.toShort

        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("a".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 0), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("b".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 1), leaderEpoch = 0)
        log.roll()
        log.appendAsLeader(TestUtils.records(List(new SimpleRecord("c".getBytes)), producerId = pid1,
            producerEpoch = epoch, sequence = 2), leaderEpoch = 0)
        if (createEmptyActiveSegment) {
            log.roll()
        }

        log.updateHighWatermark(log.logEndOffset)

        val numProducerSnapshots = if (createEmptyActiveSegment) 3 else 2
        assertEquals(numProducerSnapshots, log.asInstanceOf[ElasticUnifiedLog].listProducerSnapshots().size())
        // Sleep to breach the retention period
        mockTime.sleep(1000 * 60 + 1)
        log.deleteOldSegments()
        // Sleep to breach the file delete delay and run scheduled file deletion tasks
        mockTime.sleep(1)
        assertEquals(1, log.asInstanceOf[ElasticUnifiedLog].listProducerSnapshots().size(),
            "expect a single producer state snapshot remaining")
    }

    override def testLoadingLogDeletesProducerStateSnapshotsPastLogEndOffset(): Unit = {
        // AutoMQ don't have local file
    }

    override def testWriteLeaderEpochCheckpointAfterDirectoryRename(): Unit = {
        // AutoMQ don't have local file
    }

    override def testRebuildProducerIdMapWithCompactedData(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testNoOpWhenKeepPartitionMetadataFileIsFalse(): Unit = {
        // AutoMQ run in Kraft mode, the topicId is provided by initial log creation
    }

    @Test
    override def testTransactionIndexUpdatedThroughReplication(): Unit = super.testTransactionIndexUpdatedThroughReplication()

    override def testStartOffsetsRemoteLogStorageIsEnabled(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testInitializationOfProducerSnapshotsUpgradePath(): Unit = {
        // AutoMQ don't have local file
    }

    override def testTruncateTo(): Unit = {
        // AutoMQ embedded replication in S3Stream, so AutoMQ don't need truncateTo
    }

    override def testActiveSegmentDeletionDueToRetentionTimeBreachWithRemoteStorage(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testLogDeletionAfterClose(): Unit = {
        // TODO: support UnifiedLog#delete
    }

    override def testAppendToOrReadFromLogInFailedLogDir(): Unit = {
        // AutoMQ don't have local file
    }

    @Test
    override def testTransactionIndexUpdated(): Unit = super.testTransactionIndexUpdated()

    override def testProducerExpireCheckAfterDelete(): Unit = {
        // TODO: support UnifiedLog#delete
    }

    override def testLogFlushesPartitionMetadataOnClose(): Unit = {
        // AutoMQ don't have local file
    }

    override def testReadWithTooSmallMaxLength(): Unit = {
        // AutoMQ
    }

    @Test
    override def testFetchUpToLastStableOffset(): Unit = super.testFetchUpToLastStableOffset()

    override protected def assertValidLogOffsetMetadata(log: UnifiedLog,
        offsetMetadata: LogOffsetMetadata): Unit = {
        assertFalse(offsetMetadata.messageOffsetOnly)

        val segmentBaseOffset = offsetMetadata.segmentBaseOffset
        val segmentOpt = log.logSegments(segmentBaseOffset, segmentBaseOffset + 1).headOption
        assertTrue(segmentOpt.isDefined)

        val segment = segmentOpt.get
        assertEquals(segmentBaseOffset, segment.baseOffset)
        assertTrue(offsetMetadata.relativePositionInSegment <= segment.size)

        val readInfo = segment.read(offsetMetadata.messageOffset,
            2048,
            Optional.of(segment.size),
            false)

        if (offsetMetadata.relativePositionInSegment < segment.size) {
            // AutoMQ Record don't have physical position
            assertEquals(offsetMetadata.messageOffset, readInfo.fetchOffsetMetadata.messageOffset)
        } else
            assertNull(readInfo)
    }

    override def testFetchLatestTieredTimestampWithRemoteStorage(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testIncrementLocalLogStartOffsetAfterLocalLogDeletion(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testConvertToOffsetMetadataDoesNotThrowOffsetOutOfRangeError(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testRetentionOnLocalLogDeletionWhenRemoteLogCopyEnabledAndDefaultLocalRetentionBytes(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }


    override def testRetentionOnLocalLogDeletionWhenRemoteLogCopyDisabled(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

    override def testRetentionOnLocalLogDeletionWhenRemoteLogCopyEnabledAndDefaultLocalRetentionMs(): Unit = {
        // AutoMQ embedded tiered storage in S3Stream
    }

}
