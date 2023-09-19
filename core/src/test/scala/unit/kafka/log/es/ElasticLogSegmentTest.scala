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

package kafka.log.es

import kafka.log._
import kafka.log.streamaspect.{ElasticLogManager, ElasticLogSegment}
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.server.{KafkaConfig, LogDirFailureChannel}
import kafka.utils.TestUtils.checkEquals
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test}

import java.io.File
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters.MapHasAsJava

@Tag("esUnit")
class ElasticLogSegmentTest {
    private val topicPartition = new TopicPartition("topic", 0)
    private val segments = mutable.Map[Long, ElasticLogSegment]()
    private var logDir: File = _

    private val namespace = "test_ns"
    private val properties: Properties = TestUtils.createSimpleEsBrokerConfig(namespace = namespace)
    private val kafkaConfig: KafkaConfig = KafkaConfig.fromProps(properties)
    private val mockTime = new MockTime()
    private val logDirFailureChannel = new LogDirFailureChannel(10)
    private val producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs)

    /* get or create a segment with the given base offset */
    def getOrCreateSegment(offset: Long,
        indexIntervalBytes: Int = 10,
        time: Time = Time.SYSTEM): ElasticLogSegment = {
        val logProps = new Properties()
        logProps.put(LogConfig.IndexIntervalBytesProp, indexIntervalBytes: Integer)
        getOrCreateSegment(offset, LogConfig(logProps), time)
    }

    def getOrCreateSegment(offset: Long,
        logConfig: LogConfig,
        time: Time): ElasticLogSegment = {
        // make sure ElasticLog is generated.
        if (segments.isEmpty) {
            ElasticLogManager.getOrCreateLog(dir = logDir,
                config = logConfig,
                scheduler = mockTime.scheduler,
                time = mockTime,
                topicPartition = topicPartition,
                logDirFailureChannel = logDirFailureChannel,
                maxTransactionTimeoutMs = 5 * 60 * 1000,
                producerStateManagerConfig = producerStateManagerConfig,
                leaderEpoch = 0)
        }

        segments.getOrElseUpdate(offset, ElasticLogManager.newSegment(topicPartition, offset, time, ""))
    }

    /* create a ByteBufferMessageSet for the given messages starting from the given offset */
    def records(offset: Long, records: String*): MemoryRecords = {
        MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, offset, CompressionType.NONE, TimestampType.CREATE_TIME,
            records.map { s => new SimpleRecord(offset * 10, s.getBytes) }: _*)
    }

    @BeforeEach
    def setup(): Unit = {
        segments.clear()
        logDir = TestUtils.tempDir()
        ElasticLogManager.init(kafkaConfig, "fake_cluster_id", appendWithAsyncCallbacks = false)
    }

    @AfterEach
    def teardown(): Unit = {
        segments.values.foreach(_.close())
        ElasticLogManager.destroyLog(topicPartition, 0)
        Utils.delete(logDir)
    }

    /**
     * A read on an empty log segment should return no records
     */
    @Test
    def testReadOnEmptySegment(): Unit = {
        val seg = getOrCreateSegment(40)
        val read = seg.read(startOffset = 40, maxSize = 300)
        assertFalse(read.records.records().iterator().hasNext)
    }

    /**
     * Reading from before the first offset in the segment should return messages
     * beginning with the first message in the segment
     */
    @Test
    def testReadBeforeFirstOffset(): Unit = {
        val seg = getOrCreateSegment(40)
        val ms = records(40, "hello", "there", "little", "bee")
        seg.append(43, RecordBatch.NO_TIMESTAMP, -1L, ms)
        val read = seg.read(startOffset = 31, maxSize = 300).records
        checkEquals(ms.records.iterator, read.records.iterator)
    }

    /**
     * If we read from an offset beyond the last offset in the segment we should get null
     */
    @Test
    def testReadAfterLast(): Unit = {
        val seg = getOrCreateSegment(40)
        val ms = records(40, "hello", "there")
        seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, ms)
        val read = seg.read(startOffset = 52, maxSize = 200).records
        assertFalse(read.records().iterator().hasNext, "Read beyond the last offset in the segment should give no data")
    }

    /**
     * If we read from an offset which doesn't exist we should get a message set beginning
     * with the least offset greater than the given startOffset.
     */
    @Test
    def testReadFromGap(): Unit = {
        val seg = getOrCreateSegment(40)
        val ms = records(40, "hello", "there")
        seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, ms)
        val read = seg.read(startOffset = 35, maxSize = 200)
        checkEquals(ms.records.iterator, read.records.records.iterator)
    }

    /**
     * This test was intended to test truncation and rolling in LogSegment. However, truncation is not supported in
     * ElasticLogSegment. Therefore, we cut this test down to just verify the rolling and leave the test name as is.
     */
    @Test
    def testTruncateEmptySegment(): Unit = {
        // This tests the scenario in which the follower truncates to an empty segment. In this
        // case we must ensure that the index is resized so that the log segment is not mistakenly
        // rolled due to a full index

        val maxSegmentMs = 300000
        val time = new MockTime
        val seg = getOrCreateSegment(0, time = time)
        // Force load indexes before closing the segment
        seg.timeIndex
        seg.close()

        val reopened = getOrCreateSegment(0, time = time)
        assertEquals(0, seg.timeIndex.sizeInBytes)

        assertFalse(reopened.timeIndex.isFull)

        var rollParams = RollParams(maxSegmentMs, maxSegmentBytes = Int.MaxValue, RecordBatch.NO_TIMESTAMP,
            maxOffsetInMessages = 100L, messagesSize = 1024, time.milliseconds())
        assertFalse(reopened.shouldRoll(rollParams))

        // The segment should not be rolled even if maxSegmentMs has been exceeded
        time.sleep(maxSegmentMs + 1)
        assertEquals(maxSegmentMs + 1, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP))
        rollParams = RollParams(maxSegmentMs, maxSegmentBytes = Int.MaxValue, RecordBatch.NO_TIMESTAMP,
            maxOffsetInMessages = 100L, messagesSize = 1024, time.milliseconds())
        assertFalse(reopened.shouldRoll(rollParams))
    }

    @Test
    def testIndexEntries(): Unit = {
        val numMessages = 30
        val seg = getOrCreateSegment(40, 2 * records(0, "hello").sizeInBytes - 1)
        var offset = 40
        for (_ <- 0 until numMessages) {
            seg.append(offset, offset, offset, records(offset, "hello"))
            offset += 1
        }
        assertEquals(offset, seg.readNextOffset)

        val expectedNumEntries = numMessages / 2 - 1
        assertEquals(expectedNumEntries, seg.timeIndex.entries, s"Should have $expectedNumEntries time indexes")
    }

    /**
     * Append messages with timestamp and search message by timestamp.
     */
    @Test
    def testFindOffsetByTimestamp(): Unit = {
        val messageSize = records(0, s"msg00").sizeInBytes
        val seg = getOrCreateSegment(40, messageSize * 2 - 1)
        // Produce some messages
        for (i <- 40 until 50)
            seg.append(i, i * 10, i, records(i, s"msg$i"))

        assertEquals(490, seg.largestTimestamp)
        // Search for an indexed timestamp
        assertEquals(42, seg.findOffsetByTimestamp(420).get.offset)
        assertEquals(43, seg.findOffsetByTimestamp(421).get.offset)
        // Search for an un-indexed timestamp
        assertEquals(43, seg.findOffsetByTimestamp(430).get.offset)
        assertEquals(44, seg.findOffsetByTimestamp(431).get.offset)
        // Search beyond the last timestamp
        assertEquals(None, seg.findOffsetByTimestamp(491))
        // Search before the first indexed timestamp
        assertEquals(41, seg.findOffsetByTimestamp(401).get.offset)
        // Search before the first timestamp
        assertEquals(40, seg.findOffsetByTimestamp(399).get.offset)
    }

    /**
     * Test that offsets are assigned sequentially and that the nextOffset variable is incremented
     */
    @Test
    def testNextOffsetCalculation(): Unit = {
        val seg = getOrCreateSegment(40)
        assertEquals(40, seg.readNextOffset)
        seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, records(50, "hello", "there", "you"))
        assertEquals(53, seg.readNextOffset)
    }

    @Test
    def testRecoverTransactionIndex(): Unit = {
        val segment = getOrCreateSegment(100)
        val producerEpoch = 0.toShort
        val partitionLeaderEpoch = 15
        val sequence = 100

        val pid1 = 5L
        val pid2 = 10L

        // append transactional records from pid1
        segment.append(largestOffset = 101L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 100L, records = MemoryRecords.withTransactionalRecords(100L, CompressionType.NONE,
                pid1, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        // append transactional records from pid2
        segment.append(largestOffset = 103L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 102L, records = MemoryRecords.withTransactionalRecords(102L, CompressionType.NONE,
                pid2, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        // append non-transactional records
        segment.append(largestOffset = 105L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 104L, records = MemoryRecords.withRecords(104L, CompressionType.NONE,
                partitionLeaderEpoch, new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        // abort the transaction from pid2 (note LSO should be 100L since the txn from pid1 has not completed)
        segment.append(largestOffset = 106L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 106L, records = endTxnRecords(ControlRecordType.ABORT, pid2, producerEpoch, offset = 106L))

        // commit the transaction from pid1
        segment.append(largestOffset = 107L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 107L, records = endTxnRecords(ControlRecordType.COMMIT, pid1, producerEpoch, offset = 107L))

        var stateManager = newProducerStateManager()
        segment.recover(stateManager)
        assertEquals(108L, stateManager.mapEndOffset)

        var abortedTxns = segment.txnIndex.allAbortedTxns
        assertEquals(1, abortedTxns.size)
        var abortedTxn = abortedTxns.head
        assertEquals(pid2, abortedTxn.producerId)
        assertEquals(102L, abortedTxn.firstOffset)
        assertEquals(106L, abortedTxn.lastOffset)
        assertEquals(100L, abortedTxn.lastStableOffset)

        // recover again, but this time assuming the transaction from pid2 began on a previous segment
        stateManager = newProducerStateManager()
        stateManager.loadProducerEntry(new ProducerStateEntry(pid2,
            mutable.Queue[BatchMetadata](BatchMetadata(10, 10L, 5, RecordBatch.NO_TIMESTAMP)), producerEpoch,
            0, RecordBatch.NO_TIMESTAMP, Some(75L)))
        segment.recover(stateManager)
        assertEquals(108L, stateManager.mapEndOffset)

        abortedTxns = segment.txnIndex.allAbortedTxns
        assertEquals(1, abortedTxns.size)
        abortedTxn = abortedTxns.head
        assertEquals(pid2, abortedTxn.producerId)
        assertEquals(75L, abortedTxn.firstOffset)
        assertEquals(106L, abortedTxn.lastOffset)
        assertEquals(100L, abortedTxn.lastStableOffset)
    }

    /**
     * Create a segment with some data, then recover the segment.
     * The epoch cache entries should reflect the segment.
     */
    @Test
    def testRecoveryRebuildsEpochCache(): Unit = {
        val seg = getOrCreateSegment(0)

        val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
            private var epochs = Seq.empty[EpochEntry]

            override def write(epochs: Iterable[EpochEntry]): Unit = {
                this.epochs = epochs.toVector
            }

            override def read(): Seq[EpochEntry] = this.epochs
        }

        val cache = new LeaderEpochFileCache(topicPartition, checkpoint)
        seg.append(largestOffset = 105L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 104L, records = MemoryRecords.withRecords(104L, CompressionType.NONE, 0,
                new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        seg.append(largestOffset = 107L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 106L, records = MemoryRecords.withRecords(106L, CompressionType.NONE, 1,
                new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        seg.append(largestOffset = 109L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 108L, records = MemoryRecords.withRecords(108L, CompressionType.NONE, 1,
                new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        seg.append(largestOffset = 111L, largestTimestamp = RecordBatch.NO_TIMESTAMP,
            shallowOffsetOfMaxTimestamp = 110, records = MemoryRecords.withRecords(110L, CompressionType.NONE, 2,
                new SimpleRecord("a".getBytes), new SimpleRecord("b".getBytes)))

        seg.recover(newProducerStateManager(), Some(cache))
        assertEquals(ArrayBuffer(EpochEntry(epoch = 0, startOffset = 104L),
            EpochEntry(epoch = 1, startOffset = 106),
            EpochEntry(epoch = 2, startOffset = 110)),
            cache.epochEntries)
    }

    private def endTxnRecords(controlRecordType: ControlRecordType,
        producerId: Long,
        producerEpoch: Short,
        offset: Long,
        partitionLeaderEpoch: Int = 0,
        coordinatorEpoch: Int = 0,
        timestamp: Long = RecordBatch.NO_TIMESTAMP): MemoryRecords = {
        val marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch)
        MemoryRecords.withEndTransactionMarker(offset, timestamp, partitionLeaderEpoch, producerId, producerEpoch, marker)
    }

    /**
     * Create a segment with some data and an index. Then corrupt the index,
     * and recover the segment, the entries should all be readable.
     * Since time index is not a real file in ElasticLogSegment, corruption of time index is not tested here.
     */
    @Test
    def testRecoveryFixesCorruptTimeIndex(): Unit = {
        val seg = getOrCreateSegment(0)
        for (i <- 0 until 100)
            seg.append(i, i * 10, i, records(i, i.toString))
        //        val timeIndexFile = seg.lazyTimeIndex.file
        //        TestUtils.writeNonsenseToFile(timeIndexFile, 5, timeIndexFile.length.toInt)
        seg.recover(newProducerStateManager())
        for (i <- 0 until 100) {
            assertEquals(i, seg.findOffsetByTimestamp(i * 10).get.offset)
            if (i < 99)
                assertEquals(i + 1, seg.findOffsetByTimestamp(i * 10 + 1).get.offset)
        }
    }

    /**
     * This test was intended to check file size and positions after closing and reopening a segment.
     * Since there is no log file in ElasticLogSegment, we only test the segment size when reopening.
     * The method name is kept the same for compatibility.
     */
    @Test
    def testCreateWithInitFileSizeClearShutdown(): Unit = {
        val logConfig = LogConfig(Map(
            LogConfig.IndexIntervalBytesProp -> 10,
            LogConfig.SegmentIndexBytesProp -> 1000,
            LogConfig.SegmentJitterMsProp -> 0
        ).asJava)

        val seg = getOrCreateSegment(40, logConfig, Time.SYSTEM)

        val ms = records(40, "hello", "there")
        seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, ms)
        val ms2 = records(42, "alpha", "beta")
        seg.append(43, RecordBatch.NO_TIMESTAMP, -1L, ms2)
        val read = seg.read(startOffset = 42, maxSize = 200)
        checkEquals(ms2.records.iterator, read.records.records.iterator)
        val oldSize = seg.size
        seg.close()

        val segReopen = getOrCreateSegment(40, logConfig, Time.SYSTEM)

        val readAgain = segReopen.read(startOffset = 42, maxSize = 200)
        checkEquals(ms2.records.iterator, readAgain.records.records.iterator)
        val size = segReopen.size

        assertEquals(oldSize, size)
    }

    private def newProducerStateManager(): ProducerStateManager = {
        new ProducerStateManager(
            topicPartition,
            logDir,
            maxTransactionTimeoutMs = 5 * 60 * 1000,
            producerStateManagerConfig = new ProducerStateManagerConfig(kafka.server.Defaults.ProducerIdExpirationMs),
            time = new MockTime()
        )
    }

}
