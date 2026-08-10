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

import com.automq.stream.DefaultRecordBatch
import com.automq.stream.api.{AppendResult, CreateStreamOptions, FetchResult, OpenStreamOptions, RecordBatch, Stream, StreamClient}
import com.automq.stream.s3.context.{AppendContext, FetchContext}
import kafka.log._
import kafka.log.streamaspect.reassignment._
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.{MemoryRecords, Record, SimpleRecord}
import org.apache.kafka.common.utils.{LogCaptureAppender, Time, Utils}
import org.apache.kafka.common.{KafkaException, Node, TopicPartition, Uuid}
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.server.util.{MockTime, Scheduler}
import org.apache.kafka.storage.internals.log._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, Test, Timeout}

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.{Collections, Optional}
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._

// TODO: extends the LocalLogTest
@Timeout(60)
@Tag("S3Unit")
class ElasticLogTest {
    val kafkaConfig: KafkaConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1))
    val tmpDir: File = TestUtils.tempDir()
    val logDir: File = TestUtils.randomPartitionLogDir(tmpDir)
    val topicPartition: TopicPartition = LocalLog.parseTopicPartitionName(logDir)
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val mockTime = new MockTime()
    val producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, true)
    var log: ElasticLog = _

    @BeforeEach
    def setup(): Unit = {
        log = createElasticLogWithActiveSegment(config = LogTestUtils.createLogConfig())
    }

    @AfterEach
    def tearDown(): Unit = {
        try {
            log.close()
        } catch {
            case _: KafkaStorageException => {
                // ignore
            }
        }
        Utils.delete(tmpDir)
    }

    case class KeyValue(key: String, value: String) {
        def toRecord(timestamp: => Long = mockTime.milliseconds): SimpleRecord = {
            new SimpleRecord(timestamp, key.getBytes, value.getBytes)
        }
    }

    object KeyValue {
        def fromRecord(record: Record): KeyValue = {
            val key =
                if (record.hasKey)
                    StandardCharsets.UTF_8.decode(record.key()).toString
                else
                    ""
            val value =
                if (record.hasValue)
                    StandardCharsets.UTF_8.decode(record.value()).toString
                else
                    ""
            KeyValue(key, value)
        }
    }

    private def kvsToRecords(keyValues: Iterable[KeyValue]): Iterable[SimpleRecord] = {
        keyValues.map(kv => kv.toRecord())
    }

    private def recordsToKvs(records: Iterable[Record]): Iterable[KeyValue] = {
        records.map(r => KeyValue.fromRecord(r))
    }

    private def appendRecords(records: Iterable[SimpleRecord],
        log: ElasticLog = log,
        initialOffset: Long = 0L): Unit = {
        log.append(lastOffset = initialOffset + records.size - 1,
            records = MemoryRecords.withRecords(initialOffset, Compression.NONE, 0, records.toList: _*))
    }

    private def readRecords(log: ElasticLog = log,
        startOffset: Long = 0L,
        maxLength: => Int = log.segments.activeSegment.size,
        minOneMessage: Boolean = false,
        maxOffsetMetadata: => LogOffsetMetadata = log.logEndOffsetMetadata,
        includeAbortedTxns: Boolean = false): FetchDataInfo = {
        log.read(startOffset,
            maxLength,
            minOneMessage = minOneMessage,
            maxOffsetMetadata,
            includeAbortedTxns = includeAbortedTxns)
    }

    @Test
    def testLogDeleteSegmentsSuccess(): Unit = {
        val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
        appendRecords(List(record))
        log.roll()
        assertEquals(2, log.segments.numberOfSegments)
        assertTrue(logDir.listFiles.isEmpty)
        val segmentsBeforeDelete = log.segments.values.asScala.toVector
        val deletedSegments = log.deleteAllSegments()
        assertTrue(log.segments.isEmpty)
        assertEquals(segmentsBeforeDelete, deletedSegments)
        assertThrows(classOf[KafkaStorageException], () => log.checkIfMemoryMappedBufferClosed())
        assertTrue(logDir.exists)
    }

    @Test
    def testRollEmptyActiveSegment(): Unit = {
        val oldActiveSegment = log.segments.activeSegment
        log.roll()
        assertEquals(1, log.segments.numberOfSegments)
        assertNotEquals(oldActiveSegment, log.segments.activeSegment)
        assertTrue(logDir.listFiles.isEmpty)
        // AutoMQ don't need rename the file to delete
        //        assertTrue(oldActiveSegment.hasSuffix(LocalLog.DeletedFileSuffix))
    }

    @Test
    def testLogDeleteDirSuccessWhenEmptyAndFailureWhenNonEmpty(): Unit = {
        val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
        appendRecords(List(record))
        log.roll()
        assertEquals(2, log.segments.numberOfSegments)
        assertTrue(logDir.listFiles.isEmpty)
        log.deleteAllSegments()
        log.deleteEmptyDir()
        assertFalse(logDir.exists)
    }

    @Test
    def testUpdateConfig(): Unit = {
        val oldConfig = log.config
        assertEquals(oldConfig, log.config)

        val newConfig = LogTestUtils.createLogConfig(segmentBytes = oldConfig.segmentSize + 1)
        log.updateConfig(newConfig)
        assertEquals(newConfig, log.config)
    }


    @Test
    def testLogDirRenameToExistingDir(): Unit = {
        assertFalse(log.renameDir(log.dir.getName))
    }

    @Test
    def testLogFlush(): Unit = {
        assertEquals(0L, log.recoveryPoint)
        assertEquals(mockTime.milliseconds, log.lastFlushTime)

        val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
        appendRecords(List(record))
        mockTime.sleep(1)
        val newSegment = log.roll()
        log.flush(newSegment.baseOffset)
        log.markFlushed(newSegment.baseOffset)
        assertEquals(1L, log.recoveryPoint)
        assertEquals(mockTime.milliseconds, log.lastFlushTime)
    }

    @Test
    def testLogAppend(): Unit = {
        val fetchDataInfoBeforeAppend = readRecords(maxLength = 1)
        assertTrue(fetchDataInfoBeforeAppend.records.records.asScala.isEmpty)

        mockTime.sleep(1)
        val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
        appendRecords(kvsToRecords(keyValues))
        assertEquals(2L, log.logEndOffset)
        assertEquals(0L, log.recoveryPoint)
        val fetchDataInfo = readRecords()
        assertEquals(2L, fetchDataInfo.records.records.asScala.size)
        assertEquals(keyValues, recordsToKvs(fetchDataInfo.records.records.asScala))
    }

    /** An admitted append publishes its callback completion before the underlying stream append completes. */
    @Test
    def testAppendPublishesAckFutureBeforeStreamCompletion(): Unit = {
        val client = new DelayedDataAppendClient()
        val dir = TestUtils.randomPartitionLogDir(tmpDir)
        val delayedLog = createElasticLogWithActiveSegment(
            dir = dir,
            config = LogTestUtils.createLogConfig(),
            topicPartition = LocalLog.parseTopicPartitionName(dir),
            client = client)
        val callbackCount = new AtomicInteger()
        delayedLog.confirmOffsetChangeListener = Some(() => callbackCount.incrementAndGet())

        try {
            appendRecords(Seq(new SimpleRecord(mockTime.milliseconds(), "value".getBytes)), delayedLog)

            assertFalse(delayedLog.lastAppendAckFuture.isDone)
            client.completeDataAppend()
            delayedLog.lastAppendAckFuture.get(10, TimeUnit.SECONDS)
            assertEquals(1, callbackCount.get())
        } finally {
            client.completeDataAppend()
            delayedLog.close()
        }
    }

    @Test
    def testLogCloseSuccess(): Unit = {
        val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
        appendRecords(kvsToRecords(keyValues))
        log.close()
        assertThrows(classOf[IOException], () => appendRecords(kvsToRecords(keyValues), initialOffset = 2L))
    }

    /**
     * Given a log closes, final log and cleaned-shutdown partition metadata are frozen by MetaStream close.
     */
    @Test
    def testLogClosePersistsCleanedShutdownMetadata(): Unit = {
        log.close()

        val handoff = log.metaStream.freeze().join()
        val metadata = handoff.records().asScala
            .map(record => MetaKeyValue.decode(record.encodedMetaKeyValue()))
            .map(keyValue => keyValue.getKey -> keyValue)
            .toMap
        assertTrue(metadata.contains(MetaStream.LOG_META_KEY))
        assertTrue(metadata.contains(MetaStream.PARTITION_META_KEY))
        assertTrue(ElasticPartitionMeta.decode(metadata(MetaStream.PARTITION_META_KEY).getValue).getCleanedShutdown)
    }

    /**
     * Given a V6 source and a healthy target, when prepare remains pending, then data streams are already closed while
     * MetaStream stays open, and MetaStream close resumes only after prepare completes.
     */
    @Test
    def testV6SourceClosesDataStreamsBeforeWaitingForPrepare(): Unit = {
        val client = new CloseTrackingClient
        val sendResult = new CompletableFuture[Void]()
        val prepared = new CountDownLatch(1)
        val captured = new AtomicReference[PartitionHandoff]()
        val sender = new TestSendOperation {
            override def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void] = {
                captured.set(handoff)
                assertTrue(client.createdStreams.asScala.exists(_.closeCount.get() == 0))
                assertTrue(client.createdStreams.asScala.exists(_.closeCount.get() == 1))
                prepared.countDown()
                sendResult
            }
        }
        val manager = fastReassignmentManager(sender)
        val source = createElasticLogWithActiveSegment(
            dir = TestUtils.randomPartitionLogDir(tmpDir),
            config = LogTestUtils.createLogConfig(),
            client = client,
            fastReassignmentManager = manager)
        val closeFuture = CompletableFuture.runAsync(() => source.close())

        assertTrue(prepared.await(10, TimeUnit.SECONDS))
        assertFalse(closeFuture.isDone)
        assertTrue(captured.get().metaStreamHandoff().records().asScala.exists(record =>
            MetaKeyValue.decode(record.encodedMetaKeyValue()).getKey == MetaStream.PARTITION_META_KEY))
        assertTrue(client.createdStreams.asScala.exists(_.closeCount.get() == 0))

        sendResult.complete(null)
        closeFuture.get(10, TimeUnit.SECONDS)
        assertTrue(client.createdStreams.asScala.forall(_.closeCount.get() == 1))
    }

    /**
     * Given final metadata persistence fails, the source closes normally without preparing a handoff.
     */
    @Test
    def testV6SourceFinalMetadataFailureDisablesHandoff(): Unit = {
        val failAppendOnce = new AtomicBoolean()
        val client = new CloseTrackingClient(failAppendOnce = failAppendOnce)
        val attempts = new AtomicInteger()
        val sender = new TestSendOperation {
            override def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void] = {
                attempts.incrementAndGet()
                CompletableFuture.completedFuture(null)
            }
        }
        val manager = fastReassignmentManager(sender)
        val source = createElasticLogWithActiveSegment(
            dir = TestUtils.randomPartitionLogDir(tmpDir),
            config = LogTestUtils.createLogConfig(),
            client = client,
            fastReassignmentManager = manager)
        failAppendOnce.set(true)
        source.close()
        assertEquals(0, attempts.get())
        assertTrue(client.createdStreams.asScala.forall(_.closeCount.get() == 1))
    }

    /**
     * Given a V6 prepare attempt fails, when source close observes the outcome, then the optional hint does not prevent
     * every source stream from closing.
     */
    @Test
    def testV6SourceClosesAfterPrepareFailure(): Unit = {
        val client = new CloseTrackingClient
        val attempts = new AtomicInteger()
        val sender = new TestSendOperation {
            override def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void] = {
                attempts.incrementAndGet()
                CompletableFuture.failedFuture(new IOException("prepare failed"))
            }
        }
        val manager = fastReassignmentManager(sender)
        val source = createElasticLogWithActiveSegment(
            dir = TestUtils.randomPartitionLogDir(tmpDir),
            config = LogTestUtils.createLogConfig(),
            client = client,
            fastReassignmentManager = manager)
        source.close()

        assertEquals(1, attempts.get())
        assertTrue(client.createdStreams.asScala.forall(_.closeCount.get() == 1))
    }

    /** A data-stream close failure remains contained without preventing the concurrent MetaStream handoff and close. */
    @Test
    def testV6SourceDataStreamCloseFailureStillSendsHandoff(): Unit = {
        val failCloseOnce = new AtomicBoolean(true)
        val client = new CloseTrackingClient(failCloseOnce)
        val attempts = new AtomicInteger()
        val sender = new TestSendOperation {
            override def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void] = {
                attempts.incrementAndGet()
                CompletableFuture.completedFuture(null)
            }
        }
        val manager = fastReassignmentManager(sender)
        val source = createElasticLogWithActiveSegment(
            dir = TestUtils.randomPartitionLogDir(tmpDir),
            config = LogTestUtils.createLogConfig(),
            client = client,
            fastReassignmentManager = manager)
        source.close()

        assertEquals(1, attempts.get())
        assertTrue(client.createdStreams.asScala.forall(_.closeCount.get() == 1))
    }

    /**
     * Given the finalized version does not enable fast reassignment, when the source closes, then no handoff behavior
     * is activated and the complete legacy stream close still runs.
     */
    @Test
    def testV5SourceCloseDoesNotPrepareHandoff(): Unit = {
        val client = new CloseTrackingClient
        val source = createElasticLogWithActiveSegment(
            dir = TestUtils.randomPartitionLogDir(tmpDir),
            config = LogTestUtils.createLogConfig(),
            client = client)
        source.close()

        assertTrue(client.createdStreams.asScala.forall(_.closeCount.get() == 1))
    }

    /**
     * Given source PUT completion and target GET are both blocked, a direct handoff still lets the authorized target
     * reach write readiness and append exactly at the source tail.
     */
    @Test
    def testGracefulHandoffConnectsSourceTailToTargetWriteReady(): Unit = {
        val sourceDir = TestUtils.randomPartitionLogDir(tmpDir)
        val partition = LocalLog.parseTopicPartitionName(sourceDir)
        val topicId = Uuid.randomUuid()
        val blockedPut = new CompletableFuture[Void]()
        val sourceClient = new CloseTrackingClient(blockedPut = Some(blockedPut))
        val cache = new PartitionHandoffCache()
        val prepared = new AtomicReference[PartitionHandoff]()
        val sender = new TestSendOperation {
            override def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void] = {
                prepared.set(handoff)
                cache.putAll(List(handoff).asJava)
                CompletableFuture.completedFuture(null)
            }
        }
        val manager = fastReassignmentManager(sender)
        val source = createElasticLogWithActiveSegment(
            dir = sourceDir,
            config = LogTestUtils.createLogConfig(),
            topicPartition = partition,
            topicId = topicId,
            client = sourceClient,
            fastReassignmentManager = manager)
        appendRecords(List(new SimpleRecord("source-0".getBytes), new SimpleRecord("source-1".getBytes)), source)
        val sourceTail = source.logEndOffset

        source.close()

        val handoff = prepared.get()
        assertNotNull(handoff)
        val targetParent = Files.createTempDirectory(tmpDir.toPath, "handoff-target").toFile
        val targetDir = new File(targetParent, sourceDir.getName)
        val targetMetaStream = new TargetMetaStream(source.metaStream.streamId(), failFetch = true)
        val targetClient = new TargetClient(targetMetaStream,
            controllerEndOffset = Some(handoff.metaStreamHandoff().endOffset()),
            dataStreamEndOffset = Some(sourceTail))
        registerMetaStream(targetClient, partition, topicId, targetMetaStream.streamId())

        val target = openExistingLog(targetClient, targetDir, partition, topicId, cache)
        try {
            assertEquals(0, targetMetaStream.fetchCount.get())
            assertEquals(sourceTail, target.logEndOffset)
            appendRecords(List(new SimpleRecord("target-first".getBytes)), target, sourceTail)
            assertEquals(sourceTail + 1, target.logEndOffset)
            assertTrue(sourceClient.uploadStarted.get())
            assertFalse(blockedPut.isDone)
        } finally {
            target.close()
        }
    }

    /**
     * Given a successful source-to-target handoff, stable prepare, close, and open markers expose correlation,
     * outcomes without coupling the marker contract to internal phase timings.
     */
    @Test
    def testFastReassignmentMarkersExposeStableExternalFields(): Unit = {
        val appender = LogCaptureAppender.createAndRegister()
        val sourceDir = TestUtils.randomPartitionLogDir(tmpDir)
        val partition = LocalLog.parseTopicPartitionName(sourceDir)
        val topicId = Uuid.randomUuid()
        val sourceClient = new CloseTrackingClient
        val cache = new PartitionHandoffCache()
        val prepared = new AtomicReference[PartitionHandoff]()
        val sender = new TestSendOperation {
            override def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void] = {
                prepared.set(handoff)
                cache.putAll(List(handoff).asJava)
                CompletableFuture.completedFuture(null)
            }
        }
        val manager = fastReassignmentManager(sender)
        val source = createElasticLogWithActiveSegment(
            dir = sourceDir,
            config = LogTestUtils.createLogConfig(),
            topicPartition = partition,
            topicId = topicId,
            client = sourceClient,
            fastReassignmentManager = manager)

        try {
            source.close()
            val handoff = prepared.get()
            val targetParent = Files.createTempDirectory(tmpDir.toPath, "marker-target").toFile
            val targetDir = new File(targetParent, sourceDir.getName)
            val targetMetaStream = new TargetMetaStream(handoff.metaStreamHandoff().endOffset(), failFetch = true)
            val targetClient = new TargetClient(targetMetaStream,
                controllerEndOffset = Some(handoff.metaStreamHandoff().endOffset()))
            registerMetaStream(targetClient, partition, topicId, targetMetaStream.streamId())
            val target = openExistingLog(targetClient, targetDir, partition, topicId, cache)
            target.close()

            val correlation = s"topicId=$topicId partitionId=${partition.partition()} " +
                s"handoffEndOffset=${handoff.endOffset()}"
            val messages = appender.getMessages.asScala
            assertTrue(messages.exists(message => message.contains("FAST_REASSIGNMENT_PREPARE") &&
                message.contains(correlation) && message.contains("result=success") && message.contains("reason=none") &&
                !message.contains("Ms=")))
            assertTrue(messages.exists(message => message.contains("FAST_REASSIGNMENT_CLOSE") &&
                message.contains(correlation) && message.contains("result=success") && message.contains("reason=none") &&
                !message.contains("Ms=")))
            assertTrue(messages.exists(message => message.contains("FAST_REASSIGNMENT_OPEN") &&
                message.contains(correlation) && message.contains("result=handoff") &&
                !message.contains("Ms=")))
        } finally {
            appender.close()
        }
    }

    /** A normal broker reopen with no matching handoff retains ObjectStorage replay without a fast marker. */
    @Test
    def testOrdinaryExistingLogReopenDoesNotEmitFastReassignmentOpen(): Unit = {
        val appender = LogCaptureAppender.createAndRegister()
        val targetDir = TestUtils.randomPartitionLogDir(tmpDir)
        val targetPartition = LocalLog.parseTopicPartitionName(targetDir)
        val topicId = Uuid.randomUuid()
        val metaStream = targetMetaStreamWithObjectStorageMetadata(47L, startOffset = 19L)
        val client = new TargetClient(metaStream)
        registerMetaStream(client, targetPartition, topicId, metaStream.streamId())
        val reopened = openExistingLogWithoutReassignment(client, targetDir, targetPartition, topicId,
            new PartitionHandoffCache())
        try {
            assertEquals(19L, reopened.partitionMeta.getStartOffset)
            assertTrue(metaStream.fetchCount.get() > 0)
            assertFalse(appender.getMessages.asScala.exists(_.contains("FAST_REASSIGNMENT_OPEN")))
        } finally {
            reopened.close()
            appender.close()
        }
    }

    /** A cache miss retains ObjectStorage recovery without emitting a fast OPEN marker. */
    @Test
    def testCacheMissDoesNotEmitFastReassignmentOpen(): Unit = {
        val appender = LogCaptureAppender.createAndRegister()
        val targetDir = TestUtils.randomPartitionLogDir(tmpDir)
        val targetPartition = LocalLog.parseTopicPartitionName(targetDir)
        val topicId = Uuid.randomUuid()
        val metaStream = targetMetaStreamWithObjectStorageMetadata(48L, startOffset = 20L)
        val client = new TargetClient(metaStream)
        registerMetaStream(client, targetPartition, topicId, metaStream.streamId())

        val reopened = openExistingLog(client, targetDir, targetPartition, topicId,
            new PartitionHandoffCache())
        try {
            assertFalse(appender.getMessages.asScala.exists(_.contains("FAST_REASSIGNMENT_OPEN")))
        } finally {
            reopened.close()
            appender.close()
        }
    }

    /**
     * Given only the Controller-open end offset matches a staged handoff, when the target opens, then it consumes the
     * exact entry after open and restores metadata without ObjectStorage replay.
     */
    @Test
    def testTargetOpenConsumesExactAuthorizedHandoffWithoutObjectStorageReplay(): Unit = {
        val targetDir = TestUtils.randomPartitionLogDir(tmpDir)
        val targetPartition = LocalLog.parseTopicPartitionName(targetDir)
        val topicId = Uuid.randomUuid()
        val metaStream = new TargetMetaStream(41L, failFetch = true)
        val client = new TargetClient(metaStream, controllerEndOffset = Some(7L))
        registerMetaStream(client, targetPartition, topicId, metaStream.streamId())
        val cache = new PartitionHandoffCache()
        val exact = partitionHandoff(topicId, targetPartition.partition(), 7L, startOffset = 23L)
        val wrongEnd = partitionHandoff(topicId, targetPartition.partition(), 6L, startOffset = 99L)
        cache.putAll(List(exact, wrongEnd).asJava)
        val checker = new ControllableOpenStreamChecker()

        val openFuture = CompletableFuture.supplyAsync(() =>
            openExistingLog(client, targetDir, targetPartition, topicId, cache, checker))
        assertTrue(checker.awaitCheck())
        assertEquals(0, client.metaOpenCount.get())
        assertTrue(cache.take(exact.key()).isPresent)
        cache.putAll(List(exact).asJava)
        checker.authorize()

        val recovered = openFuture.get(10, TimeUnit.SECONDS)
        try {
            assertEquals(23L, recovered.partitionMeta.getStartOffset)
            assertEquals(1, client.metaOpenCount.get())
            assertEquals(0, metaStream.fetchCount.get())
            assertTrue(cache.take(exact.key()).isEmpty)
            assertTrue(cache.take(wrongEnd.key()).isPresent)
            val initialEndOffset = recovered.logEndOffset
            appendRecords(List(new SimpleRecord("target-write".getBytes)), recovered, initialEndOffset)
            assertEquals(initialEndOffset + 1, recovered.logEndOffset)
        } finally {
            recovered.close()
        }
    }

    /**
     * Given a consumed handoff entry, when the same identity is retried on a fresh target open, then the retry uses
     * ObjectStorage replay and restores equivalent partition metadata.
     */
    @Test
    def testConsumedHandoffRetryFallsBackToObjectStorageReplay(): Unit = {
        val topicId = Uuid.randomUuid()
        val cache = new PartitionHandoffCache()
        val firstDir = TestUtils.randomPartitionLogDir(tmpDir)
        val firstPartition = LocalLog.parseTopicPartitionName(firstDir)
        val firstStream = targetMetaStreamWithObjectStorageMetadata(51L, startOffset = 31L)
        val firstClient = new TargetClient(firstStream)
        registerMetaStream(firstClient, firstPartition, topicId, firstStream.streamId())
        val handoff = partitionHandoff(topicId, firstPartition.partition(), firstStream.nextOffset(), 31L)
        cache.putAll(List(handoff).asJava)

        val firstRecovered = openExistingLog(firstClient, firstDir, firstPartition, topicId, cache)
        val handoffMetadata = partitionMetadata(firstRecovered)
        firstRecovered.close()
        assertEquals(0, firstStream.fetchCount.get())

        val retryDir = firstDir
        val retryPartition = firstPartition
        val retryStream = targetMetaStreamWithObjectStorageMetadata(52L, startOffset = 31L)
        val retryClient = new TargetClient(retryStream)
        registerMetaStream(retryClient, retryPartition, topicId, retryStream.streamId())

        val retryRecovered = openExistingLog(retryClient, retryDir, retryPartition, topicId, cache)
        try {
            assertEquals(handoffMetadata, partitionMetadata(retryRecovered))
            assertTrue(retryStream.fetchCount.get() > 0)
        } finally {
            retryRecovered.close()
        }
    }

    /**
     * Given a matching handoff fails during apply, when the target opens, then it discards temporary state and restores
     * the authoritative ObjectStorage metadata before appending new-owner metadata.
     */
    @Test
    def testHandoffReplayFailureFallsBackBeforeNewOwnerMetadataAppend(): Unit = {
        val appender = LogCaptureAppender.createAndRegister()
        val targetDir = TestUtils.randomPartitionLogDir(tmpDir)
        val targetPartition = LocalLog.parseTopicPartitionName(targetDir)
        val topicId = Uuid.randomUuid()
        val metaStream = targetMetaStreamWithObjectStorageMetadata(61L, startOffset = 47L)
        val client = new TargetClient(metaStream)
        registerMetaStream(client, targetPartition, topicId, metaStream.streamId())
        val invalidPartition = MetaKeyValue.of(
            MetaStream.PARTITION_META_KEY, ByteBuffer.wrap(Array[Byte](1, 2, 3)))
        val invalidHandoff = new PartitionHandoff(topicId, targetPartition.partition(),
            new MetaStreamHandoff(metaStream.nextOffset(), List(
                handoffRecord(0L, MetaKeyValue.of("UNKNOWN_FUTURE_KEY", ByteBuffer.wrap(Array[Byte](9)))),
                handoffRecord(1L, invalidPartition)).asJava))
        val cache = new PartitionHandoffCache()
        cache.putAll(List(invalidHandoff).asJava)

        val recovered = openExistingLog(client, targetDir, targetPartition, topicId, cache)
        try {
            assertEquals(47L, recovered.partitionMeta.getStartOffset)
            assertTrue(metaStream.fetchCount.get() > 0)
            assertEquals(metaStream.seededMetadataAppendCount.get(), metaStream.appendCountAtFirstFetch.get())
            assertTrue(cache.take(invalidHandoff.key()).isEmpty)
            assertFalse(appender.getMessages.asScala.exists(_.contains("FAST_REASSIGNMENT_OPEN")))
        } finally {
            recovered.close()
            appender.close()
        }
    }

    /**
     * Given Controller-authorized MetaStream open fails before cache lookup, when target open is retried, then the
     * staged hint remains available and the retry reaches write readiness without ObjectStorage replay.
     */
    @Test
    def testMetaStreamOpenFailureRetainsHandoffForRetry(): Unit = {
        val targetDir = TestUtils.randomPartitionLogDir(tmpDir)
        val targetPartition = LocalLog.parseTopicPartitionName(targetDir)
        val topicId = Uuid.randomUuid()
        val metaStream = new TargetMetaStream(62L, failFetch = true)
        val failMetaOpenOnce = new AtomicBoolean(true)
        val client = new TargetClient(metaStream, failMetaOpenOnce = failMetaOpenOnce)
        registerMetaStream(client, targetPartition, topicId, metaStream.streamId())
        val handoff = partitionHandoff(topicId, targetPartition.partition(), 0L, startOffset = 53L)
        val cache = new PartitionHandoffCache()
        cache.putAll(List(handoff).asJava)

        assertThrows(classOf[Throwable], () =>
            openExistingLog(client, targetDir, targetPartition, topicId, cache))
        assertTrue(cache.take(handoff.key()).isPresent)
        cache.putAll(List(handoff).asJava)

        val recovered = openExistingLog(client, targetDir, targetPartition, topicId, cache)
        try {
            assertEquals(53L, recovered.partitionMeta.getStartOffset)
            assertEquals(0, metaStream.fetchCount.get())
        } finally {
            recovered.close()
        }
    }

    /**
     * Given a handoff was consumed but opening a data stream fails, when target open is retried, then the
     * consumed entry is not reused and authoritative ObjectStorage replay restores the partition.
     */
    @Test
    def testDataStreamOpenFailureRetriesThroughObjectStorageFallback(): Unit = {
        val targetDir = TestUtils.randomPartitionLogDir(tmpDir)
        val targetPartition = LocalLog.parseTopicPartitionName(targetDir)
        val topicId = Uuid.randomUuid()
        val metaStream = targetMetaStreamWithObjectStorageMetadata(63L, startOffset = 59L)
        val failDataStreamCreateOnce = new AtomicBoolean(true)
        val firstClient = new TargetClient(metaStream, failDataStreamCreateOnce = failDataStreamCreateOnce)
        registerMetaStream(firstClient, targetPartition, topicId, metaStream.streamId())
        val handoff = partitionHandoff(
            topicId, targetPartition.partition(), metaStream.nextOffset(), startOffset = 59L)
        val cache = new PartitionHandoffCache()
        cache.putAll(List(handoff).asJava)

        assertThrows(classOf[Throwable], () =>
            openExistingLog(firstClient, targetDir, targetPartition, topicId, cache))
        assertTrue(cache.take(handoff.key()).isEmpty)

        val retryClient = new TargetClient(metaStream)
        registerMetaStream(retryClient, targetPartition, topicId, metaStream.streamId())
        val recovered = openExistingLog(retryClient, targetDir, targetPartition, topicId, cache)
        try {
            assertEquals(59L, recovered.partitionMeta.getStartOffset)
            assertTrue(metaStream.fetchCount.get() > 0)
        } finally {
            recovered.close()
        }
    }

    @Test
    def testLogCloseIdempotent(): Unit = {
        log.close()
        // Check that LocalLog.close() is idempotent
        log.close()
    }

    @Test
    def testLogCloseHandlers(): Unit = {
        val keyValues = Seq(KeyValue("abc", "ABC"), KeyValue("de", "DE"))
        appendRecords(kvsToRecords(keyValues))
        log.closeHandlers()
        assertThrows(classOf[IOException],
            () => appendRecords(kvsToRecords(keyValues), initialOffset = 2L))
    }

    @Test
    def testLogCloseHandlersIdempotent(): Unit = {
        log.closeHandlers()
        // Check that LocalLog.closeHandlers() is idempotent
        log.closeHandlers()
    }

    private def testRemoveAndDeleteSegments(asyncDelete: Boolean): Unit = {
        for (offset <- 0 to 8) {
            val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
            appendRecords(List(record), initialOffset = offset)
            log.roll()
        }

        assertEquals(10L, log.segments.numberOfSegments)

        class TestDeletionReason extends SegmentDeletionReason {
            private var _deletedSegments: Iterable[LogSegment] = List[LogSegment]()

            override def logReason(toDelete: List[LogSegment]): Unit = {
                _deletedSegments = List[LogSegment]() ++ toDelete
            }

            def deletedSegments: Iterable[LogSegment] = _deletedSegments
        }
        val reason = new TestDeletionReason()
        val toDelete = log.segments.values.asScala.toVector
        log.removeAndDeleteSegments(toDelete, asyncDelete = asyncDelete, reason)
        if (asyncDelete) {
            mockTime.sleep(log.config.fileDeleteDelayMs + 1)
        }
        assertTrue(log.segments.isEmpty)
        assertEquals(toDelete, reason.deletedSegments)
        //        toDelete.foreach(segment => assertTrue(segment.deleted()))
    }

    @Test
    def testRemoveAndDeleteSegmentsSync(): Unit = {
        testRemoveAndDeleteSegments(asyncDelete = false)
    }

    @Test
    def testRemoveAndDeleteSegmentsAsync(): Unit = {
        testRemoveAndDeleteSegments(asyncDelete = true)
    }

    private def testDeleteSegmentFiles(asyncDelete: Boolean): Unit = {
        for (offset <- 0 to 8) {
            val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
            appendRecords(List(record), initialOffset = offset)
            log.roll()
        }

        assertEquals(10L, log.segments.numberOfSegments)

        val toDelete = log.segments.values.asScala.toVector
        LocalLog.deleteSegmentFiles(toDelete, asyncDelete = asyncDelete, log.dir, log.topicPartition, log.config, log.scheduler, log.logDirFailureChannel, "")
        if (asyncDelete) {
            toDelete.foreach {
                segment =>
                    assertFalse(segment.deleted())
                //                    assertTrue(segment.hasSuffix(LocalLog.DeletedFileSuffix))
            }
            mockTime.sleep(log.config.fileDeleteDelayMs + 1)
        }
        //        toDelete.foreach(segment => assertTrue(segment.deleted()))
    }

    @Test
    def testDeleteSegmentFilesSync(): Unit = {
        testDeleteSegmentFiles(asyncDelete = false)
    }

    @Test
    def testDeleteSegmentFilesAsync(): Unit = {
        testDeleteSegmentFiles(asyncDelete = true)
    }

    @Test
    def testCreateAndDeleteSegment(): Unit = {
        val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
        appendRecords(List(record))
        val newOffset = log.segments.activeSegment.baseOffset + 1
        val oldActiveSegment = log.segments.activeSegment
        val newActiveSegment = log.createAndDeleteSegment(newOffset, log.segments.activeSegment, asyncDelete = true, LogTruncation(log))
        assertEquals(1, log.segments.numberOfSegments)
        assertEquals(newActiveSegment, log.segments.activeSegment)
        assertNotEquals(oldActiveSegment, log.segments.activeSegment)
        //        assertTrue(oldActiveSegment.hasSuffix(LocalLog.DeletedFileSuffix))
        assertEquals(newOffset, log.segments.activeSegment.baseOffset)
        assertEquals(0L, log.recoveryPoint)
        assertEquals(newOffset, log.logEndOffset)
        val fetchDataInfo = readRecords(startOffset = newOffset)
        assertTrue(fetchDataInfo.records.records.asScala.isEmpty)
    }

    @Test
    def testTruncateFullyAndStartAt(): Unit = {
        val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
        for (offset <- 0 to 7) {
            appendRecords(List(record), initialOffset = offset)
            if (offset % 2 != 0)
                log.roll()
        }
        for (offset <- 8 to 12) {
            val record = new SimpleRecord(mockTime.milliseconds, "a".getBytes)
            appendRecords(List(record), initialOffset = offset)
        }
        assertEquals(5, log.segments.numberOfSegments)
        assertNotEquals(10L, log.segments.activeSegment.baseOffset)
        val expected = log.segments.values.asScala.toVector
        val deleted = log.truncateFullyAndStartAt(10L)
        assertEquals(expected, deleted)
        assertEquals(1, log.segments.numberOfSegments)
        assertEquals(10L, log.segments.activeSegment.baseOffset)
        assertEquals(0L, log.recoveryPoint)
        assertEquals(10L, log.logEndOffset)
        val fetchDataInfo = readRecords(startOffset = 10L)
        assertTrue(fetchDataInfo.records.records.asScala.isEmpty)
    }

    @Test
    def testNonActiveSegmentsFrom(): Unit = {
        for (i <- 0 until 5) {
            val keyValues = Seq(KeyValue(i.toString, i.toString))
            appendRecords(kvsToRecords(keyValues), initialOffset = i)
            log.roll()
        }

        def nonActiveBaseOffsetsFrom(startOffset: Long): Seq[Long] = {
            log.segments.nonActiveLogSegmentsFrom(startOffset).asScala.map(_.baseOffset).toSeq
        }

        assertEquals(5L, log.segments.activeSegment.baseOffset)
        assertEquals(0 until 5, nonActiveBaseOffsetsFrom(0L))
        assertEquals(Seq.empty, nonActiveBaseOffsetsFrom(5L))
        assertEquals(2 until 5, nonActiveBaseOffsetsFrom(2L))
        assertEquals(Seq.empty, nonActiveBaseOffsetsFrom(6L))
    }

    private def topicPartitionName(topic: String, partition: String): String = topic + "-" + partition

    @Test
    def testParseTopicPartitionName(): Unit = {
        val topic = "test_topic"
        val partition = "143"
        val dir = new File(logDir, topicPartitionName(topic, partition))
        val topicPartition = LocalLog.parseTopicPartitionName(dir)
        assertEquals(topic, topicPartition.topic)
        assertEquals(partition.toInt, topicPartition.partition)
    }

    /**
     * Tests that log directories with a period in their name that have been marked for deletion
     * are parsed correctly by `Log.parseTopicPartitionName` (see KAFKA-5232 for details).
     */
    @Test
    def testParseTopicPartitionNameWithPeriodForDeletedTopic(): Unit = {
        val topic = "foo.bar-testtopic"
        val partition = "42"
        val dir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, partition.toInt)))
        val topicPartition = LocalLog.parseTopicPartitionName(dir)
        assertEquals(topic, topicPartition.topic, "Unexpected topic name parsed")
        assertEquals(partition.toInt, topicPartition.partition, "Unexpected partition number parsed")
    }

    @Test
    def testParseTopicPartitionNameForEmptyName(): Unit = {
        val dir = new File("")
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
            () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
    }

    @Test
    def testParseTopicPartitionNameForNull(): Unit = {
        val dir: File = null
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
            () => "KafkaException should have been thrown for dir: " + dir)
    }

    @Test
    def testParseTopicPartitionNameForMissingSeparator(): Unit = {
        val topic = "test_topic"
        val partition = "1999"
        val dir = new File(logDir, topic + partition)
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
            () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)
        // also test the "-delete" marker case
        val deleteMarkerDir = new File(logDir, topic + partition + "." + LocalLog.DeleteDirSuffix)
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
            () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    }

    @Test
    def testParseTopicPartitionNameForMissingTopic(): Unit = {
        val topic = ""
        val partition = "1999"
        val dir = new File(logDir, topicPartitionName(topic, partition))
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
            () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)

        // also test the "-delete" marker case
        val deleteMarkerDir = new File(logDir, LocalLog.logDeleteDirName(new TopicPartition(topic, partition.toInt)))

        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
            () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    }

    @Test
    def testParseTopicPartitionNameForMissingPartition(): Unit = {
        val topic = "test_topic"
        val partition = ""
        val dir = new File(logDir.getPath + topicPartitionName(topic, partition))
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
            () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)

        // also test the "-delete" marker case
        val deleteMarkerDir = new File(logDir, topicPartitionName(topic, partition) + "." + LocalLog.DeleteDirSuffix)
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
            () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    }

    @Test
    def testParseTopicPartitionNameForInvalidPartition(): Unit = {
        val topic = "test_topic"
        val partition = "1999a"
        val dir = new File(logDir, topicPartitionName(topic, partition))
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir),
            () => "KafkaException should have been thrown for dir: " + dir.getCanonicalPath)

        // also test the "-delete" marker case
        val deleteMarkerDir = new File(logDir, topic + partition + "." + LocalLog.DeleteDirSuffix)
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(deleteMarkerDir),
            () => "KafkaException should have been thrown for dir: " + deleteMarkerDir.getCanonicalPath)
    }

    @Test
    def testParseTopicPartitionNameForExistingInvalidDir(): Unit = {
        val dir1 = new File(logDir.getPath + "/non_kafka_dir")
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir1),
            () => "KafkaException should have been thrown for dir: " + dir1.getCanonicalPath)
        val dir2 = new File(logDir.getPath + "/non_kafka_dir-delete")
        assertThrows(classOf[KafkaException], () => LocalLog.parseTopicPartitionName(dir2),
            () => "KafkaException should have been thrown for dir: " + dir2.getCanonicalPath)
    }

    @Test
    def testLogDeleteDirName(): Unit = {
        val name1 = LocalLog.logDeleteDirName(new TopicPartition("foo", 3))
        assertTrue(name1.length <= 255)
        assertTrue(Pattern.compile("foo-3\\.[0-9a-z]{32}-delete").matcher(name1).matches())
        assertTrue(LocalLog.DeleteDirPattern.matcher(name1).matches())
        assertFalse(LocalLog.FutureDirPattern.matcher(name1).matches())
        val name2 = LocalLog.logDeleteDirName(
            new TopicPartition("n" + String.join("", Collections.nCopies(248, "o")), 5))
        assertEquals(255, name2.length)
        assertTrue(Pattern.compile("n[o]{212}-5\\.[0-9a-z]{32}-delete").matcher(name2).matches())
        assertTrue(LocalLog.DeleteDirPattern.matcher(name2).matches())
        assertFalse(LocalLog.FutureDirPattern.matcher(name2).matches())
    }

    @Test
    def testOffsetFromFile(): Unit = {
        val offset = 23423423L

        val logFile = LogFileUtils.logFile(tmpDir, offset)
        assertEquals(offset, LogFileUtils.offsetFromFile(logFile))

        val offsetIndexFile = LogFileUtils.offsetIndexFile(tmpDir, offset)
        assertEquals(offset, LogFileUtils.offsetFromFile(offsetIndexFile))

        val timeIndexFile = LogFileUtils.timeIndexFile(tmpDir, offset)
        assertEquals(offset, LogFileUtils.offsetFromFile(timeIndexFile))
    }

    @Test
    def testRollSegmentThatAlreadyExists(): Unit = {
        assertEquals(1, log.segments.numberOfSegments, "Log begins with a single empty segment.")

        // roll active segment with the same base offset of size zero should recreate the segment
        log.roll(Some(0L))
        assertEquals(1, log.segments.numberOfSegments, "Expect 1 segment after roll() empty segment with base offset.")

        // should be able to append records to active segment
        val keyValues1 = List(KeyValue("k1", "v1"))
        appendRecords(kvsToRecords(keyValues1))
        assertEquals(0L, log.segments.activeSegment.baseOffset)
        // make sure we can append more records
        val keyValues2 = List(KeyValue("k2", "v2"))
        appendRecords(keyValues2.map(_.toRecord(mockTime.milliseconds + 10)), initialOffset = 1L)
        assertEquals(2, log.logEndOffset, "Expect two records in the log")
        val readResult = readRecords()
        assertEquals(2L, readResult.records.records.asScala.size)
        assertEquals(keyValues1 ++ keyValues2, recordsToKvs(readResult.records.records.asScala))

        // roll so that active segment is empty
        log.roll()
        assertEquals(2L, log.segments.activeSegment.baseOffset, "Expect base offset of active segment to be LEO")
        assertEquals(2, log.segments.numberOfSegments, "Expect two segments.")
        assertEquals(2L, log.logEndOffset)
    }

    @Test
    def testNewSegmentsAfterRoll(): Unit = {
        assertEquals(1, log.segments.numberOfSegments, "Log begins with a single empty segment.")

        // roll active segment with the same base offset of size zero should recreate the segment
        {
            val newSegment = log.roll()
            assertEquals(0L, newSegment.baseOffset)
            assertEquals(1, log.segments.numberOfSegments)
            assertEquals(0L, log.logEndOffset)
        }

        appendRecords(List(KeyValue("k1", "v1").toRecord()))

        {
            val newSegment = log.roll()
            assertEquals(1L, newSegment.baseOffset)
            assertEquals(2, log.segments.numberOfSegments)
            assertEquals(1L, log.logEndOffset)
        }

        appendRecords(List(KeyValue("k2", "v2").toRecord()), initialOffset = 1L)

        {
            val newSegment = log.roll(Some(1L))
            assertEquals(2L, newSegment.baseOffset)
            assertEquals(3, log.segments.numberOfSegments)
            assertEquals(2L, log.logEndOffset)
        }
    }

    @Test
    def testRollSegmentErrorWhenNextOffsetIsIllegal(): Unit = {
        assertEquals(1, log.segments.numberOfSegments, "Log begins with a single empty segment.")

        val keyValues = List(KeyValue("k1", "v1"), KeyValue("k2", "v2"), KeyValue("k3", "v3"))
        appendRecords(kvsToRecords(keyValues))
        assertEquals(0L, log.segments.activeSegment.baseOffset)
        assertEquals(3, log.logEndOffset, "Expect two records in the log")

        // roll to create an empty active segment
        log.roll()
        assertEquals(3L, log.segments.activeSegment.baseOffset)

        // intentionally setup the logEndOffset to introduce an error later
        log.updateLogEndOffset(1L)

        // expect an error because of attempt to roll to a new offset (1L) that's lower than the
        // base offset (3L) of the active segment
        assertThrows(classOf[KafkaException], () => log.roll())
    }

    @Test
    def testAbortTxn_withRoll(): Unit = {
        var keyValues = Seq(KeyValue("a=", "1"))
        appendRecords(kvsToRecords(keyValues), initialOffset = 0)
        keyValues = Seq(KeyValue("a=", "2"))
        appendRecords(kvsToRecords(keyValues), initialOffset = 1)
        log.roll()
        keyValues = Seq(KeyValue("a=", "3"))
        appendRecords(kvsToRecords(keyValues), initialOffset = 2)
        log.segments.activeSegment.updateTxnIndex(new CompletedTxn(1, 2, 2, true), 1)
        keyValues = Seq(KeyValue("a=", "4"))
        appendRecords(kvsToRecords(keyValues), initialOffset = 3)
        keyValues = Seq(KeyValue("a=", "5"))
        appendRecords(kvsToRecords(keyValues), initialOffset = 4)
        log.segments.activeSegment.updateTxnIndex(new CompletedTxn(1, 4, 4, true), 4)
        ReadHint.markReadAll()
        val fetchDataInfo = log.read(2, 1024, true, log.logEndOffsetMetadata, true)
        assertEquals(3L, fetchDataInfo.records.records.asScala.size)
        val abortedTxns = fetchDataInfo.abortedTransactions.get.toArray
        assertEquals(2, abortedTxns.size)
        assertEquals(2L, abortedTxns(0).asInstanceOf[FetchResponseData.AbortedTransaction].firstOffset())
        assertEquals(4L, abortedTxns(1).asInstanceOf[FetchResponseData.AbortedTransaction].firstOffset())
    }

    @Test
    def testReadCommittedEmptyFetchReturnsEmptyAbortedTransactions(): Unit = {
        appendRecords(kvsToRecords(Seq(KeyValue("a=", "1"))), initialOffset = 0)

        val fetchDataInfo = log.read(0, 0, minOneMessage = false, log.logEndOffsetMetadata, includeAbortedTxns = true)

        assertEquals(0, fetchDataInfo.records.sizeInBytes)
        assertTrue(fetchDataInfo.abortedTransactions.isPresent)
        assertTrue(fetchDataInfo.abortedTransactions.get.isEmpty)
    }

    private def createElasticLogWithActiveSegment(dir: File = logDir,
        config: LogConfig,
        scheduler: Scheduler = mockTime.scheduler,
        time: Time = mockTime,
        topicPartition: TopicPartition = topicPartition,
        logDirFailureChannel: LogDirFailureChannel = logDirFailureChannel,
        clusterId: String = "test_cluster",
        client: MemoryClient = new MemoryClient(),
        topicId: Uuid = Uuid.ZERO_UUID,
        fastReassignmentManager: FastPartitionReassignmentManager =
            FastPartitionReassignmentManager.disabled()): ElasticLog = {
        //        Context.enableTestMode()
        ElasticLog.apply(
            client,
            "",
            dir,
            config,
            scheduler,
            time = time,
            topicPartition = topicPartition,
            logDirFailureChannel = logDirFailureChannel,
            maxTransactionTimeoutMs = 5 * 60 * 1000,
            producerStateManagerConfig = producerStateManagerConfig,
            topicId = Some(topicId),
            leaderEpoch = 0,
            openStreamChecker = OpenStreamChecker.NOOP,
            fastReassignmentManager = fastReassignmentManager,
        )
    }

    private def openExistingLog(client: MemoryClient,
        dir: File,
        partition: TopicPartition,
        topicId: Uuid,
        cache: PartitionHandoffCache,
        checker: OpenStreamChecker = OpenStreamChecker.NOOP): ElasticLog = {
        openExistingLogWithoutReassignment(client, dir, partition, topicId, cache, checker)
    }

    private def openExistingLogWithoutReassignment(client: MemoryClient,
        dir: File,
        partition: TopicPartition,
        topicId: Uuid,
        cache: PartitionHandoffCache,
        checker: OpenStreamChecker = OpenStreamChecker.NOOP): ElasticLog = {
        ElasticLog.apply(
            client,
            "",
            dir,
            LogTestUtils.createLogConfig(),
            mockTime.scheduler,
            time = mockTime,
            topicPartition = partition,
            logDirFailureChannel = logDirFailureChannel,
            maxTransactionTimeoutMs = 5 * 60 * 1000,
            producerStateManagerConfig = producerStateManagerConfig,
            topicId = Some(topicId),
            leaderEpoch = 1,
            openStreamChecker = checker,
            fastReassignmentManager = fastReassignmentManager(cache = cache))
    }

    private trait TestSendOperation {
        def send(target: Node, handoff: PartitionHandoff): CompletableFuture[Void]
    }

    private def fastReassignmentManager(
        sender: TestSendOperation = null,
        cache: PartitionHandoffCache = new PartitionHandoffCache()
    ): FastPartitionReassignmentManager =
        new FastPartitionReassignmentManager {
            override def send(handoff: PartitionHandoff): CompletableFuture[Void] =
                if (sender == null)
                    CompletableFuture.failedFuture(new PartitionHandoffSendException(
                        PartitionHandoffSendException.Reason.NOT_ATTEMPTED))
                else
                    sender.send(new Node(2, "target", 9092), handoff)

            override def receive(handoffs: java.util.Collection[PartitionHandoff]): Unit =
                cache.putAll(handoffs)

            override def take(key: PartitionHandoff.Key): Optional[PartitionHandoff] =
                cache.take(key)

            override def close(): Unit =
                cache.clear()
        }

    private def registerMetaStream(client: MemoryClient,
        partition: TopicPartition,
        topicId: Uuid,
        streamId: Long): Unit = {
        val value = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(streamId)
        value.flip()
        client.kvClient().putKV(com.automq.stream.api.KeyValue.of(
            ElasticLog.formatStreamKey("", partition, Some(topicId)), value)).join()
    }

    private def partitionHandoff(topicId: Uuid,
        partitionId: Int,
        endOffset: Long,
        startOffset: Long): PartitionHandoff = {
        val partitionMeta = new ElasticPartitionMeta(startOffset, startOffset + 1, startOffset + 2)
        partitionMeta.setCleanedShutdown(true)
        val records = List(
            handoffRecord(0L, MetaKeyValue.of(MetaStream.LOG_META_KEY, ElasticLogMeta.encode(new ElasticLogMeta()))),
            handoffRecord(1L, MetaKeyValue.of(MetaStream.PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta))),
            handoffRecord(2L, MetaKeyValue.of(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY,
                ByteBuffer.wrap(new ElasticLeaderEpochCheckpointMeta(0, Collections.emptyList()).encode()))))
        new PartitionHandoff(topicId, partitionId, new MetaStreamHandoff(endOffset, records.asJava))
    }

    private def targetMetaStreamWithObjectStorageMetadata(streamId: Long, startOffset: Long): TargetMetaStream = {
        val stream = new TargetMetaStream(streamId, failFetch = false)
        val handoff = partitionHandoff(Uuid.ZERO_UUID, 0, 3L, startOffset).metaStreamHandoff()
        handoff.records().forEach(record => stream.append(new DefaultRecordBatch(
            1, 0L, Collections.emptyMap(), record.encodedMetaKeyValue())).join())
        stream.seededMetadataAppendCount.set(stream.appendCount.get())
        stream
    }

    private def handoffRecord(offset: Long, keyValue: MetaKeyValue): MetaStreamHandoffRecord =
        new MetaStreamHandoffRecord(offset, MetaKeyValue.encode(keyValue))

    private def partitionMetadata(log: ElasticLog): (Long, Long, Long, Boolean) =
        (log.partitionMeta.getStartOffset, log.partitionMeta.getCleanerOffset,
            log.partitionMeta.getRecoverOffset, log.partitionMeta.getCleanedShutdown)

    private class TargetMetaStream(streamId: Long, failFetch: Boolean) extends MemoryClient.StreamImpl(streamId) {
        val fetchCount = new AtomicInteger()
        val appendCount = new AtomicInteger()
        val seededMetadataAppendCount = new AtomicInteger()
        val appendCountAtFirstFetch = new AtomicInteger(-1)
        private val trimmedStartOffset = new AtomicLong()

        override def startOffset(): Long = trimmedStartOffset.get()

        override def trim(newStartOffset: Long): CompletableFuture[Void] = {
            trimmedStartOffset.set(newStartOffset)
            super.trim(newStartOffset)
        }

        override def append(context: com.automq.stream.s3.context.AppendContext,
            recordBatch: com.automq.stream.api.RecordBatch): CompletableFuture[com.automq.stream.api.AppendResult] = {
            appendCount.incrementAndGet()
            super.append(context, recordBatch)
        }

        override def fetch(context: FetchContext,
            startOffset: Long,
            endOffset: Long,
            maxSizeHint: Int): CompletableFuture[FetchResult] = {
            fetchCount.incrementAndGet()
            appendCountAtFirstFetch.compareAndSet(-1, appendCount.get())
            if (failFetch) {
                CompletableFuture.failedFuture(new IOException("ObjectStorage fetch blocked"))
            } else {
                super.fetch(context, startOffset, endOffset, maxSizeHint)
            }
        }
    }

    private class DelayedDataAppendClient extends MemoryClient {
        private val streamId = new AtomicLong()
        private val dataAppendCompletion = new CompletableFuture[Void]()
        private val delayedStreamClient = new StreamClient {
            override def createAndOpenStream(options: CreateStreamOptions): CompletableFuture[Stream] = {
                val id = streamId.incrementAndGet()
                val stream = if (id == 1) {
                    new MemoryClient.StreamImpl(id)
                } else {
                    new MemoryClient.StreamImpl(id) {
                        override def append(context: AppendContext, recordBatch: RecordBatch): CompletableFuture[AppendResult] = {
                            val result = super.append(context, recordBatch).join()
                            dataAppendCompletion.thenApply(_ => result)
                        }
                    }
                }
                CompletableFuture.completedFuture(stream)
            }

            override def openStream(id: Long, options: OpenStreamOptions): CompletableFuture[Stream] =
                CompletableFuture.completedFuture(new MemoryClient.StreamImpl(id))

            override def getStream(id: Long): Optional[Stream] = Optional.empty()

            override def shutdown(): Unit = {}
        }

        override def streamClient(): StreamClient = delayedStreamClient

        def completeDataAppend(): Unit = dataAppendCompletion.complete(null)
    }

    private class CloseTrackingStream(streamId: Long, failCloseOnce: AtomicBoolean, failAppendOnce: AtomicBoolean,
        blockedPut: Option[CompletableFuture[Void]], uploadStarted: AtomicBoolean)
        extends MemoryClient.StreamImpl(streamId) {
        val closeCount = new AtomicInteger()

        override def append(context: com.automq.stream.s3.context.AppendContext,
            recordBatch: com.automq.stream.api.RecordBatch): CompletableFuture[com.automq.stream.api.AppendResult] = {
            if (failAppendOnce.compareAndSet(true, false)) {
                CompletableFuture.failedFuture(new IOException("metadata append failed"))
            } else {
                super.append(context, recordBatch)
            }
        }

        override def close(): CompletableFuture[Void] = {
            closeCount.incrementAndGet()
            blockedPut.foreach(_ => uploadStarted.set(true))
            if (failCloseOnce.compareAndSet(true, false)) {
                CompletableFuture.failedFuture(new IOException("stream close failed"))
            } else {
                CompletableFuture.completedFuture(null)
            }
        }
    }

    private class CloseTrackingClient(failCloseOnce: AtomicBoolean = new AtomicBoolean(false),
        failAppendOnce: AtomicBoolean = new AtomicBoolean(false),
        blockedPut: Option[CompletableFuture[Void]] = None) extends MemoryClient {
        val createdStreams = new java.util.concurrent.CopyOnWriteArrayList[CloseTrackingStream]()
        val uploadStarted = new AtomicBoolean()
        private val streamId = new AtomicLong()
        private val trackingStreamClient = new StreamClient {
            override def createAndOpenStream(options: CreateStreamOptions): CompletableFuture[Stream] = {
                val stream = new CloseTrackingStream(
                    streamId.incrementAndGet(), failCloseOnce, failAppendOnce, blockedPut, uploadStarted)
                createdStreams.add(stream)
                CompletableFuture.completedFuture(stream)
            }

            override def openStream(id: Long, options: OpenStreamOptions): CompletableFuture[Stream] = {
                val stream = new CloseTrackingStream(id, failCloseOnce, failAppendOnce, blockedPut, uploadStarted)
                createdStreams.add(stream)
                CompletableFuture.completedFuture(stream)
            }

            override def getStream(id: Long): Optional[Stream] = Optional.empty()

            override def shutdown(): Unit = {}
        }

        override def streamClient(): StreamClient = trackingStreamClient
    }

    private class TargetClient(metaStream: TargetMetaStream,
        controllerEndOffset: Option[Long] = None,
        failMetaOpenOnce: AtomicBoolean = new AtomicBoolean(false),
        failDataStreamCreateOnce: AtomicBoolean = new AtomicBoolean(false),
        dataStreamEndOffset: Option[Long] = None) extends MemoryClient {
        val metaOpenCount = new AtomicInteger()
        private val streamId = new AtomicLong(1000L)
        private val targetStreamClient = new StreamClient {
            override def createAndOpenStream(options: CreateStreamOptions): CompletableFuture[Stream] = {
                if (failDataStreamCreateOnce.compareAndSet(true, false)) {
                    CompletableFuture.failedFuture(new IOException("data stream open failed"))
                } else {
                    val stream = new MemoryClient.StreamImpl(streamId.incrementAndGet())
                    dataStreamEndOffset.foreach(stream.confirmOffset)
                    CompletableFuture.completedFuture(stream)
                }
            }

            override def openStream(id: Long, options: OpenStreamOptions): CompletableFuture[Stream] = {
                if (id == metaStream.streamId()) {
                    metaOpenCount.incrementAndGet()
                    if (failMetaOpenOnce.compareAndSet(true, false)) {
                        CompletableFuture.failedFuture(new IOException("MetaStream open failed"))
                    } else {
                        controllerEndOffset.foreach(metaStream.confirmOffset)
                        CompletableFuture.completedFuture(metaStream)
                    }
                } else {
                    val stream = new MemoryClient.StreamImpl(id)
                    dataStreamEndOffset.foreach(stream.confirmOffset)
                    CompletableFuture.completedFuture(stream)
                }
            }

            override def getStream(id: Long): Optional[Stream] = Optional.empty()

            override def shutdown(): Unit = {}
        }

        override def streamClient(): StreamClient = targetStreamClient
    }

    private class ControllableOpenStreamChecker extends OpenStreamChecker {
        private val checkEntered = new CountDownLatch(1)
        private val authorized = new CompletableFuture[Void]()

        override def check(topicId: Uuid, partition: Int, streamId: Long, epoch: Long): CompletableFuture[Void] = {
            checkEntered.countDown()
            authorized
        }

        def awaitCheck(): Boolean = checkEntered.await(10, TimeUnit.SECONDS)

        def authorize(): Unit = authorized.complete(null)
    }

}
