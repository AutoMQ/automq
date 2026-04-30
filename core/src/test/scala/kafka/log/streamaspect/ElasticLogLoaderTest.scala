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

import kafka.log.LogTestUtils
import kafka.utils.TestUtils
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Tag, Test, Timeout}

import java.io.File

@Timeout(60)
@Tag("S3Unit")
class ElasticLogLoaderTest {

    val tmpDir: File      = TestUtils.tempDir()
    val logDir: File      = TestUtils.randomPartitionLogDir(tmpDir)
    val topicPartition    = UnifiedLog.parseTopicPartitionName(logDir)
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val mockTime          = new MockTime()
    val producerStateManagerConfig = new ProducerStateManagerConfig(
        TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, true)

    @AfterEach
    def tearDown(): Unit = Utils.delete(tmpDir)

    // Helper: open a fresh ElasticLog backed by the same MemoryClient
    private def openLog(client: MemoryClient, epoch: Long = 0): ElasticLog =
        ElasticLog.apply(
            client, "", logDir,
            LogTestUtils.createLogConfig(),
            mockTime.scheduler, mockTime,
            topicPartition, logDirFailureChannel,
            new java.util.concurrent.ConcurrentHashMap[String, Int](),
            5 * 60 * 1000, producerStateManagerConfig,
            topicId    = Some(Uuid.ZERO_UUID),
            leaderEpoch = epoch,
            openStreamChecker = OpenStreamChecker.NOOP)

    /**
     * Regression test for https://github.com/AutoMQ/automq/issues/2326
     *
     * If logStartOffset was advanced past the current logEndOffset (e.g. after
     * retention/compaction with a non-clean shutdown), ElasticLogLoader used to
     * throw an IllegalStateException, causing an infinite open-retry loop that
     * leaks S3 stream objects on every attempt.
     *
     * After the fix, load() must succeed and produce a log whose logStartOffset
     * equals the persisted value.
     */
    @Test
    def testLoadSucceedsWhenLogStartOffsetAheadOfLogEnd(): Unit = {
        // 1. Open the log and write a few records (offsets 0-2).
        val log1 = openLog(client)
        log1.append(2, mockTime.milliseconds(), 0,
            MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
                new SimpleRecord("a".getBytes),
                new SimpleRecord("b".getBytes),
                new SimpleRecord("c".getBytes)))

        // 2. Simulate a retention/compaction advancing logStartOffset past
        //    logEndOffset. We do this by directly setting the persisted
        //    partitionMeta before close() writes it.
        //    logEndOffset == 3, so set startOffset = 10 (ahead of logEnd).
        log1.partitionMeta.setStartOffset(10L)
        log1.partitionMeta.setCleanedShutdown(false) // simulate unclean shutdown
        // persist the corrupted meta into the meta stream so the next open sees it
        log1.persistPartitionMeta()
        // close without going through the normal close() path that would overwrite startOffset
        log1.closeStreams().get()

        // 3. Re-open the log — this must NOT throw IllegalStateException.
    val log2 = assertDoesNotThrow(new org.junit.jupiter.api.function.ThrowingSupplier[ElasticLog] { def get() = openLog(client, epoch = 1) })

        // 4. The recovered log start offset must match what was persisted (10).
        assertEquals(10L, log2.logStartOffset)

        // 5. The log must be writable (no lingering broken state).
    assertDoesNotThrow(new org.junit.jupiter.api.function.ThrowingSupplier[Unit] { def get() = { log2.append(10, mockTime.milliseconds(), 0, MemoryRecords.withRecords(10L, CompressionType.NONE, 0, new SimpleRecord("new".getBytes))); (): Unit } })

        log2.close()
    }

    /**
     * Sanity check: normal open (logStartOffset <= logEndOffset) still works.
     */
    @Test
    def testNormalLoadSucceeds(): Unit = {
        val client = new MemoryClient()
        val log1 = openLog(client)
        log1.append(2, mockTime.milliseconds(), 0,
            MemoryRecords.withRecords(0L, Compression.NONE, 0,
                new SimpleRecord("x".getBytes),
                new SimpleRecord("y".getBytes),
                new SimpleRecord("z".getBytes)))
        log1.close()

    val log2 = assertDoesNotThrow(new org.junit.jupiter.api.function.ThrowingSupplier[ElasticLog] { def get() = openLog(client, epoch = 1) })
        assertEquals(0L, log2.logStartOffset)
        assertEquals(3L, log2.logEndOffset)
        log2.close()
    }
}