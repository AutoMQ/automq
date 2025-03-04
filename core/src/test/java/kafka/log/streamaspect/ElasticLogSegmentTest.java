/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package kafka.log.streamaspect;

import kafka.utils.TestUtils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs;
import org.apache.kafka.server.util.MockScheduler;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.RollParams;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.test.TestUtils.checkEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// TODO: extends the LogSegmentTest
@Tag("S3Unit")
public class ElasticLogSegmentTest {
    private TopicPartition topicPartition = new TopicPartition("topic", 0);
    private File logDir;
    private Map<Long, ElasticStreamSegmentMeta> segmentMetas = new HashMap<>();
    private Map<Long, ElasticStreamSliceManager> segmentStreams = new HashMap<>();

    @BeforeEach
    public void setup() throws IOException {
        ReadHint.markReadAll();
        logDir = TestUtils.tempDir();
    }

    @AfterEach
    public void clear() throws IOException {
        Utils.delete(logDir);
    }

    @Test
    public void testReadOnEmptySegment() throws IOException {
        ElasticLogSegment seg = createOrLoadSegment(40);
        assertNull(seg.read(40, 300));
    }

    @Test
    public void testReadBeforeFirstOffset() throws IOException {
        ElasticLogSegment seg = createOrLoadSegment(40);
        MemoryRecords ms = records(40, "hello", "there", "little", "bee");
        seg.append(43, RecordBatch.NO_TIMESTAMP, -1L, ms);
        Records read = seg.read(31, 300).records;
        checkEquals(ms.records().iterator(), read.records().iterator());
    }

    @Test
    public void testReadAfterLast() throws IOException {
        ElasticLogSegment seg = createOrLoadSegment(40);
        MemoryRecords ms = records(40, "hello", "there");
        seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, ms);
        FetchDataInfo read = seg.read(52, 200);
        assertNull(read, "Read beyond the last offset in the segment should give null");
    }

    @Test
    public void testReadFromGap() throws IOException {
        ElasticLogSegment seg = createOrLoadSegment(40);
        MemoryRecords ms = records(40, "hello", "there");
        seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, ms);
        FetchDataInfo read = seg.read(35, 200);
        checkEquals(ms.records().iterator(), read.records.records().iterator());
    }

    /**
     * This test was intended to test truncation and rolling in LogSegment. However, truncation is not supported in
     * ElasticLogSegment. Therefore, we cut this test down to just verify the rolling and leave the test name as is.
     */
    @Test
    public void testTruncateEmptySegment() throws IOException {
        // This tests the scenario in which the follower truncates to an empty segment. In this
        // case we must ensure that the index is resized so that the log segment is not mistakenly
        // rolled due to a full index

        long maxSegmentMs = 300000;
        Time time = new MockTime();
        ElasticLogSegment seg = createOrLoadSegment(0, 10, time);
        seg.close();

        ElasticLogSegment reopened = createOrLoadSegment(0, 10, time);
        assertEquals(0, seg.timeIndex().sizeInBytes());

        assertFalse(reopened.timeIndex().isFull());

        RollParams rollParams = new RollParams(maxSegmentMs, Integer.MAX_VALUE, RecordBatch.NO_TIMESTAMP,
            100L, 1024, time.milliseconds());
        assertFalse(reopened.shouldRoll(rollParams));

        // The segment should not be rolled even if maxSegmentMs has been exceeded
        time.sleep(maxSegmentMs + 1);
        assertEquals(maxSegmentMs + 1, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP));
        rollParams = new RollParams(maxSegmentMs, Integer.MAX_VALUE, RecordBatch.NO_TIMESTAMP,
            100L, 1024, time.milliseconds());
        assertFalse(reopened.shouldRoll(rollParams));

        // But we should still roll the segment if we cannot fit the next offset
        rollParams = new RollParams(maxSegmentMs, Integer.MAX_VALUE, RecordBatch.NO_TIMESTAMP,
            Integer.MAX_VALUE + 200L, 1024, time.milliseconds());
        assertTrue(reopened.shouldRoll(rollParams));
    }

    @Test
    public void testIndexEntries() throws IOException {
        int numMessages = 30;
        ElasticLogSegment seg = createOrLoadSegment(40, 2 * records(0, "hello").sizeInBytes() - 1, Time.SYSTEM);
        var offset = 40;
        for (int i = 0; i < numMessages; i++) {
            seg.append(offset, offset, offset, records(offset, "hello"));
            offset += 1;
        }
        assertEquals(offset, seg.readNextOffset());

        int expectedNumEntries = numMessages / 2 - 1;
        assertEquals(expectedNumEntries, seg.timeIndex().entries(), "Should have " + expectedNumEntries + " time indexes");
    }

    @Test
    public void testFindOffsetByTimestamp() throws IOException {
        int messageSize = records(0, "msg00").sizeInBytes();
        ElasticLogSegment seg = createOrLoadSegment(40, messageSize * 2 - 1, Time.SYSTEM);
        // Produce some messages
        for (int i = 40; i < 50; i++) {
            seg.append(i, i * 10, i, records(i, "msg" + i));
        }

        assertEquals(490, seg.largestTimestamp());
        // Search for an indexed timestamp
        assertEquals(42, seg.findOffsetByTimestamp(420, 0).get().offset);
        assertEquals(43, seg.findOffsetByTimestamp(421, 0).get().offset);
        // Search for an un-indexed timestamp
        assertEquals(43, seg.findOffsetByTimestamp(430, 0).get().offset);
        assertEquals(44, seg.findOffsetByTimestamp(431, 0).get().offset);
        // Search beyond the last timestamp
        assertTrue(seg.findOffsetByTimestamp(491, 0).isEmpty());
        // Search before the first indexed timestamp
        assertEquals(41, seg.findOffsetByTimestamp(401, 0).get().offset);
        // Search before the first timestamp
        assertEquals(40, seg.findOffsetByTimestamp(399, 0).get().offset);
    }

    @Test
    public void testNextOffsetCalculation() throws IOException {
        ElasticLogSegment seg = createOrLoadSegment(40);
        assertEquals(40, seg.readNextOffset());
        seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, records(50, "hello", "there", "you"));
        assertEquals(53, seg.readNextOffset());
    }

    @Test
    public void testRecoverTransactionIndex() throws IOException {
        ElasticLogSegment segment = createOrLoadSegment(100);
        short producerEpoch = (short) 0;
        int partitionLeaderEpoch = 15;
        int sequence = 100;

        long pid1 = 5L;
        long pid2 = 10L;

        // append transactional records from pid1
        segment.append(101L, RecordBatch.NO_TIMESTAMP,
            100L, MemoryRecords.withTransactionalRecords(100L, Compression.NONE,
                pid1, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        // append transactional records from pid2
        segment.append(103L, RecordBatch.NO_TIMESTAMP, 102L, MemoryRecords.withTransactionalRecords(102L, Compression.NONE,
            pid2, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        // append non-transactional records
        segment.append(105L, RecordBatch.NO_TIMESTAMP, 104L, MemoryRecords.withRecords(104L, Compression.NONE,
            partitionLeaderEpoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        // abort the transaction from pid2 (note LSO should be 100L since the txn from pid1 has not completed)
        segment.append(106L, RecordBatch.NO_TIMESTAMP,
            106L, endTxnRecords(ControlRecordType.ABORT, pid2, producerEpoch, 106L));

        // commit the transaction from pid1
        segment.append(107L, RecordBatch.NO_TIMESTAMP,
            107L, endTxnRecords(ControlRecordType.COMMIT, pid1, producerEpoch, 107L));

        ProducerStateManager stateManager = newProducerStateManager();
        segment.recover(stateManager, Optional.empty());
        assertEquals(108L, stateManager.mapEndOffset());

        var abortedTxns = segment.txnIndex().allAbortedTxns();
        assertEquals(1, abortedTxns.size());
        var abortedTxn = abortedTxns.get(0);
        assertEquals(pid2, abortedTxn.producerId());
        assertEquals(102L, abortedTxn.firstOffset());
        assertEquals(106L, abortedTxn.lastOffset());
        assertEquals(100L, abortedTxn.lastStableOffset());

        // recover again, but this time assuming the transaction from pid2 began on a previous segment
        // TODO: simulate this case
        // the elastic log segment instance cannot be repeated recover, so we need to reset the index to support it.
//        segment.timeIndex.reset()
//        segment.txnIndex.reset()
//        stateManager = newProducerStateManager()
//        stateManager.loadProducerEntry(new ProducerStateEntry(pid2,
//            mutable.Queue[BatchMetadata](BatchMetadata(10, 10L, 5, RecordBatch.NO_TIMESTAMP)), producerEpoch,
//            0, RecordBatch.NO_TIMESTAMP, Some(75L)))
//        segment.recover(stateManager)
//        assertEquals(108L, stateManager.mapEndOffset)
//
//        abortedTxns = segment.txnIndex.allAbortedTxns
//        assertEquals(1, abortedTxns.size)
//        abortedTxn = abortedTxns.head
//        assertEquals(pid2, abortedTxn.producerId)
//        assertEquals(75L, abortedTxn.firstOffset)
//        assertEquals(106L, abortedTxn.lastOffset)
//        assertEquals(100L, abortedTxn.lastStableOffset)
    }

    @Test
    public void testRecoveryRebuildsEpochCache() throws IOException {
        ElasticLogSegment seg = createOrLoadSegment(0);

        ElasticLeaderEpochCheckpoint checkpoint = new ElasticLeaderEpochCheckpoint(new ElasticLeaderEpochCheckpointMeta(0, new ArrayList<>()), meta -> {
        });
        LeaderEpochFileCache cache = new LeaderEpochFileCache(topicPartition, checkpoint, new MockScheduler(Time.SYSTEM));
        seg.append(105L, RecordBatch.NO_TIMESTAMP,
            104L, MemoryRecords.withRecords(104L, Compression.NONE, 0,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        seg.append(107L, RecordBatch.NO_TIMESTAMP,
            106L, MemoryRecords.withRecords(106L, Compression.NONE, 1,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        seg.append(109L, RecordBatch.NO_TIMESTAMP,
            108L, MemoryRecords.withRecords(108L, Compression.NONE, 1,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        seg.append(111L, RecordBatch.NO_TIMESTAMP,
            110, MemoryRecords.withRecords(110L, Compression.NONE, 2,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

        seg.recover(newProducerStateManager(), Optional.of(cache));
        assertEquals(List.of(new EpochEntry(0, 104L),
                new EpochEntry(1, 106),
                new EpochEntry(2, 110)),
            cache.epochEntries());
    }

    //TODO: real unclean recover test for AutoMQ

    /**
     * This test was intended to check file size and positions after closing and reopening a segment.
     * Since there is no log file in ElasticLogSegment, we only test the segment size when reopening.
     * The method name is kept the same for compatibility.
     */
    @Test
    public void testCreateWithInitFileSizeClearShutdown() throws IOException {

        ElasticLogSegment seg = createOrLoadSegment(40, 10, Time.SYSTEM);

        MemoryRecords ms = records(40, "hello", "there");
        seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, ms);
        MemoryRecords ms2 = records(42, "alpha", "beta");
        seg.append(43, RecordBatch.NO_TIMESTAMP, -1L, ms2);
        FetchDataInfo read = seg.read(42, 200);
        checkEquals(ms2.records().iterator(), read.records.records().iterator());
        int oldSize = seg.size();
        seg.close();

        ElasticLogSegment segReopen = createOrLoadSegment(40, 10, Time.SYSTEM);

        FetchDataInfo readAgain = segReopen.read(42, 200);
        checkEquals(ms2.records().iterator(), readAgain.records.records().iterator());
        int size = segReopen.size();

        assertEquals(oldSize, size);
    }

    ElasticLogSegment createOrLoadSegment(long offset) throws IOException {
        return createOrLoadSegment(offset, 10, Time.SYSTEM);
    }

    ElasticLogSegment createOrLoadSegment(long offset, int indexIntervalBytes, Time time) throws IOException {
        Properties props = new Properties();
        props.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, indexIntervalBytes);

        ElasticStreamSegmentMeta meta = segmentMetas.computeIfAbsent(offset, k -> {
            ElasticStreamSegmentMeta m = new ElasticStreamSegmentMeta();
            m.baseOffset(offset);
            return m;
        });
        ElasticStreamSliceManager manager = segmentStreams.computeIfAbsent(offset, k -> {
            try {
                return new ElasticStreamSliceManager(new ElasticLogStreamManager(new HashMap<>(), new MemoryClient.StreamClientImpl(), 1, 0, new HashMap<>(), false));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return new ElasticLogSegment(logDir, meta, manager, new LogConfig(props), time, (a, b) -> {
        }, "");
    }

    MemoryRecords records(long offset, String... records) {
        return MemoryRecords.withRecords(
            RecordBatch.MAGIC_VALUE_V1,
            offset,
            Compression.NONE,
            TimestampType.CREATE_TIME,
            Arrays.stream(records).map(s -> new SimpleRecord(offset * 10, s.getBytes())).toArray(SimpleRecord[]::new));
    }

    MemoryRecords endTxnRecords(
        ControlRecordType controlRecordType,
        long producerId,
        short producerEpoch,
        long offset
    ) {
        return endTxnRecords(controlRecordType, producerId, producerEpoch, offset, 0, 0, RecordBatch.NO_TIMESTAMP);
    }

    MemoryRecords endTxnRecords(ControlRecordType controlRecordType,
        long producerId,
        short producerEpoch,
        long offset,
        int partitionLeaderEpoch,
        int coordinatorEpoch,
        long timestamp) {
        EndTransactionMarker marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch);
        return MemoryRecords.withEndTransactionMarker(offset, timestamp, partitionLeaderEpoch, producerId, producerEpoch, marker);
    }

    ProducerStateManager newProducerStateManager() throws IOException {
        return new ProducerStateManager(
            topicPartition,
            logDir,
            5 * 60 * 1000,
            new ProducerStateManagerConfig(TransactionLogConfigs.PRODUCER_ID_EXPIRATION_MS_DEFAULT, true),
            new MockTime()
        );
    }
}
