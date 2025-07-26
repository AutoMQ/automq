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

package kafka.log.streamaspect;

import org.apache.kafka.common.compress.NoCompression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Time;

import com.automq.stream.api.Stream;
import com.automq.stream.s3.context.FetchContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@Tag("S3Unit")
@ExtendWith(MockitoExtension.class)
class ElasticLogFileRecordsTest {

    private ElasticStreamSlice streamSlice;
    private Stream stream;

    private ElasticLogFileRecords elasticLogFileRecords;

    private final Random random = new Random();

    @BeforeEach
    void setUp() {
        stream = spy(new MemoryClient.StreamImpl(1));
        streamSlice = spy(new DefaultElasticStreamSlice(stream, SliceRange.of(0, Offsets.NOOP_OFFSET)));
        elasticLogFileRecords = new ElasticLogFileRecords(streamSlice, 0, 0);
    }

    /**
     * Test reading a single, complete batch of records.
     */
    @Test
    void testReadSingleBatch() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = 0;
        int recordCount = 100;
        long maxOffset = startOffset + recordCount;
        final int MAX_READ_BYTES = 4096;

        Map<Long, SimpleRecord> expectedRecords = prepareRecords(startOffset, recordCount);
        MemoryRecords memoryRecords = createMemoryRecords(expectedRecords);
        elasticLogFileRecords.append(memoryRecords, maxOffset);

        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, MAX_READ_BYTES).get();

        // Assert
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(expectedRecords, readRecords);
        verify(streamSlice).fetch(any(FetchContext.class), eq(0L), eq(maxOffset), eq(MAX_READ_BYTES));
    }

    /**
     * Test reading data that spans across multiple record batches.
     */
    @Test
    void testReadAcrossMultipleBatches() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = 0;
        int recordsPerBatch = 50;
        int batchCount = 3;
        long totalRecords = (long) recordsPerBatch * batchCount;
        long maxOffset = startOffset + totalRecords;
        final int MAX_READ_BYTES = Integer.MAX_VALUE;

        Map<Long, SimpleRecord> allExpectedRecords = new HashMap<>();
        long currentStartOffset = startOffset;
        List<Long> nextOffsetList = new ArrayList<>();
        for (int i = 0; i < batchCount; i++) {
            Map<Long, SimpleRecord> batchRecords = prepareRecords(currentStartOffset, recordsPerBatch);
            allExpectedRecords.putAll(batchRecords);
            MemoryRecords memoryRecords = createMemoryRecords(batchRecords);
            elasticLogFileRecords.append(memoryRecords, currentStartOffset + recordsPerBatch);

            nextOffsetList.add(currentStartOffset);
            currentStartOffset += recordsPerBatch;
        }

        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, MAX_READ_BYTES).get();

        // Assert
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(allExpectedRecords, readRecords);
        nextOffsetList.forEach(nextOffset -> {
            verify(streamSlice).fetch(any(FetchContext.class), eq(nextOffset), eq(maxOffset), anyInt());
        });
    }

    /**
     * Test reading a batch that has been compacted, resulting in offset gaps.
     * The reader should correctly iterate over the existing records and skip the gaps.
     */
    @Test
    void testReadCompactedBatchWithGaps() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long batchStartOffset = 0;
        long lastOffsetInBatch = batchStartOffset + 5; // Batch spans from 0 to 5
        final int MAX_READ_BYTES = 4096;

        // Create a MemoryRecords buffer with offset gaps to simulate compaction.
        // We will only include records for offsets 0, 2, 4.
        Map<Long, SimpleRecord> expectedRecords = new HashMap<>();
        expectedRecords.put(batchStartOffset, createSimpleRecord("key" + batchStartOffset, "value" + batchStartOffset));
        // Skip 1
        expectedRecords.put(batchStartOffset + 2, createSimpleRecord("key" + (batchStartOffset + 2), "value" + (batchStartOffset + 2)));
        // Skip 3
        expectedRecords.put(batchStartOffset + 4, createSimpleRecord("key" + (batchStartOffset + 4), "value" + (batchStartOffset + 4)));
        // Skip 5
        MemoryRecords memoryRecords = createMemoryRecords(expectedRecords);
        elasticLogFileRecords.append(memoryRecords, lastOffsetInBatch + 1);

        // Act: Read the entire range that contains the gappy batch.
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(batchStartOffset, lastOffsetInBatch + 1, MAX_READ_BYTES).get();

        // Assert: Verify that only the existing records are returned.
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(expectedRecords, readRecords);
        verify(streamSlice).fetch(any(FetchContext.class), eq(batchStartOffset), eq(lastOffsetInBatch + 1), eq(MAX_READ_BYTES));
    }

    /**
     * Test reading across multiple batches where at least one batch is compacted and has offset gaps.
     */
    @Test
    void testReadAcrossMultipleBatchesWithGaps() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = 0;
        int recordsPerBatch = 50;
        int batchCount = 3;
        long totalRecords = (long) recordsPerBatch * batchCount;
        long maxOffset = startOffset + totalRecords;
        final int MAX_READ_BYTES = Integer.MAX_VALUE;

        Map<Long, SimpleRecord> allExpectedRecords = new HashMap<>();
        long currentStartOffset = startOffset;

        List<Long> nextOffsetList = new ArrayList<>();
        for (int i = 0; i < batchCount; i++) {
            int skippedRecords = random.nextInt(recordsPerBatch);
            Map<Long, SimpleRecord> batchRecords = prepareRecords(currentStartOffset, recordsPerBatch - skippedRecords);
            allExpectedRecords.putAll(batchRecords);
            MemoryRecords memoryRecords = createMemoryRecords(batchRecords);
            elasticLogFileRecords.append(memoryRecords, currentStartOffset + recordsPerBatch);

            nextOffsetList.add(currentStartOffset);
            currentStartOffset += recordsPerBatch;
        }

        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, MAX_READ_BYTES).get();

        // Assert
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(allExpectedRecords, readRecords);
        nextOffsetList.forEach(nextOffset -> {
            verify(streamSlice).fetch(any(FetchContext.class), eq(nextOffset), eq(maxOffset), anyInt());
        });
    }


    // Helper for preparing records to avoid code duplication
    private Map<Long, SimpleRecord> prepareRecords(long startOffset, int count) {
        final int FETCH_BATCH_SIZE = ElasticLogFileRecords.StreamSegmentInputStream.FETCH_BATCH_SIZE;
        Map<Long, SimpleRecord> records = new HashMap<>();

        // Calculate approximate size per record to ensure total size > FETCH_BATCH_SIZE
        int estimatedRecordOverhead = 50; // Approximate overhead per record (headers, etc.)
        int targetValueSize = Math.max(100, (FETCH_BATCH_SIZE / count) + estimatedRecordOverhead);

        // Create a large value string to ensure we exceed FETCH_BATCH_SIZE
        StringBuilder largeValue = new StringBuilder();
        for (int j = 0; j < targetValueSize; j++) {
            largeValue.append('a');
        }
        String valueTemplate = largeValue.toString();

        for (int i = 0; i < count; i++) {
            long currentOffset = startOffset + i;
            String value = valueTemplate + "_" + currentOffset; // Make each value unique
            records.put(currentOffset, createSimpleRecord("key" + currentOffset, value));
        }
        return records;
    }

    // Helper for asserting records to avoid code duplication
    private void assertRecords(Map<Long, SimpleRecord> expectedRecords, org.apache.kafka.common.record.Records actualRecords) {
        List<Record> records = new ArrayList<>();
        actualRecords.records().forEach(records::add);

        assertEquals(expectedRecords.size(), records.size());

        for (Record record : records) {
            SimpleRecord expectedRecord = expectedRecords.get(record.offset());
            assertNotNull(expectedRecord, "Unexpected record with offset " + record.offset());
            assertEquals(new String(expectedRecord.key().array()), new String(readBytes(record.key())));
            assertEquals(new String(expectedRecord.value().array()), new String(readBytes(record.value())));
        }
    }

    /**
     * Test reading an empty range (startOffset >= maxOffset).
     * Expects an empty Records object and no interaction with the underlying streamSlice.
     */
    @Test
    void testReadEmptyRange() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = 100;
        long maxOffset = 100; // startOffset >= maxOffset

        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, 4096).get();

        // Assert
        assertNotNull(readRecords);
        assertEquals(0, readRecords.sizeInBytes());
        // Verify that streamSlice.fetch was NOT called
        verify(streamSlice, never()).fetch(any(FetchContext.class), anyLong(), anyLong(), anyInt());
    }

    // Helper methods
    private SimpleRecord createSimpleRecord(String key, String value) {
        return new SimpleRecord(System.currentTimeMillis(), key.getBytes(), value.getBytes());
    }

    private MemoryRecords createMemoryRecords(Map<Long, SimpleRecord> records) {
        ByteBuffer buffer = ByteBuffer.allocate(128 * 1024); // Increased buffer size to accommodate larger records
        long baseOffset = records.keySet().stream().min(Long::compare).get();
        ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer);
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(stream,
            RecordBatch.CURRENT_MAGIC_VALUE,
            NoCompression.NONE,
            TimestampType.CREATE_TIME,
            baseOffset,
            Time.SYSTEM.milliseconds(),
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            false,
            false,
            RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.limit(), 0L)) {

            records.keySet().stream().sorted().forEach(
                offset -> builder.appendWithOffset(offset, records.get(offset))
            );
            return builder.build();
        }
    }

    private byte[] readBytes(ByteBuffer buffer) {
        if (buffer == null) return new byte[0];
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    static class ReadHint {
        private static final ThreadLocal<Boolean> READ_ALL = ThreadLocal.withInitial(() -> false);
        private static final ThreadLocal<Boolean> FAST_READ = ThreadLocal.withInitial(() -> false);

        public static boolean isReadAll() {
            return READ_ALL.get();
        }

        public static void setReadAll(boolean readAll) {
            READ_ALL.set(readAll);
        }

        public static boolean isFastRead() {
            return FAST_READ.get();
        }
    }
}
