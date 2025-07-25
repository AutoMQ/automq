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

import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.api.Stream;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.context.FetchContext;
import org.apache.kafka.common.compress.NoCompression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ElasticLogFileRecordsTest {

    @Mock
    private ElasticStreamSlice mockStreamSlice;
    @Mock
    private Stream mockStream;

    private ElasticLogFileRecords elasticLogFileRecords;

    private static final long BASE_OFFSET = 1000L;
    private static final int INITIAL_SIZE = 0;
    private final Random random = new Random();

    @BeforeEach
    void setUp() {
        lenient().when(mockStreamSlice.stream()).thenReturn(mockStream);
        lenient().when(mockStream.streamId()).thenReturn(1L);
        elasticLogFileRecords = new ElasticLogFileRecords(mockStreamSlice, BASE_OFFSET, INITIAL_SIZE);
    }

    /**
     * Test reading a single, complete batch of records.
     */
    @Test
    void testReadSingleBatch() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = BASE_OFFSET;
        int recordCount = 100;
        long maxOffset = startOffset + recordCount;
        final int MAX_READ_BYTES = 4096;

        Map<Long, SimpleRecord> expectedRecords = prepareRecords(startOffset, recordCount);
        MemoryRecords memoryRecords = createMemoryRecords(expectedRecords);
        // The base offset for FetchResult should be the stream-relative offset.
        FetchResult fetchResult = createFetchResult(memoryRecords, startOffset - BASE_OFFSET, recordCount);

        long relativeEndOffset = maxOffset - BASE_OFFSET;
        lenient().when(mockStreamSlice.fetch(any(FetchContext.class), eq(0L), eq(relativeEndOffset), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(fetchResult));
        when(mockStreamSlice.confirmOffset()).thenReturn((long) recordCount);

        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, MAX_READ_BYTES).get();

        // Assert
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(expectedRecords, readRecords);
    }

    /**
     * Test reading data that spans across multiple record batches.
     */
    @Test
    void testReadAcrossMultipleBatches() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = BASE_OFFSET;
        int recordsPerBatch = 50;
        int batchCount = 3;
        long totalRecords = (long) recordsPerBatch * batchCount;
        long maxOffset = startOffset + totalRecords;
        final int MAX_READ_BYTES = 8192;

        Map<Long, SimpleRecord> allExpectedRecords = new HashMap<>();
        List<RecordBatchWithContext> s3Records = new ArrayList<>();
        long currentStartOffset = startOffset;

        for (int i = 0; i < batchCount; i++) {
            Map<Long, SimpleRecord> batchRecords = prepareRecords(currentStartOffset, recordsPerBatch);
            allExpectedRecords.putAll(batchRecords);
            MemoryRecords memoryRecords = createMemoryRecords(batchRecords);
            com.automq.stream.DefaultRecordBatch streamRecordBatch = new com.automq.stream.DefaultRecordBatch(recordsPerBatch, 0, Collections.emptyMap(), memoryRecords.buffer());
            // The base offset for RecordBatchWithContext should be the stream-relative offset.
            s3Records.add(new com.automq.stream.RecordBatchWithContextWrapper(streamRecordBatch, currentStartOffset));

            FetchResult fetchResult = createFetchResult(memoryRecords, currentStartOffset - BASE_OFFSET, recordsPerBatch);

            long relativeStartOffset = currentStartOffset - BASE_OFFSET;
            long relativeEndOffset = maxOffset - BASE_OFFSET;
            lenient().when(mockStreamSlice.fetch(any(FetchContext.class), eq(relativeStartOffset), eq(relativeEndOffset), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(fetchResult));

            currentStartOffset += recordsPerBatch;
        }

        when(mockStreamSlice.confirmOffset()).thenReturn(totalRecords);


        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, MAX_READ_BYTES).get();

        // Assert
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(allExpectedRecords, readRecords);
    }

    /**
     * Test reading a batch that has been compacted, resulting in offset gaps.
     * The reader should correctly iterate over the existing records and skip the gaps.
     */
    @Test
    void testReadCompactedBatchWithGaps() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long batchStartOffset = BASE_OFFSET;
        long lastOffsetInBatch = batchStartOffset + 5; // Batch spans from 1000 to 1005
        final int MAX_READ_BYTES = 4096;

        // 1. Create a MemoryRecords buffer with offset gaps to simulate compaction.
        // We will only include records for offsets 1000, 1002, 1004.
        Map<Long, SimpleRecord> expectedRecords = new HashMap<>();
        expectedRecords.put(batchStartOffset, createSimpleRecord("key" + batchStartOffset, "value" + batchStartOffset));
        // Skip 1001
        expectedRecords.put(batchStartOffset + 2, createSimpleRecord("key" + (batchStartOffset + 2), "value" + (batchStartOffset + 2)));
        // Skip 1003
        expectedRecords.put(batchStartOffset + 4, createSimpleRecord("key" + (batchStartOffset + 4), "value" + (batchStartOffset + 4)));
        // Skip 1005
        MemoryRecords memoryRecords = createMemoryRecords(expectedRecords);

        // 2. The S3 record batch indicates the original record count (6), but the buffer contains fewer (3).
        int originalRecordCount = 6;
        FetchResult fetchResult = createFetchResult(memoryRecords, batchStartOffset - BASE_OFFSET, originalRecordCount);

        long relativeEndOffset = (lastOffsetInBatch + 1) - BASE_OFFSET;
        lenient().when(mockStreamSlice.fetch(any(FetchContext.class), eq(0L), eq(relativeEndOffset), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(fetchResult));
        when(mockStreamSlice.confirmOffset()).thenReturn((long) originalRecordCount);

        // Act: Read the entire range that contains the gappy batch.
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(batchStartOffset, lastOffsetInBatch + 1, MAX_READ_BYTES).get();

        // Assert: Verify that only the existing records are returned.
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(expectedRecords, readRecords);
    }

    /**
     * Test reading across multiple batches where at least one batch is compacted and has offset gaps.
     */
    @Test
    void testReadAcrossMultipleBatchesWithGaps() throws ExecutionException, InterruptedException, IOException {
        // Arrange
        long startOffset = BASE_OFFSET;
        int recordsPerBatch = 50;
        int batchCount = 3;
        long totalRecords = (long) recordsPerBatch * batchCount;
        long maxOffset = startOffset + totalRecords;
        final int MAX_READ_BYTES = 8192;

        Map<Long, SimpleRecord> allExpectedRecords = new HashMap<>();
        List<RecordBatchWithContext> s3Records = new ArrayList<>();
        long currentStartOffset = startOffset;

        for (int i = 0; i < batchCount; i++) {
            int skippedRecords = random.nextInt(recordsPerBatch);
            Map<Long, SimpleRecord> batchRecords = prepareRecords(currentStartOffset, recordsPerBatch - skippedRecords);
            allExpectedRecords.putAll(batchRecords);
            MemoryRecords memoryRecords = createMemoryRecords(batchRecords);
            com.automq.stream.DefaultRecordBatch streamRecordBatch = new com.automq.stream.DefaultRecordBatch(recordsPerBatch, 0, Collections.emptyMap(), memoryRecords.buffer());
            // The base offset for RecordBatchWithContext should be the stream-relative offset.
            s3Records.add(new com.automq.stream.RecordBatchWithContextWrapper(streamRecordBatch, currentStartOffset));

            FetchResult fetchResult = createFetchResult(memoryRecords, currentStartOffset - BASE_OFFSET, recordsPerBatch);

            long relativeStartOffset = currentStartOffset - BASE_OFFSET;
            long relativeEndOffset = maxOffset - BASE_OFFSET;
            lenient().when(mockStreamSlice.fetch(any(FetchContext.class), eq(relativeStartOffset), eq(relativeEndOffset), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(fetchResult));

            currentStartOffset += recordsPerBatch;
        }

        when(mockStreamSlice.confirmOffset()).thenReturn(totalRecords);

        // Act
        ReadHint.setReadAll(false);
        org.apache.kafka.common.record.Records readRecords = elasticLogFileRecords.read(startOffset, maxOffset, MAX_READ_BYTES).get();

        // Assert
        assertNotNull(readRecords);
        assertTrue(readRecords instanceof ElasticLogFileRecords.BatchIteratorRecordsAdaptor);
        assertRecords(allExpectedRecords, readRecords);
    }


    // Helper for preparing records to avoid code duplication
    private Map<Long, SimpleRecord> prepareRecords(long startOffset, int count) {
        Map<Long, SimpleRecord> records = new HashMap<>();
        for (int i = 0; i < count; i++) {
            long currentOffset = startOffset + i;
            records.put(currentOffset, createSimpleRecord("key" + currentOffset, "value" + currentOffset));
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

    // Helper methods
    private SimpleRecord createSimpleRecord(String key, String value) {
        return new SimpleRecord(System.currentTimeMillis(), key.getBytes(), value.getBytes());
    }

    private MemoryRecords createMemoryRecords(Map<Long, SimpleRecord> records) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        long baseOffset = records.keySet().stream().min(Long::compare).get();
        ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(stream,
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
            RecordBatch.NO_PARTITION_LEADER_EPOCH, buffer.limit(), 0L);

        for (Long offset : records.keySet().stream().sorted().toList()) {
            builder.appendWithOffset(offset, records.get(offset));
        }
        return builder.build();
    }

    private FetchResult createFetchResult(MemoryRecords records, long baseOffset, int count) {
        com.automq.stream.DefaultRecordBatch batch = new com.automq.stream.DefaultRecordBatch(count, 0, Collections.emptyMap(), records.buffer());
        RecordBatchWithContext recordBatchWithContext = new com.automq.stream.RecordBatchWithContextWrapper(batch, baseOffset);
        FetchResult result = mock(FetchResult.class);
        lenient().when(result.recordBatchList()).thenReturn(Collections.singletonList(recordBatchWithContext));
        lenient().when(result.getCacheAccessType()).thenReturn(CacheAccessType.BLOCK_CACHE_MISS);
        return result;
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
