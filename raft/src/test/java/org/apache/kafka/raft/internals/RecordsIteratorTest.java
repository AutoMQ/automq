/*
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
package org.apache.kafka.raft.internals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.MockRawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RecordsIteratorTest {
    private static final RecordSerde<String> STRING_SERDE = new StringSerde();

    private static Stream<Arguments> emptyRecords() throws IOException {
        return Stream.of(
            FileRecords.open(TestUtils.tempFile()),
            MemoryRecords.EMPTY
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("emptyRecords")
    void testEmptyRecords(Records records) {
        testIterator(Collections.emptyList(), records, true);
    }

    @Property(tries = 50)
    public void testMemoryRecords(
        @ForAll CompressionType compressionType,
        @ForAll long seed
    ) {
        List<TestBatch<String>> batches = createBatches(seed);

        MemoryRecords memRecords = buildRecords(compressionType, batches);
        testIterator(batches, memRecords, true);
    }

    @Property(tries = 50)
    public void testFileRecords(
        @ForAll CompressionType compressionType,
        @ForAll long seed
    ) throws IOException {
        List<TestBatch<String>> batches = createBatches(seed);

        MemoryRecords memRecords = buildRecords(compressionType, batches);
        FileRecords fileRecords = FileRecords.open(TestUtils.tempFile());
        fileRecords.append(memRecords);

        testIterator(batches, fileRecords, true);
        fileRecords.close();
    }

    @Property(tries = 50)
    public void testCrcValidation(
        @ForAll CompressionType compressionType,
        @ForAll long seed
    ) throws IOException {
        List<TestBatch<String>> batches = createBatches(seed);
        MemoryRecords memRecords = buildRecords(compressionType, batches);
        // Read the Batch CRC for the first batch from the buffer
        ByteBuffer readBuf = memRecords.buffer();
        readBuf.position(DefaultRecordBatch.CRC_OFFSET);
        int actualCrc = readBuf.getInt();
        // Corrupt the CRC on the first batch
        memRecords.buffer().putInt(DefaultRecordBatch.CRC_OFFSET, actualCrc + 1);

        assertThrows(CorruptRecordException.class, () -> testIterator(batches, memRecords, true));

        FileRecords fileRecords = FileRecords.open(TestUtils.tempFile());
        fileRecords.append(memRecords);
        assertThrows(CorruptRecordException.class, () -> testIterator(batches, fileRecords, true));

        // Verify check does not trigger when doCrcValidation is false
        assertDoesNotThrow(() -> testIterator(batches, memRecords, false));
        assertDoesNotThrow(() -> testIterator(batches, fileRecords, false));

        // Fix the corruption
        memRecords.buffer().putInt(DefaultRecordBatch.CRC_OFFSET, actualCrc);

        // Verify check does not trigger when the corruption is fixed
        assertDoesNotThrow(() -> testIterator(batches, memRecords, true));
        FileRecords moreFileRecords = FileRecords.open(TestUtils.tempFile());
        moreFileRecords.append(memRecords);
        assertDoesNotThrow(() -> testIterator(batches, moreFileRecords, true));

        fileRecords.close();
        moreFileRecords.close();
    }

    @Test
    public void testControlRecordIteration() {
        AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);
        try (RecordsSnapshotWriter<String> snapshot = RecordsSnapshotWriter.createWithHeader(
                new MockRawSnapshotWriter(new OffsetAndEpoch(100, 10), snapshotBuf -> buffer.set(snapshotBuf)),
                4 * 1024,
                MemoryPool.NONE,
                new MockTime(),
                0,
                CompressionType.NONE,
                STRING_SERDE
            )
        ) {
            snapshot.append(Arrays.asList("a", "b", "c"));
            snapshot.append(Arrays.asList("d", "e", "f"));
            snapshot.append(Arrays.asList("g", "h", "i"));
            snapshot.freeze();
        }

        try (RecordsIterator<String> iterator = createIterator(
                MemoryRecords.readableRecords(buffer.get()),
                BufferSupplier.NO_CACHING,
                true
            )
        ) {
            // Check snapshot header control record
            Batch<String> batch = iterator.next();

            assertEquals(1, batch.controlRecords().size());
            assertEquals(ControlRecordType.SNAPSHOT_HEADER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotHeaderRecord(), batch.controlRecords().get(0).message());

            // Consume the iterator until we find a control record
            do {
                batch = iterator.next();
            }
            while (batch.controlRecords().isEmpty());

            // Check snapshot footer control record
            assertEquals(1, batch.controlRecords().size());
            assertEquals(ControlRecordType.SNAPSHOT_FOOTER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotFooterRecord(), batch.controlRecords().get(0).message());

            // Snapshot footer must be last record
            assertFalse(iterator.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(value = ControlRecordType.class, names = {"LEADER_CHANGE", "SNAPSHOT_HEADER", "SNAPSHOT_FOOTER"})
    void testWithAllSupportedControlRecords(ControlRecordType type) {
        MemoryRecords records = buildControlRecords(type);
        final ApiMessage expectedMessage;
        switch (type) {
            case LEADER_CHANGE:
                expectedMessage = new LeaderChangeMessage();
                break;
            case SNAPSHOT_HEADER:
                expectedMessage = new SnapshotHeaderRecord();
                break;
            case SNAPSHOT_FOOTER:
                expectedMessage = new SnapshotFooterRecord();
                break;
            default:
                throw new RuntimeException("Should not happen. Poorly configured test");
        }

        try (RecordsIterator<String> iterator = createIterator(records, BufferSupplier.NO_CACHING, true)) {
            assertTrue(iterator.hasNext());
            assertEquals(
                Collections.singletonList(new ControlRecord(type, expectedMessage)),
                iterator.next().controlRecords()
            );
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void testControlRecordTypeValues() {
        // If this test fails then it means that ControlRecordType was changed. Please review the
        // implementation for RecordsIterator to see if it needs to be updated based on the changes
        // to ControlRecordType.
        assertEquals(6, ControlRecordType.values().length);
    }

    private void testIterator(
        List<TestBatch<String>> expectedBatches,
        Records records,
        boolean validateCrc
    ) {
        Set<ByteBuffer> allocatedBuffers = Collections.newSetFromMap(new IdentityHashMap<>());

        try (RecordsIterator<String> iterator = createIterator(
                records,
                mockBufferSupplier(allocatedBuffers),
                validateCrc
            )
        ) {
            for (TestBatch<String> batch : expectedBatches) {
                assertTrue(iterator.hasNext());
                assertEquals(batch, TestBatch.from(iterator.next()));
            }

            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }

        assertEquals(Collections.emptySet(), allocatedBuffers);
    }

    static RecordsIterator<String> createIterator(
        Records records,
        BufferSupplier bufferSupplier,
        boolean validateCrc
    ) {
        return new RecordsIterator<>(records, STRING_SERDE, bufferSupplier, Records.HEADER_SIZE_UP_TO_MAGIC, validateCrc);
    }

    static BufferSupplier mockBufferSupplier(Set<ByteBuffer> buffers) {
        BufferSupplier bufferSupplier = Mockito.mock(BufferSupplier.class);

        Mockito.when(bufferSupplier.get(Mockito.anyInt())).thenAnswer(invocation -> {
            int size = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffers.add(buffer);
            return buffer;
        });

        Mockito.doAnswer(invocation -> {
            ByteBuffer released = invocation.getArgument(0);
            buffers.remove(released);
            return null;
        }).when(bufferSupplier).release(Mockito.any(ByteBuffer.class));

        return bufferSupplier;
    }

    public static List<TestBatch<String>> createBatches(long seed) {
        Random random = new Random(seed);
        long baseOffset = random.nextInt(100);
        int epoch = random.nextInt(3) + 1;
        long appendTimestamp = random.nextInt(1000);

        int numberOfBatches = random.nextInt(100) + 1;
        List<TestBatch<String>> batches = new ArrayList<>(numberOfBatches);
        for (int i = 0; i < numberOfBatches; i++) {
            int numberOfRecords = random.nextInt(100) + 1;
            List<String> records = random
                .ints(numberOfRecords, 0, 10)
                .mapToObj(String::valueOf)
                .collect(Collectors.toList());

            batches.add(new TestBatch<>(baseOffset, epoch, appendTimestamp, records));
            baseOffset += records.size();
            if (i % 5 == 0) {
                epoch += random.nextInt(3);
            }
            appendTimestamp += random.nextInt(1000);
        }

        return batches;
    }

    public static MemoryRecords buildControlRecords(ControlRecordType type) {
        final MemoryRecords records;
        switch (type) {
            case LEADER_CHANGE:
                records = MemoryRecords.withLeaderChangeMessage(
                    0,
                    0,
                    1,
                    ByteBuffer.allocate(128),
                    new LeaderChangeMessage()
                );
                break;
            case SNAPSHOT_HEADER:
                records = MemoryRecords.withSnapshotHeaderRecord(
                    0,
                    0,
                    1,
                    ByteBuffer.allocate(128),
                    new SnapshotHeaderRecord()
                );
                break;
            case SNAPSHOT_FOOTER:
                records = MemoryRecords.withSnapshotFooterRecord(
                    0,
                    0,
                    1,
                    ByteBuffer.allocate(128),
                    new SnapshotFooterRecord()
                );
                break;
            default:
                throw new RuntimeException(String.format("Control record type %s is not supported", type));
        }

        return records;
    }

    public static MemoryRecords buildRecords(
        CompressionType compressionType,
        List<TestBatch<String>> batches
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(102400);

        for (TestBatch<String> batch : batches) {
            BatchBuilder<String> builder = new BatchBuilder<>(
                buffer,
                STRING_SERDE,
                compressionType,
                batch.baseOffset,
                batch.appendTimestamp,
                false,
                batch.epoch,
                1024
            );

            for (String record : batch.records) {
                builder.appendRecord(record, null);
            }

            builder.build();
        }

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    public static final class TestBatch<T> {
        final long baseOffset;
        final int epoch;
        final long appendTimestamp;
        final List<T> records;

        TestBatch(long baseOffset, int epoch, long appendTimestamp, List<T> records) {
            this.baseOffset = baseOffset;
            this.epoch = epoch;
            this.appendTimestamp = appendTimestamp;
            this.records = records;
        }

        @Override
        public String toString() {
            return String.format(
                "TestBatch(baseOffset=%s, epoch=%s, records=%s)",
                baseOffset,
                epoch,
                records
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestBatch<?> testBatch = (TestBatch<?>) o;
            return baseOffset == testBatch.baseOffset &&
                epoch == testBatch.epoch &&
                Objects.equals(records, testBatch.records);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseOffset, epoch, records);
        }

        static <T> TestBatch<T> from(Batch<T> batch) {
            return new TestBatch<>(batch.baseOffset(), batch.epoch(), batch.appendTimestamp(), batch.records());
        }
    }
}
