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

package com.automq.stream.s3.wal;

import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.automq.stream.s3.wal.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;
import static com.automq.stream.s3.wal.WriteAheadLog.OverCapacityException;
import static com.automq.stream.s3.wal.WriteAheadLog.RecoverResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class BlockWALServiceTest {

    @Test
    public void testSingleThreadAppendBasic() throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        final int recordCount = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), blockDeviceCapacity)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
                .build()
                .start();
        try {
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);

                final AppendResult appendResult = wal.append(data);

                final long expectedOffset = i * WALUtil.alignLargeByBlockSize(recordSize);
                assertEquals(expectedOffset, appendResult.recordOffset());
                assertEquals(0, appendResult.recordOffset() % WALUtil.BLOCK_SIZE);
                appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                    assertNull(throwable);
                    assertTrue(callbackResult.flushedOffset() > expectedOffset, "flushedOffset: " + callbackResult.flushedOffset() + ", expectedOffset: " + expectedOffset);
                    assertEquals(0, callbackResult.flushedOffset() % WALUtil.alignLargeByBlockSize(recordSize));
                });
            }
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testSingleThreadAppendWhenOverCapacity() throws IOException, InterruptedException {
        final int recordSize = 4096 + 1;
        final int recordCount = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount / 3 + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), blockDeviceCapacity)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
                .build()
                .start();
        try {
            AtomicLong appendedOffset = new AtomicLong(-1);
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);
                AppendResult appendResult;

                while (true) {
                    try {
                        appendResult = wal.append(data);
                    } catch (OverCapacityException e) {
                        Thread.yield();
                        long appendedOffsetValue = appendedOffset.get();
                        if (appendedOffsetValue < 0) {
                            Thread.sleep(100);
                            continue;
                        }
                        wal.trim(appendedOffsetValue).join();
                        continue;
                    }
                    break;
                }

                final long recordOffset = appendResult.recordOffset();
                assertEquals(0, recordOffset % WALUtil.BLOCK_SIZE);
                appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                    assertNull(throwable);
                    assertTrue(callbackResult.flushedOffset() > recordOffset, "flushedOffset: " + callbackResult.flushedOffset() + ", recordOffset: " + recordOffset);
                    assertEquals(0, callbackResult.flushedOffset() % WALUtil.alignLargeByBlockSize(recordSize));

                    // Update the appended offset.
                    long old;
                    do {
                        old = appendedOffset.get();
                        if (old >= recordOffset) {
                            break;
                        }
                    } while (!appendedOffset.compareAndSet(old, recordOffset));
                });
            }
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testMultiThreadAppend() throws InterruptedException, IOException {
        final int recordSize = 4096 + 1;
        final int recordCount = 10;
        final int threadCount = 8;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount * threadCount + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), blockDeviceCapacity)
                .build()
                .start();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        try {
            for (int t = 0; t < threadCount; t++) {
                executorService.submit(() -> Assertions.assertDoesNotThrow(() -> {
                    for (int i = 0; i < recordCount; i++) {
                        ByteBuf data = TestUtils.random(recordSize);

                        final AppendResult appendResult = wal.append(data);

                        appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                            assertNull(throwable);
                            assertEquals(0, appendResult.recordOffset() % WALUtil.BLOCK_SIZE);
                            assertTrue(callbackResult.flushedOffset() > appendResult.recordOffset(), "flushedOffset: " + callbackResult.flushedOffset() + ", recordOffset: " + appendResult.recordOffset());
                            assertEquals(0, callbackResult.flushedOffset() % WALUtil.alignLargeByBlockSize(recordSize));
                        });
                    }
                }));
            }
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(15, TimeUnit.SECONDS));
            wal.shutdownGracefully();
        }
    }

    private long append(WriteAheadLog wal, int recordSize) throws OverCapacityException {
        final AppendResult appendResult = wal.append(TestUtils.random(recordSize));
        final long recordOffset = appendResult.recordOffset();
        assertEquals(0, recordOffset % WALUtil.BLOCK_SIZE);
        appendResult.future().whenComplete((callbackResult, throwable) -> {
            assertNull(throwable);
            assertTrue(callbackResult.flushedOffset() > recordOffset, "flushedOffset: " + callbackResult.flushedOffset() + ", recordOffset: " + recordOffset);
            assertEquals(0, callbackResult.flushedOffset() % WALUtil.BLOCK_SIZE);
        }).join();
        return recordOffset;
    }

    private List<Long> append(WriteAheadLog wal, int recordSize, int recordCount) {
        List<Long> recordOffsets = new ArrayList<>(recordCount);
        long offset = 0;
        for (int i = 0; i < recordCount; i++) {
            try {
                offset = append(wal, recordSize);
                recordOffsets.add(offset);
            } catch (OverCapacityException e) {
                wal.trim(offset).join();
                final long trimmedOffset = offset;
                recordOffsets = recordOffsets.stream()
                        .filter(recordOffset -> recordOffset > trimmedOffset)
                        .collect(Collectors.toList());
                i--;
            }
        }
        return recordOffsets;
    }

    private List<Long> appendWithoutTrim(WriteAheadLog wal, int recordSize, int recordCount) throws OverCapacityException {
        List<Long> recordOffsets = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            long offset = append(wal, recordSize);
            recordOffsets.add(offset);
        }
        return recordOffsets;
    }

    @ParameterizedTest(name = "Test {index}: shutdown={0}, overCapacity={1}, recordCount={2}")
    @CsvSource({
            "true, false, 10",
            "true, true, 9",
            "true, true, 10",
            "true, true, 11",

            "false, false, 10",
            "false, true, 9",
            "false, true, 10",
            "false, true, 11",
    })
    public void testSingleThreadRecover(boolean shutdown, boolean overCapacity, int recordCount) throws IOException {
        final int recordSize = 4096 + 1;
        long blockDeviceCapacity;
        if (overCapacity) {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount / 3 + WAL_HEADER_TOTAL_CAPACITY;
        } else {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount + WAL_HEADER_TOTAL_CAPACITY;
        }
        final String tempFilePath = TestUtils.tempFilePath();

        // Append records
        final WriteAheadLog previousWAL = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        List<Long> appended = append(previousWAL, recordSize, recordCount);
        if (shutdown) {
            previousWAL.shutdownGracefully();
        }

        // Recover records
        final WriteAheadLog wal = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .build()
                .start();
        try {
            Iterator<RecoverResult> recover = wal.recover();
            assertNotNull(recover);

            List<Long> recovered = new ArrayList<>(recordCount);
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                recovered.add(next.recordOffset());
            }
            assertEquals(appended, recovered);
            wal.reset().join();
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testAppendAfterRecover() throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        final String tempFilePath = TestUtils.tempFilePath();

        final WriteAheadLog previousWAL = BlockWALService.builder(tempFilePath, 1 << 20)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        // Append 2 records
        long appended0 = append(previousWAL, recordSize);
        assertEquals(0, appended0);
        long appended1 = append(previousWAL, recordSize);
        assertEquals(WALUtil.alignLargeByBlockSize(recordSize), appended1);

        final WriteAheadLog wal = BlockWALService.builder(tempFilePath, 1 << 20)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        try {
            // Recover records
            Iterator<RecoverResult> recover = wal.recover();
            assertNotNull(recover);

            List<Long> recovered = new ArrayList<>();
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                recovered.add(next.recordOffset());
            }
            assertEquals(Arrays.asList(appended0, appended1), recovered);

            // Reset after recover
            wal.reset().join();

            // Append another 2 records
            long appended2 = append(wal, recordSize);
            long appended3 = append(wal, recordSize);
            assertEquals(WALUtil.alignLargeByBlockSize(recordSize) + appended2, appended3);
        } finally {
            wal.shutdownGracefully();
        }
    }


    private ByteBuffer recordHeader(ByteBuffer body, long offset) {
        return new SlidingWindowService.RecordHeaderCoreData()
                .setMagicCode(BlockWALService.RECORD_HEADER_MAGIC_CODE)
                .setRecordBodyLength(body.limit())
                .setRecordBodyOffset(offset + BlockWALService.RECORD_HEADER_SIZE)
                .setRecordBodyCRC(WALUtil.crc32(body))
                .marshal();
    }

    private void write(WALChannel walChannel, long logicOffset, int recordSize) throws IOException {
        ByteBuffer recordBody = TestUtils.random(recordSize).nioBuffer();
        ByteBuffer recordHeader = recordHeader(recordBody, logicOffset);

        ByteBuffer record = ByteBuffer.allocate(recordHeader.limit() + recordBody.limit());
        record.put(recordHeader);
        record.put(recordBody);
        record.position(0);

        // TODO: make this beautiful
        long position = WALUtil.recordOffsetToPosition(logicOffset, walChannel.capacity() - WAL_HEADER_TOTAL_CAPACITY, WAL_HEADER_TOTAL_CAPACITY);
        walChannel.write(record, position);
    }

    private void writeWALHeader(WALChannel walChannel, long trimOffset, long startOffset, long nextOffset, long maxLength) throws IOException {
        ByteBuffer header = new BlockWALService.WALHeaderCoreData()
                .setCapacity(walChannel.capacity())
                .updateTrimOffset(trimOffset)
                .setSlidingWindowStartOffset(startOffset)
                .setSlidingWindowNextWriteOffset(nextOffset)
                .setSlidingWindowMaxLength(maxLength)
                .marshal();
        walChannel.write(header, 0);
    }

    private static class RecoverFromDisasterParam {
        public static final long CAPACITY = (long) WALUtil.BLOCK_SIZE * (100 + 2);
        public static final int RECORD_SIZE = WALUtil.BLOCK_SIZE + 1;
        // WAL header
        long trimOffset;
        long startOffset;
        long nextOffset;
        long maxLength;
        // WAL records
        List<Long> writeOffsets;
        List<Long> recoveredOffsets;

        public RecoverFromDisasterParam(
                long trimOffsetBlock,
                long startOffsetBlock,
                long nextOffsetBlock,
                long maxLengthBlock,
                List<Long> writeOffsetsBlock,
                List<Long> recoveredOffsetsBlock
        ) {
            this.trimOffset = trimOffsetBlock * WALUtil.BLOCK_SIZE;
            this.startOffset = startOffsetBlock * WALUtil.BLOCK_SIZE;
            this.nextOffset = nextOffsetBlock * WALUtil.BLOCK_SIZE;
            this.maxLength = maxLengthBlock * WALUtil.BLOCK_SIZE;
            this.writeOffsets = writeOffsetsBlock.stream().map(offset -> offset * WALUtil.BLOCK_SIZE).collect(Collectors.toList());
            this.recoveredOffsets = recoveredOffsetsBlock.stream().map(offset -> offset * WALUtil.BLOCK_SIZE).collect(Collectors.toList());
        }

        public Arguments toArguments(String name) {
            return Arguments.of(name, trimOffset, startOffset, nextOffset, maxLength, writeOffsets, recoveredOffsets);
        }
    }

    public static Stream<Arguments> testRecoverFromDisasterData() {
        return Stream.of(
                new RecoverFromDisasterParam(
                        0,
                        0,
                        0,
                        50,
                        Arrays.asList(0L, 2L, 4L),
                        Arrays.asList(0L, 2L, 4L)
                ).toArguments("base"),
                new RecoverFromDisasterParam(
                        0,
                        10,
                        10,
                        50,
                        Arrays.asList(0L, 2L, 4L),
                        Arrays.asList(0L, 2L, 4L)
                ).toArguments("trimmed at zero (not trimmed)"),
                new RecoverFromDisasterParam(
                        2,
                        10,
                        10,
                        50,
                        Arrays.asList(0L, 2L, 4L, 6L),
                        Arrays.asList(4L, 6L)
                ).toArguments("trimmed"),
                new RecoverFromDisasterParam(
                        2,
                        3,
                        3,
                        50,
                        Arrays.asList(0L, 2L, 4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L),
                        Arrays.asList(4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L)
                ).toArguments("WAL header flushed slow"),
                new RecoverFromDisasterParam(
                        2,
                        3,
                        3,
                        50,
                        Arrays.asList(0L, 2L, 8L, 10L, 14L, 20L),
                        Arrays.asList(8L, 10L, 14L, 20L)
                ).toArguments("many invalid records"),
                new RecoverFromDisasterParam(
                        2,
                        3,
                        3,
                        50,
                        Arrays.asList(14L, 8L, 10L, 20L, 0L, 2L),
                        Arrays.asList(8L, 10L, 14L, 20L)
                ).toArguments("write in random order"),
                new RecoverFromDisasterParam(
                        20230920,
                        20230920,
                        20230920,
                        50,
                        Arrays.asList(20230900L, 20230910L, 20230916L, 20230920L, 20230930L, 20230940L, 20230950L, 20230960L, 20230970L),
                        Arrays.asList(20230930L, 20230940L, 20230950L, 20230960L, 20230970L)
                ).toArguments("big logic offset"),
                new RecoverFromDisasterParam(
                        180,
                        210,
                        240,
                        50,
                        Arrays.asList(150L, 160L, 170L, 180L, 190L, 200L, 202L, 210L, 220L, 230L, 240L),
                        Arrays.asList(190L, 200L, 202L, 210L, 220L, 230L, 240L)
                ).toArguments("round robin"),
                new RecoverFromDisasterParam(
                        210,
                        250,
                        290,
                        50,
                        Arrays.asList(111L, 113L, 115L, 117L, 119L, 120L, 130L,
                                210L, 215L, 220L, 230L, 240L, 250L, 260L, 270L, 280L, 290L),
                        Arrays.asList(215L, 220L, 230L, 240L, 250L, 260L, 270L, 280L, 290L)
                ).toArguments("overwrite")
                // TODO: test window max length
        );
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverFromDisasterData")
    public void testRecoverFromDisaster(
            String name,
            long trimOffset,
            long startOffset,
            long nextOffset,
            long maxLength,
            List<Long> writeOffsets,
            List<Long> recoveredOffsets
    ) throws IOException {
        final String tempFilePath = TestUtils.tempFilePath();
        final WALChannel walChannel = WALChannel.builder(tempFilePath, RecoverFromDisasterParam.CAPACITY).build();

        // Simulate disaster
        walChannel.open();
        writeWALHeader(walChannel, trimOffset, startOffset, nextOffset, maxLength);
        for (long writeOffset : writeOffsets) {
            write(walChannel, writeOffset, RecoverFromDisasterParam.RECORD_SIZE);
        }
        walChannel.close();

        final WriteAheadLog wal = BlockWALService.builder(tempFilePath, RecoverFromDisasterParam.CAPACITY)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        try {
            // Recover records
            Iterator<RecoverResult> recover = wal.recover();
            assertNotNull(recover);

            List<Long> recovered = new ArrayList<>();
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                recovered.add(next.recordOffset());
            }
            assertEquals(recoveredOffsets, recovered, name);
            wal.reset().join();
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testRecoverAfterReset() throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        final int recordCount = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount * 2 + WAL_HEADER_TOTAL_CAPACITY;
        final String tempFilePath = TestUtils.tempFilePath();

        // 1. append and force shutdown
        final WriteAheadLog wal1 = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        List<Long> appended1 = appendWithoutTrim(wal1, recordSize, recordCount);

        // 2. recover and reset
        final WriteAheadLog wal2 = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        Iterator<RecoverResult> recover = wal2.recover();
        assertNotNull(recover);
        List<Long> recovered1 = new ArrayList<>(recordCount);
        while (recover.hasNext()) {
            RecoverResult next = recover.next();
            recovered1.add(next.recordOffset());
        }
        assertEquals(appended1, recovered1);
        wal2.reset().join();

        // 3. append and force shutdown again
        List<Long> appended2 = appendWithoutTrim(wal2, recordSize, recordCount);

        // 4. recover again
        final WriteAheadLog wal3 = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        recover = wal3.recover();
        assertNotNull(recover);
        List<Long> recovered2 = new ArrayList<>(recordCount);
        while (recover.hasNext()) {
            RecoverResult next = recover.next();
            recovered2.add(next.recordOffset());
        }
        assertEquals(appended2, recovered2);
    }

    @Test
    public void testTrimInvalidOffset() throws IOException, OverCapacityException {
        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), 16384)
                .build()
                .start();
        try {
            long appended = append(wal, 42);
            Assertions.assertThrows(IllegalArgumentException.class, () -> wal.trim(appended + 4096 + 1).join());
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testWindowGreaterThanCapacity() throws IOException, OverCapacityException {
        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), WALUtil.BLOCK_SIZE * 3L)
                .slidingWindowUpperLimit(WALUtil.BLOCK_SIZE * 4L)
                .build()
                .start();
        try {
            append(wal, 42);
            Assertions.assertThrows(OverCapacityException.class, () -> append(wal, 42));
        } finally {
            wal.shutdownGracefully();
        }
    }
}
