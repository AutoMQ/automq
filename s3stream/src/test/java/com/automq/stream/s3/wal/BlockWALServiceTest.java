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

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.wal.benchmark.WriteBench;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_SIZE;
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

    static final String TEST_BLOCK_DEVICE = System.getenv("WAL_TEST_BLOCK_DEVICE");

    @ParameterizedTest(name = "Test {index}: mergeWrite={0}")
    @ValueSource(booleans = {false, true})
    public void testSingleThreadAppendBasic(boolean mergeWrite) throws IOException, OverCapacityException {
        testSingleThreadAppendBasic0(mergeWrite, false);
    }

    @ParameterizedTest(name = "Test {index}: mergeWrite={0}")
    @ValueSource(booleans = {false, true})
    @EnabledOnOs(OS.LINUX)
    public void testSingleThreadAppendBasicDirectIO(boolean mergeWrite) throws IOException, OverCapacityException {
        testSingleThreadAppendBasic0(mergeWrite, true);
    }

    private static void testSingleThreadAppendBasic0(boolean mergeWrite, boolean directIO) throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        final int recordCount = 100;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount + WAL_HEADER_TOTAL_CAPACITY;

        String path = TestUtils.tempFilePath();
        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, blockDeviceCapacity);
        }

        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096);
        if (!mergeWrite) {
            builder.blockSoftLimit(0);
        }
        final WriteAheadLog wal = builder.build().start();

        AtomicLong maxFlushedOffset = new AtomicLong(-1);
        AtomicLong maxRecordOffset = new AtomicLong(-1);
        try {
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);

                final AppendResult appendResult = wal.append(data.retainedDuplicate());

                if (!mergeWrite) {
                    assertEquals(i * WALUtil.alignLargeByBlockSize(recordSize), appendResult.recordOffset());
                    assertEquals(0, appendResult.recordOffset() % WALUtil.BLOCK_SIZE);
                }
                appendResult.future().whenComplete((callbackResult, throwable) -> {
                    assertNull(throwable);
                    maxFlushedOffset.accumulateAndGet(callbackResult.flushedOffset(), Math::max);
                    maxRecordOffset.accumulateAndGet(appendResult.recordOffset(), Math::max);
                    if (!mergeWrite) {
                        assertEquals(0, callbackResult.flushedOffset() % WALUtil.alignLargeByBlockSize(recordSize));
                    } else {
                        assertEquals(0, callbackResult.flushedOffset() % WALUtil.BLOCK_SIZE);
                    }
                }).whenComplete((callbackResult, throwable) -> {
                    if (null != throwable) {
                        throwable.printStackTrace();
                        System.exit(1);
                    }
                });
            }
        } finally {
            wal.shutdownGracefully();
        }
        assertTrue(maxFlushedOffset.get() > maxRecordOffset.get(),
                "maxFlushedOffset should be greater than maxRecordOffset. maxFlushedOffset: " + maxFlushedOffset.get() + ", maxRecordOffset: " + maxRecordOffset.get());
    }

    @ParameterizedTest(name = "Test {index}: mergeWrite={0}")
    @ValueSource(booleans = {false, true})
    public void testSingleThreadAppendWhenOverCapacity(boolean mergeWrite) throws IOException {
        testSingleThreadAppendWhenOverCapacity0(mergeWrite, false);
    }

    @ParameterizedTest(name = "Test {index}: mergeWrite={0}")
    @ValueSource(booleans = {false, true})
    @EnabledOnOs(OS.LINUX)
    public void testSingleThreadAppendWhenOverCapacityDirectIO(boolean mergeWrite) throws IOException {
        testSingleThreadAppendWhenOverCapacity0(mergeWrite, true);
    }

    private static void testSingleThreadAppendWhenOverCapacity0(boolean mergeWrite, boolean directIO) throws IOException {
        final int recordSize = 4096 + 1;
        final int recordCount = 100;
        long blockDeviceCapacity;
        if (!mergeWrite) {
            blockDeviceCapacity = recordCount / 3 * WALUtil.alignLargeByBlockSize(recordSize) + WAL_HEADER_TOTAL_CAPACITY;
        } else {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize * recordCount / 3) + WAL_HEADER_TOTAL_CAPACITY;
        }

        String path = TestUtils.tempFilePath();
        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(blockDeviceCapacity);
            resetBlockDevice(path, blockDeviceCapacity);
        }

        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096);
        if (!mergeWrite) {
            builder.blockSoftLimit(0);
        }
        final WriteAheadLog wal = builder.build().start();

        AtomicLong maxFlushedOffset = new AtomicLong(-1);
        AtomicLong maxRecordOffset = new AtomicLong(-1);
        try {
            WriteBench.TrimOffset trimOffset = new WriteBench.TrimOffset();
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);
                AppendResult appendResult;

                while (true) {
                    try {
                        appendResult = wal.append(data.retainedDuplicate());
                    } catch (OverCapacityException e) {
                        Thread.yield();
                        wal.trim(trimOffset.get()).join();
                        continue;
                    }
                    break;
                }

                final long recordOffset = appendResult.recordOffset();
                if (!mergeWrite) {
                    assertEquals(0, recordOffset % WALUtil.BLOCK_SIZE);
                }
                trimOffset.appended(recordOffset);
                appendResult.future().whenComplete((callbackResult, throwable) -> {
                    assertNull(throwable);
                    maxFlushedOffset.accumulateAndGet(callbackResult.flushedOffset(), Math::max);
                    maxRecordOffset.accumulateAndGet(recordOffset, Math::max);
                    if (!mergeWrite) {
                        assertEquals(0, callbackResult.flushedOffset() % WALUtil.alignLargeByBlockSize(recordSize));
                    } else {
                        assertEquals(0, callbackResult.flushedOffset() % WALUtil.BLOCK_SIZE);
                    }

                    trimOffset.flushed(callbackResult.flushedOffset());
                }).whenComplete((callbackResult, throwable) -> {
                    if (null != throwable) {
                        throwable.printStackTrace();
                        System.exit(1);
                    }
                });
            }
        } finally {
            wal.shutdownGracefully();
        }
        assertTrue(maxFlushedOffset.get() > maxRecordOffset.get(),
                "maxFlushedOffset should be greater than maxRecordOffset. maxFlushedOffset: " + maxFlushedOffset.get() + ", maxRecordOffset: " + maxRecordOffset.get());
    }

    @ParameterizedTest(name = "Test {index}: mergeWrite={0}")
    @ValueSource(booleans = {false, true})
    public void testMultiThreadAppend(boolean mergeWrite) throws InterruptedException, IOException {
        testMultiThreadAppend0(mergeWrite, false);
    }

    @ParameterizedTest(name = "Test {index}: mergeWrite={0}")
    @ValueSource(booleans = {false, true})
    @EnabledOnOs(OS.LINUX)
    public void testMultiThreadAppendDirectIO(boolean mergeWrite) throws InterruptedException, IOException {
        testMultiThreadAppend0(mergeWrite, true);
    }

    private static void testMultiThreadAppend0(boolean mergeWrite, boolean directIO) throws IOException, InterruptedException {
        final int recordSize = 4096 + 1;
        final int recordCount = 10;
        final int threadCount = 8;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount * threadCount + WAL_HEADER_TOTAL_CAPACITY;

        String path = TestUtils.tempFilePath();
        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, blockDeviceCapacity);
        }

        BlockWALService.BlockWALServiceBuilder builder = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO);
        if (!mergeWrite) {
            builder.blockSoftLimit(0);
        }
        final WriteAheadLog wal = builder.build().start();

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        AtomicLong maxFlushedOffset = new AtomicLong(-1);
        AtomicLong maxRecordOffset = new AtomicLong(-1);
        try {
            for (int t = 0; t < threadCount; t++) {
                executorService.submit(() -> Assertions.assertDoesNotThrow(() -> {
                    for (int i = 0; i < recordCount; i++) {
                        ByteBuf data = TestUtils.random(recordSize);

                        final AppendResult appendResult = wal.append(data.retainedDuplicate());

                        appendResult.future().whenComplete((callbackResult, throwable) -> {
                            assertNull(throwable);
                            if (!mergeWrite) {
                                assertEquals(0, appendResult.recordOffset() % WALUtil.BLOCK_SIZE);
                            }
                            maxFlushedOffset.accumulateAndGet(callbackResult.flushedOffset(), Math::max);
                            maxRecordOffset.accumulateAndGet(appendResult.recordOffset(), Math::max);
                            if (!mergeWrite) {
                                assertEquals(0, callbackResult.flushedOffset() % WALUtil.alignLargeByBlockSize(recordSize));
                            } else {
                                assertEquals(0, callbackResult.flushedOffset() % WALUtil.BLOCK_SIZE);
                            }
                        }).whenComplete((callbackResult, throwable) -> {
                            if (null != throwable) {
                                throwable.printStackTrace();
                                System.exit(1);
                            }
                        });
                    }
                }));
            }
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(15, TimeUnit.SECONDS));
            wal.shutdownGracefully();
        }
        assertTrue(maxFlushedOffset.get() > maxRecordOffset.get(),
                "maxFlushedOffset should be greater than maxRecordOffset. maxFlushedOffset: " + maxFlushedOffset.get() + ", maxRecordOffset: " + maxRecordOffset.get());
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
        testSingleThreadRecover0(shutdown, overCapacity, recordCount, false);
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
    @EnabledOnOs(OS.LINUX)
    public void testSingleThreadRecoverDirectIO(boolean shutdown, boolean overCapacity, int recordCount) throws IOException {
        testSingleThreadRecover0(shutdown, overCapacity, recordCount, true);
    }

    private void testSingleThreadRecover0(boolean shutdown, boolean overCapacity, int recordCount, boolean directIO) throws IOException {
        final int recordSize = 4096 + 1;
        long blockDeviceCapacity;
        if (overCapacity) {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount / 3 + WAL_HEADER_TOTAL_CAPACITY;
        } else {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount + WAL_HEADER_TOTAL_CAPACITY;
        }
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(blockDeviceCapacity);
            resetBlockDevice(path, blockDeviceCapacity);
        }

        // Append records
        final WriteAheadLog previousWAL = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        List<Long> appended = append(previousWAL, recordSize, recordCount);
        if (shutdown) {
            previousWAL.shutdownGracefully();
        }

        // Recover records
        final WriteAheadLog wal = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .build()
                .start();
        try {
            Iterator<RecoverResult> recover = wal.recover();
            assertNotNull(recover);

            List<Long> recovered = new ArrayList<>(recordCount);
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                next.record().release();
                recovered.add(next.recordOffset());
            }
            assertEquals(appended, recovered);
            wal.reset().join();
        } finally {
            wal.shutdownGracefully();
        }
    }

    @ParameterizedTest(name = "Test {index}: shutdown={0}, overCapacity={1}")
    @CsvSource({
        "true, false",
        "true, true",
        "false, false",
        "false, true",
    })
    public void testRecoverAfterMergeWrite(boolean shutdown, boolean overCapacity) throws IOException {
        testRecoverAfterMergeWrite0(shutdown, overCapacity, false);
    }

    @ParameterizedTest(name = "Test {index}: shutdown={0}, overCapacity={1}")
    @CsvSource({
        "true, false",
        "true, true",
        "false, false",
        "false, true",
    })
    @EnabledOnOs(OS.LINUX)
    public void testRecoverAfterMergeWriteDirectIO(boolean shutdown, boolean overCapacity) throws IOException {
        testRecoverAfterMergeWrite0(shutdown, overCapacity, true);
    }

    private static void testRecoverAfterMergeWrite0(boolean shutdown, boolean overCapacity, boolean directIO) throws IOException {
        final int recordSize = 1024 + 1;
        final int recordCount = 100;
        long blockDeviceCapacity;
        if (overCapacity) {
            blockDeviceCapacity = recordSize * recordCount + WAL_HEADER_TOTAL_CAPACITY;
        } else {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount + WAL_HEADER_TOTAL_CAPACITY;
        }
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(blockDeviceCapacity);
            resetBlockDevice(path, blockDeviceCapacity);
        }

        // Append records
        final WriteAheadLog previousWAL = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        List<Long> appended = appendAsync(previousWAL, recordSize, recordCount);
        if (shutdown) {
            previousWAL.shutdownGracefully();
        }

        // Recover records
        final WriteAheadLog wal = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .build()
                .start();
        try {
            Iterator<RecoverResult> recover = wal.recover();
            assertNotNull(recover);

            List<Long> recovered = new ArrayList<>(recordCount);
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                next.record().release();
                recovered.add(next.recordOffset());
            }
            assertEquals(appended, recovered);
            wal.reset().join();
        } finally {
            wal.shutdownGracefully();
        }
    }

    private static List<Long> appendAsync(WriteAheadLog wal, int recordSize, int recordCount) {
        List<Long> appended = new ArrayList<>(recordCount);
        List<CompletableFuture<Void>> appendFutures = new LinkedList<>();
        WriteBench.TrimOffset trimOffset = new WriteBench.TrimOffset();
        for (int i = 0; i < recordCount; i++) {
            ByteBuf data = TestUtils.random(recordSize);
            AppendResult appendResult;
            try {
                appendResult = wal.append(data.retainedDuplicate());
            } catch (OverCapacityException e) {
                long offset = trimOffset.get();
                wal.trim(offset).join();
                appended = appended.stream()
                        .filter(recordOffset -> recordOffset > offset)
                        .collect(Collectors.toList());
                i--;
                continue;
            }
            appended.add(appendResult.recordOffset());
            trimOffset.appended(appendResult.recordOffset());
            appendFutures.add(appendResult.future().whenComplete((callbackResult, throwable) -> {
                assertNull(throwable);
                assertEquals(0, callbackResult.flushedOffset() % WALUtil.BLOCK_SIZE);
                trimOffset.flushed(callbackResult.flushedOffset());
            }).whenComplete((callbackResult, throwable) -> {
                if (null != throwable) {
                    throwable.printStackTrace();
                    System.exit(1);
                }
            }).thenApply(ignored -> null));
        }
        CompletableFuture.allOf(appendFutures.toArray(new CompletableFuture[0])).join();
        return appended;
    }

    @Test
    public void testAppendAfterRecover() throws IOException, OverCapacityException {
        testAppendAfterRecover0(false);
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void testAppendAfterRecoverDirectIO() throws IOException, OverCapacityException {
        testAppendAfterRecover0(true);
    }

    private void testAppendAfterRecover0(boolean directIO) throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, 1 << 20);
        }

        final WriteAheadLog previousWAL = BlockWALService.builder(path, 1 << 20)
                .direct(directIO)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        // Append 2 records
        long appended0 = append(previousWAL, recordSize);
        assertEquals(0, appended0);
        long appended1 = append(previousWAL, recordSize);
        assertEquals(WALUtil.alignLargeByBlockSize(recordSize), appended1);

        final WriteAheadLog wal = BlockWALService.builder(path, 1 << 20)
                .direct(directIO)
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
                next.record().release();
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


    private ByteBuf recordHeader(ByteBuf body, long offset) {
        return new SlidingWindowService.RecordHeaderCoreData()
                .setMagicCode(BlockWALService.RECORD_HEADER_MAGIC_CODE)
                .setRecordBodyLength(body.readableBytes())
                .setRecordBodyOffset(offset + BlockWALService.RECORD_HEADER_SIZE)
                .setRecordBodyCRC(WALUtil.crc32(body))
                .marshal();
    }

    private void write(WALChannel walChannel, long logicOffset, int recordSize) throws IOException {
        ByteBuf recordBody = TestUtils.random(recordSize - RECORD_HEADER_SIZE);
        ByteBuf recordHeader = recordHeader(recordBody, logicOffset);

        CompositeByteBuf record = DirectByteBufAlloc.compositeByteBuffer();
        record.addComponents(true, recordHeader, recordBody);

        // TODO: make this beautiful
        long position = WALUtil.recordOffsetToPosition(logicOffset, walChannel.capacity() - WAL_HEADER_TOTAL_CAPACITY, WAL_HEADER_TOTAL_CAPACITY);
        walChannel.writeAndFlush(record, position);
    }

    private void writeWALHeader(WALChannel walChannel, long trimOffset, long startOffset, long nextOffset, long maxLength) throws IOException {
        ByteBuf header = new WALHeader()
                .setCapacity(walChannel.capacity())
                .updateTrimOffset(trimOffset)
                .setSlidingWindowStartOffset(startOffset)
                .setSlidingWindowNextWriteOffset(nextOffset)
                .setSlidingWindowMaxLength(maxLength)
                .marshal();
        walChannel.writeAndFlush(header, 0);
    }

    private static class RecoverFromDisasterParam {
        int recordSize;
        long capacity;
        // WAL header
        long trimOffset;
        long startOffset;
        long nextOffset;
        long maxLength;
        // WAL records
        List<Long> writeOffsets;
        List<Long> recoveredOffsets;

        public RecoverFromDisasterParam(
                int recordSize,
                long capacity,
                long trimOffset,
                long startOffset,
                long nextOffset,
                long maxLength,
                List<Long> writeOffsets,
                List<Long> recoveredOffsets,
                int unit
        ) {
            this.recordSize = recordSize;
            this.capacity = capacity * unit + WAL_HEADER_TOTAL_CAPACITY;
            this.trimOffset = trimOffset * unit;
            this.startOffset = startOffset * unit;
            this.nextOffset = nextOffset * unit;
            this.maxLength = maxLength * unit;
            this.writeOffsets = writeOffsets.stream().map(offset -> offset * unit).collect(Collectors.toList());
            this.recoveredOffsets = recoveredOffsets.stream().map(offset -> offset * unit).collect(Collectors.toList());
        }

        public Arguments toArguments(String name) {
            return Arguments.of(name, recordSize, capacity, trimOffset, startOffset, nextOffset, maxLength, writeOffsets, recoveredOffsets);
        }
    }

    public static Stream<Arguments> testRecoverFromDisasterData() {
        return Stream.of(
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        -1L,
                        0L,
                        0L,
                        50L,
                        Arrays.asList(0L, 2L, 4L),
                        Arrays.asList(0L, 2L, 4L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("base"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        0L,
                        10L,
                        10L,
                        50L,
                        Arrays.asList(0L, 2L, 4L),
                        Arrays.asList(2L, 4L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("trimmed at zero"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        2L,
                        10L,
                        10L,
                        50L,
                        Arrays.asList(0L, 2L, 4L, 6L),
                        Arrays.asList(4L, 6L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("trimmed"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        2L,
                        3L,
                        3L,
                        50L,
                        Arrays.asList(0L, 2L, 4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L),
                        Arrays.asList(4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("WAL header flushed slow"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        2L,
                        3L,
                        3L,
                        50L,
                        Arrays.asList(0L, 2L, 8L, 10L, 14L, 20L),
                        Arrays.asList(8L, 10L, 14L, 20L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("many invalid records"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        2L,
                        3L,
                        3L,
                        50L,
                        Arrays.asList(14L, 8L, 10L, 20L, 0L, 2L),
                        Arrays.asList(8L, 10L, 14L, 20L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("write in random order"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        20230920L,
                        20230920L,
                        20230920L,
                        50L,
                        Arrays.asList(20230900L, 20230910L, 20230916L, 20230920L, 20230930L, 20230940L, 20230950L, 20230960L, 20230970L),
                        Arrays.asList(20230930L, 20230940L, 20230950L, 20230960L, 20230970L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("big logic offset"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        180L,
                        210L,
                        240L,
                        50L,
                        Arrays.asList(150L, 160L, 170L, 180L, 190L, 200L, 202L, 210L, 220L, 230L, 240L),
                        Arrays.asList(190L, 200L, 202L, 210L, 220L, 230L, 240L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("round robin"),
                new RecoverFromDisasterParam(
                        WALUtil.BLOCK_SIZE + 1,
                        100L,
                        210L,
                        250L,
                        290L,
                        50L,
                        Arrays.asList(111L, 113L, 115L, 117L, 119L, 120L, 130L,
                                210L, 215L, 220L, 230L, 240L, 250L, 260L, 270L, 280L, 290L),
                        Arrays.asList(215L, 220L, 230L, 240L, 250L, 260L, 270L, 280L, 290L),
                        WALUtil.BLOCK_SIZE
                ).toArguments("overwrite"),
                // TODO: test window max length
                new RecoverFromDisasterParam(
                        42,
                        8192L,
                        -1L,
                        0L,
                        0L,
                        8192L,
                        Arrays.asList(0L, 42L, 84L),
                        Arrays.asList(0L, 42L, 84L),
                        1
                ).toArguments("merge write - base"),
                new RecoverFromDisasterParam(
                        42,
                        8192L,
                        42L,
                        0L,
                        0L,
                        50L,
                        Arrays.asList(0L, 42L, 84L, 126L),
                        Arrays.asList(84L, 126L),
                        1
                ).toArguments("merge write - trimmed"),
                new RecoverFromDisasterParam(
                        42,
                        8192L,
                        42L,
                        4096L,
                        4096L,
                        50L,
                        Arrays.asList(0L, 42L, 42 * 2L, 42 * 4L, 4096L, 4096L + 42L, 4096L + 42 * 3L),
                        Arrays.asList(42 * 2L, 4096L, 4096L + 42L),
                        1
                ).toArguments("merge write - some invalid records"),
                new RecoverFromDisasterParam(
                        42,
                        8192L,
                        42L,
                        4096L,
                        4096L,
                        50L,
                        Arrays.asList(42L, 42 * 4L, 42 * 2L, 4096L + 42 * 3L, 0L, 4096L, 4096L + 42L),
                        Arrays.asList(42 * 2L, 4096L, 4096L + 42L),
                        1
                ).toArguments("merge write - random order"),
                new RecoverFromDisasterParam(
                        1000,
                        8192L,
                        2000L,
                        4096L,
                        4096L,
                        50L,
                        Arrays.asList(0L, 1000L, 2000L, 3000L, 4000L, 5000L, 7000L),
                        Arrays.asList(3000L, 4000L, 5000L),
                        1
                ).toArguments("merge write - record in the middle"),
                new RecoverFromDisasterParam(
                        42,
                        8192L,
                        8192L + 4096L + 42L,
                        16384L,
                        16384L,
                        50L,
                        Arrays.asList(8192L + 4096L, 8192L + 4096L + 42L, 8192L + 4096L + 42 * 2L, 8192L + 4096L + 42 * 4L, 16384L, 16384L + 42L, 16384L + 42 * 3L),
                        Arrays.asList(8192L + 4096L + 42 * 2L, 16384L, 16384L + 42L),
                        1
                ).toArguments("merge write - round robin"),
                new RecoverFromDisasterParam(
                        1000,
                        8192L,
                        12000L,
                        16384L,
                        16384L,
                        50L,
                        Arrays.asList(1000L, 2000L, 3000L, 4000L, 5000L, 6000L, 7000L,
                                9000L, 10000L, 11000L, 12000L, 13000L, 14000L, 15000L),
                        Arrays.asList(13000L, 14000L, 15000L),
                        1
                ).toArguments("merge write - overwrite")
        );
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverFromDisasterData")
    public void testRecoverFromDisaster(
            String name,
            int recordSize,
            long capacity,
            long trimOffset,
            long startOffset,
            long nextOffset,
            long maxLength,
            List<Long> writeOffsets,
            List<Long> recoveredOffsets
    ) throws IOException {
        testRecoverFromDisaster0(name, recordSize, capacity, trimOffset, startOffset, nextOffset, maxLength, writeOffsets, recoveredOffsets, false);
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverFromDisasterData")
    @EnabledOnOs({OS.LINUX})
    public void testRecoverFromDisasterDirectIO(
            String name,
            int recordSize,
            long capacity,
            long trimOffset,
            long startOffset,
            long nextOffset,
            long maxLength,
            List<Long> writeOffsets,
            List<Long> recoveredOffsets
    ) throws IOException {
        testRecoverFromDisaster0(name, recordSize, capacity, trimOffset, startOffset, nextOffset, maxLength, writeOffsets, recoveredOffsets, true);
    }

    private void testRecoverFromDisaster0(
            String name,
            int recordSize,
            long capacity,
            long trimOffset,
            long startOffset,
            long nextOffset,
            long maxLength,
            List<Long> writeOffsets,
            List<Long> recoveredOffsets,
            boolean directIO
    ) throws IOException {
        String path = TestUtils.tempFilePath();
        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            capacity = WALUtil.alignLargeByBlockSize(capacity);
            resetBlockDevice(path, capacity);
        }

        final WALChannel walChannel = WALChannel.builder(path)
                .capacity(capacity)
                // we may write to un-aligned position here, so we need to disable directIO
                .direct(false)
                .build();

        // Simulate disaster
        walChannel.open();
        writeWALHeader(walChannel, trimOffset, startOffset, nextOffset, maxLength);
        for (long writeOffset : writeOffsets) {
            if (directIO && TEST_BLOCK_DEVICE != null && writeOffset % WALUtil.BLOCK_SIZE != 0) {
                // skip the test as we can't write to un-aligned position on block device
                return;
            }
            write(walChannel, writeOffset, recordSize);
        }
        walChannel.close();

        final WriteAheadLog wal = BlockWALService.builder(path, capacity)
                .direct(directIO)
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
                next.record().release();
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
        testRecoverAfterReset0(false);
    }

    @Test
    @EnabledOnOs({OS.LINUX})
    public void testRecoverAfterResetDirectIO() throws IOException, OverCapacityException {
        testRecoverAfterReset0(true);
    }

    private void testRecoverAfterReset0(boolean directIO) throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        final int recordCount = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount * 2 + WAL_HEADER_TOTAL_CAPACITY;
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, blockDeviceCapacity);
        }

        // 1. append and force shutdown
        final WriteAheadLog wal1 = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        List<Long> appended1 = appendWithoutTrim(wal1, recordSize, recordCount);

        // 2. recover and reset
        final WriteAheadLog wal2 = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        Iterator<RecoverResult> recover = wal2.recover();
        assertNotNull(recover);
        List<Long> recovered1 = new ArrayList<>(recordCount);
        while (recover.hasNext()) {
            RecoverResult next = recover.next();
            next.record().release();
            recovered1.add(next.recordOffset());
        }
        assertEquals(appended1, recovered1);
        wal2.reset().join();

        // 3. append and force shutdown again
        List<Long> appended2 = appendWithoutTrim(wal2, recordSize, recordCount);

        // 4. recover again
        final WriteAheadLog wal3 = BlockWALService.builder(path, blockDeviceCapacity)
                .direct(directIO)
                .flushHeaderIntervalSeconds(1 << 30)
                .build()
                .start();
        recover = wal3.recover();
        assertNotNull(recover);
        List<Long> recovered2 = new ArrayList<>(recordCount);
        while (recover.hasNext()) {
            RecoverResult next = recover.next();
            next.record().release();
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

    /**
     * Write "0"s to the block device to reset it.
     */
    private static void resetBlockDevice(String path, long capacity) throws IOException {
        WALChannel channel = WALChannel.builder(path)
                .capacity(capacity)
                .direct(true)
                .build();
        channel.open();
        ByteBuf buf = Unpooled.buffer((int) capacity);
        buf.writeZero((int) capacity);
        channel.write(buf, 0);
        channel.close();
    }
}
