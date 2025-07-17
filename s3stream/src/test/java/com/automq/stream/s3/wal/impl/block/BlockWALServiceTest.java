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

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.TestUtils;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.benchmark.WriteBench;
import com.automq.stream.s3.wal.common.Record;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALCapacityMismatchException;
import com.automq.stream.s3.wal.exception.WALNotInitializedException;
import com.automq.stream.s3.wal.impl.block.BlockWALService.RecoverIteratorV0;
import com.automq.stream.s3.wal.util.WALBlockDeviceChannel;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.impl.block.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
import static com.automq.stream.s3.wal.util.WALChannelTest.TEST_BLOCK_DEVICE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class BlockWALServiceTest {

    static final String TEST_BLOCK_DEVICE = System.getenv(TEST_BLOCK_DEVICE_KEY);

    private static void testSingleThreadAppendBasic0(boolean mergeWrite,
        boolean directIO) throws IOException, OverCapacityException {
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
        recoverAndReset(wal);

        AtomicLong maxFlushedOffset = new AtomicLong(-1);
        AtomicLong maxRecordOffset = new AtomicLong(-1);
        try {
            for (int i = 0; i < recordCount; i++) {
                ByteBuf data = TestUtils.random(recordSize);

                final AppendResult appendResult = wal.append(data.retainedDuplicate());

                if (!mergeWrite) {
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
                        Assertions.fail();
                    }
                });
            }
        } finally {
            wal.shutdownGracefully();
        }
        assertTrue(maxFlushedOffset.get() > maxRecordOffset.get(),
            "maxFlushedOffset should be greater than maxRecordOffset. maxFlushedOffset: " + maxFlushedOffset.get() + ", maxRecordOffset: " + maxRecordOffset.get());
    }

    private static void testSingleThreadAppendWhenOverCapacity0(boolean mergeWrite,
        boolean directIO) throws IOException {
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
        recoverAndReset(wal);

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
                        Assertions.fail();
                    }
                });
            }
        } finally {
            wal.shutdownGracefully();
        }
        assertTrue(maxFlushedOffset.get() > maxRecordOffset.get(),
            "maxFlushedOffset should be greater than maxRecordOffset. maxFlushedOffset: " + maxFlushedOffset.get() + ", maxRecordOffset: " + maxRecordOffset.get());
    }

    private static void testMultiThreadAppend0(boolean mergeWrite,
        boolean directIO) throws IOException, InterruptedException {
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
        recoverAndReset(wal);

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
                                Assertions.fail();
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

    private static void testRecoverAfterMergeWrite0(boolean shutdown, boolean overCapacity,
        boolean directIO) throws IOException {
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
            .build()
            .start();
        recoverAndReset(previousWAL);
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
            Iterator<RecoverResult> recover = recover(wal);
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
                    Assertions.fail();
                }
            }).thenApply(ignored -> null));
        }
        CompletableFuture.allOf(appendFutures.toArray(new CompletableFuture[0])).join();
        return appended;
    }

    public static Stream<Arguments> testRecoverFromDisasterV0Data() {
        return Stream.of(
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                -1L,
                50L,
                Arrays.asList(0L, 2L, 4L),
                Arrays.asList(0L, 2L, 4L),
                WALUtil.BLOCK_SIZE
            ).toArguments("base"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                0L,
                50L,
                Arrays.asList(0L, 2L, 4L),
                Arrays.asList(2L, 4L),
                WALUtil.BLOCK_SIZE
            ).toArguments("trimmed at zero"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                2L,
                50L,
                Arrays.asList(0L, 2L, 4L, 6L),
                Arrays.asList(4L, 6L),
                WALUtil.BLOCK_SIZE
            ).toArguments("trimmed"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                2L,
                50L,
                Arrays.asList(0L, 2L, 4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L),
                Arrays.asList(4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L),
                WALUtil.BLOCK_SIZE
            ).toArguments("WAL header flushed slow"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                2L,
                50L,
                Arrays.asList(0L, 2L, 8L, 10L, 14L, 20L),
                Arrays.asList(8L, 10L, 14L, 20L),
                WALUtil.BLOCK_SIZE
            ).toArguments("many invalid records"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                2L,
                50L,
                Arrays.asList(14L, 8L, 10L, 20L, 0L, 2L),
                Arrays.asList(8L, 10L, 14L, 20L),
                WALUtil.BLOCK_SIZE
            ).toArguments("write in random order"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                20230920L,
                50L,
                Arrays.asList(20230900L, 20230910L, 20230916L, 20230920L, 20230930L, 20230940L, 20230950L, 20230960L, 20230970L, 20230980L),
                Arrays.asList(20230930L, 20230940L, 20230950L, 20230960L, 20230970L),
                WALUtil.BLOCK_SIZE
            ).toArguments("big logic offset"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                180L,
                30L,
                Arrays.asList(150L, 160L, 170L, 180L, 190L, 200L, 202L, 210L, 220L, 230L, 240L),
                Arrays.asList(190L, 200L, 202L, 210L, 220L, 230L),
                WALUtil.BLOCK_SIZE
            ).toArguments("round robin"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE * 2 + 1,
                100L,
                192L,
                3L,
                Arrays.asList(192L, 195L, /* no place for 198L, */ 200L, 203L, 206L, 209L, 212L, 215L),
                Arrays.asList(195L, 200L, 203L, 206L, 209L, 212L, 215L),
                WALUtil.BLOCK_SIZE
            ).toArguments("round robin - no place for the last record"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                210L,
                50L,
                Arrays.asList(111L, 113L, 115L, 117L, 119L, 120L, 130L,
                    210L, 215L, 220L, 230L, 240L, 250L, 260L, 270L, 280L, 290L),
                Arrays.asList(215L, 220L, 230L, 240L, 250L, 260L),
                WALUtil.BLOCK_SIZE
            ).toArguments("overwrite"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                -1L,
                1L,
                Arrays.asList(0L, 2L, 5L, 7L),
                List.of(0L, 2L),
                WALUtil.BLOCK_SIZE
            ).toArguments("small window - record size not aligned"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                10L,
                3L,
                Arrays.asList(10L, 12L, 15L, 17L, 19L),
                List.of(12L, 15L),
                WALUtil.BLOCK_SIZE
            ).toArguments("invalid record in window - record size not aligned"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE + 1,
                100L,
                10L,
                9L,
                Arrays.asList(9L, 14L, 18L, 20L),
                List.of(14L, 18L),
                WALUtil.BLOCK_SIZE
            ).toArguments("trim at an invalid record - record size not aligned"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE,
                100L,
                -1L,
                1L,
                Arrays.asList(0L, 1L, 3L, 4L, 5L),
                List.of(0L, 1L),
                WALUtil.BLOCK_SIZE
            ).toArguments("small window - record size aligned"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE,
                100L,
                10L,
                3L,
                Arrays.asList(10L, 11L, 13L, 14L, 15L, 16L),
                List.of(11L, 13L, 14L),
                WALUtil.BLOCK_SIZE
            ).toArguments("invalid record in window - record size aligned"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE,
                100L,
                10L,
                5L,
                Arrays.asList(9L, 11L, 13L, 15L, 16L, 17L),
                List.of(11L, 13L, 15L, 16L),
                WALUtil.BLOCK_SIZE
            ).toArguments("trim at an invalid record - record size aligned"),
            new RecoverFromDisasterV0Param(
                WALUtil.BLOCK_SIZE,
                100L,
                10L,
                0L,
                Arrays.asList(10L, 11L, 12L, 14L),
                List.of(11L, 12L),
                WALUtil.BLOCK_SIZE
            ).toArguments("zero window"),
            new RecoverFromDisasterV0Param(
                42,
                8192L,
                -1L,
                8192L,
                Arrays.asList(0L, 42L, 84L),
                Arrays.asList(0L, 42L, 84L),
                1
            ).toArguments("merge write - base"),
            new RecoverFromDisasterV0Param(
                42,
                8192L,
                42L,
                8192L,
                Arrays.asList(0L, 42L, 84L, 126L),
                Arrays.asList(84L, 126L),
                1
            ).toArguments("merge write - trimmed"),
            new RecoverFromDisasterV0Param(
                42,
                8192L,
                42L,
                8192L,
                Arrays.asList(0L, 42L, 42 * 2L, 42 * 4L, 4096L, 4096L + 42L, 4096L + 42 * 3L),
                Arrays.asList(42 * 2L, 4096L, 4096L + 42L),
                1
            ).toArguments("merge write - some invalid records"),
            new RecoverFromDisasterV0Param(
                42,
                8192L,
                42L,
                8192L,
                Arrays.asList(42L, 42 * 4L, 42 * 2L, 4096L + 42 * 3L, 0L, 4096L, 4096L + 42L),
                Arrays.asList(42 * 2L, 4096L, 4096L + 42L),
                1
            ).toArguments("merge write - random order"),
            new RecoverFromDisasterV0Param(
                1000,
                8192L,
                2000L,
                8192L,
                Arrays.asList(0L, 1000L, 2000L, 3000L, 4000L, 5000L, 7000L),
                Arrays.asList(3000L, 4000L, 5000L),
                1
            ).toArguments("merge write - record in the middle"),
            new RecoverFromDisasterV0Param(
                42,
                8192L,
                8192L + 4096L + 42L,
                8192L,
                Arrays.asList(8192L + 4096L, 8192L + 4096L + 42L, 8192L + 4096L + 42 * 2L, 8192L + 4096L + 42 * 4L, 16384L, 16384L + 42L, 16384L + 42 * 3L),
                Arrays.asList(8192L + 4096L + 42 * 2L, 16384L, 16384L + 42L),
                1
            ).toArguments("merge write - round robin"),
            new RecoverFromDisasterV0Param(
                1000,
                8192L,
                12000L,
                8192L,
                Arrays.asList(1000L, 2000L, 3000L, 4000L, 5000L, 6000L, 7000L,
                    9000L, 10000L, 11000L, 12000L, 13000L, 14000L, 15000L),
                Arrays.asList(13000L, 14000L, 15000L),
                1
            ).toArguments("merge write - overwrite"),
            new RecoverFromDisasterV0Param(
                42,
                4096L * 20,
                -1L,
                4096L,
                Arrays.asList(0L, 42L, 42 * 3L, 4096L, 4096L + 42L, 4096L + 42 * 3L, 12288L, 12288L + 42L, 12288L + 42 * 3L, 16384L),
                Arrays.asList(0L, 42L, 4096L, 4096L + 42L),
                1
            ).toArguments("merge write - small window"),
            new RecoverFromDisasterV0Param(
                42,
                4096L * 20,
                4096L * 2,
                4096L * 4,
                Arrays.asList(4096L * 2, 4096L * 2 + 42L, 4096L * 2 + 42 * 3L,
                    4096L * 4, 4096L * 4 + 42L, 4096L * 4 + 42 * 3L,
                    4096L * 5, 4096L * 5 + 42L, 4096L * 5 + 42 * 3L,
                    4096L * 6, 4096L * 6 + 42L, 4096L * 6 + 42 * 3L,
                    4096L * 7, 4096L * 7 + 42L, 4096L * 7 + 42 * 3L,
                    4096L * 8),
                Arrays.asList(4096L * 2 + 42L, 4096L * 4, 4096L * 4 + 42L,
                    4096L * 5, 4096L * 5 + 42L, 4096L * 6, 4096L * 6 + 42L),
                1
            ).toArguments("merge write - invalid record in window"),
            new RecoverFromDisasterV0Param(
                42,
                4096L * 20,
                4096L * 2 + 42 * 2L,
                4096L * 2,
                Arrays.asList(4096L * 2, 4096L * 2 + 42L, 4096L * 2 + 42 * 3L,
                    4096L * 3, 4096L * 3 + 42L, 4096L * 3 + 42 * 3L,
                    4096L * 5, 4096L * 5 + 42L, 4096L * 5 + 42 * 3L,
                    4096L * 6, 4096L * 6 + 42L, 4096L * 6 + 42 * 3L,
                    4096L * 7),
                Arrays.asList(4096L * 3, 4096L * 3 + 42L, 4096L * 5, 4096L * 5 + 42L),
                1
            ).toArguments("merge write - trim at an invalid record"),
            new RecoverFromDisasterV0Param(
                42,
                4096L * 20,
                4096L * 2,
                0L,
                Arrays.asList(4096L * 2, 4096L * 2 + 42L, 4096L * 2 + 42 * 3L,
                    4096L * 3, 4096L * 3 + 42L, 4096L * 3 + 42 * 3L,
                    4096L * 5, 4096L * 5 + 42L, 4096L * 5 + 42 * 3L,
                    4096L * 6),
                Arrays.asList(4096L * 2 + 42L, 4096L * 3, 4096L * 3 + 42L),
                1
            ).toArguments("merge write - zero window")
        );
    }

    /**
     * Call {@link WriteAheadLog#recover()} and set to strict mode.
     */
    private static Iterator<RecoverResult> recover(WriteAheadLog wal) {
        Iterator<RecoverResult> iterator = wal.recover();
        assertNotNull(iterator);
        if (iterator instanceof RecoverIteratorV0) {
            ((RecoverIteratorV0) iterator).strictMode();
        }
        return iterator;
    }

    /**
     * Call {@link WriteAheadLog#recover()} {@link WriteAheadLog#reset()} and drop all records.
     */
    private static void recoverAndReset(WriteAheadLog wal) {
        for (Iterator<RecoverResult> it = recover(wal); it.hasNext(); ) {
            it.next().record().release();
        }
        wal.reset().join();
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

    private List<Long> appendWithoutTrim(WriteAheadLog wal, int recordSize,
        int recordCount) throws OverCapacityException {
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
    public void testSingleThreadRecoverDirectIO(boolean shutdown, boolean overCapacity,
        int recordCount) throws IOException {
        testSingleThreadRecover0(shutdown, overCapacity, recordCount, true);
    }

    private void testSingleThreadRecover0(boolean shutdown, boolean overCapacity, int recordCount,
        boolean directIO) throws IOException {
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
            .build()
            .start();
        recoverAndReset(previousWAL);
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
            Iterator<RecoverResult> recover = recover(wal);
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
            .build()
            .start();
        recoverAndReset(previousWAL);
        // Append 2 records
        long appended0 = append(previousWAL, recordSize);
        long appended1 = append(previousWAL, recordSize);

        final WriteAheadLog wal = BlockWALService.builder(path, 1 << 20)
            .direct(directIO)
            .build()
            .start();
        try {
            // Recover records
            Iterator<RecoverResult> recover = recover(wal);
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
        return new RecordHeader(offset, body.readableBytes(), WALUtil.crc32(body))
            .marshal(ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE));
    }

    private void write(WALChannel walChannel, long logicOffset, int recordSize) throws IOException {
        ByteBuf recordBody = TestUtils.random(recordSize - RECORD_HEADER_SIZE);
        ByteBuf recordHeader = recordHeader(recordBody, logicOffset);

        CompositeByteBuf record = ByteBufAlloc.compositeByteBuffer();
        record.addComponents(true, recordHeader, recordBody);

        long position = WALUtil.recordOffsetToPosition(logicOffset, walChannel.capacity(), WAL_HEADER_TOTAL_CAPACITY);
        writeAndFlush(walChannel, record, position);
    }

    private void writePadding(WALChannel walChannel, long logicOffset, int recordSize) throws IOException {
        ByteBuf header = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        Record paddingRecord = WALUtil.generatePaddingRecord(header, logicOffset, recordSize);

        CompositeByteBuf record = ByteBufAlloc.compositeByteBuffer();
        record.addComponents(true, paddingRecord.header(), paddingRecord.body());

        long position = WALUtil.recordOffsetToPosition(logicOffset, walChannel.capacity(), WAL_HEADER_TOTAL_CAPACITY);
        writeAndFlush(walChannel, record, position);
    }

    private void writeWALHeader(WALChannel walChannel, int version, long trimOffset, long maxLength) throws IOException {
        ByteBuf header = new BlockWALHeader(walChannel.capacity(), maxLength)
            .updateVersion(version)
            .updateTrimOffset(trimOffset)
            .marshal();
        writeAndFlush(walChannel, header, 0);
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverFromDisasterV0Data")
    public void testRecoverFromDisasterV0(
        String name,
        int recordSize,
        long capacity,
        long trimOffset,
        long maxLength,
        List<Long> writeOffsets,
        List<Long> recoveredOffsets
    ) throws IOException {
        testRecoverFromDisasterV00(name, recordSize, capacity, trimOffset, maxLength, writeOffsets, recoveredOffsets, false);
    }

    @ParameterizedTest(name = "Test {index} {0}")
    @MethodSource("testRecoverFromDisasterV0Data")
    @EnabledOnOs({OS.LINUX})
    public void testRecoverFromDisasterV0DirectIO(
        String name,
        int recordSize,
        long capacity,
        long trimOffset,
        long maxLength,
        List<Long> writeOffsets,
        List<Long> recoveredOffsets
    ) throws IOException {
        testRecoverFromDisasterV00(name, recordSize, capacity, trimOffset, maxLength, writeOffsets, recoveredOffsets, true);
    }

    private void testRecoverFromDisasterV00(
        String name,
        int recordSize,
        long capacity,
        long trimOffset,
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

        WALChannel walChannel;
        if (directIO) {
            WALBlockDeviceChannel blockDeviceChannel = new WALBlockDeviceChannel(path, capacity);
            blockDeviceChannel.unalignedWrite = true;
            walChannel = blockDeviceChannel;
        } else {
            walChannel = WALChannel.builder(path)
                .capacity(capacity)
                .direct(false)
                .build();
        }

        // Simulate disaster
        walChannel.open();
        writeWALHeader(walChannel, 0, trimOffset, maxLength);
        for (long writeOffset : writeOffsets) {
            write(walChannel, writeOffset, recordSize);
        }
        walChannel.close();

        final WriteAheadLog wal = BlockWALService.builder(path, capacity)
            .direct(directIO)
            .build()
            .start();
        try {
            // Recover records
            Iterator<RecoverResult> recover = recover(wal);
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
            .build()
            .start();
        recoverAndReset(wal1);
        List<Long> appended1 = appendWithoutTrim(wal1, recordSize, recordCount);

        // 2. recover and reset
        final WriteAheadLog wal2 = BlockWALService.builder(path, blockDeviceCapacity)
            .direct(directIO)
            .build()
            .start();
        Iterator<RecoverResult> recover = recover(wal2);
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
            .build()
            .start();
        recover = recover(wal3);
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
        recoverAndReset(wal);
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
        recoverAndReset(wal);
        try {
            append(wal, 42);
            Assertions.assertThrows(OverCapacityException.class, () -> append(wal, 42));
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testRecoveryMode() throws IOException, OverCapacityException {
        testRecoveryMode0(false);
    }

    @Test
    @EnabledOnOs({OS.LINUX})
    public void testRecoveryModeDirectIO() throws IOException, OverCapacityException {
        testRecoveryMode0(true);
    }

    private void testRecoveryMode0(boolean directIO) throws IOException, OverCapacityException {
        final long capacity = 1 << 20;
        final int nodeId = 10;
        final long epoch = 100;
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, capacity);
        }

        // simulate a crash
        WriteAheadLog wal1 = BlockWALService.builder(path, capacity)
            .nodeId(nodeId)
            .epoch(epoch)
            .direct(directIO)
            .build()
            .start();
        recoverAndReset(wal1);
        wal1.append(TestUtils.random(4097)).future().join();

        // open in recovery mode
        WriteAheadLog wal2 = BlockWALService.recoveryBuilder(path)
            .direct(directIO)
            .build()
            .start();
        assertEquals(nodeId, wal2.metadata().nodeId());
        assertEquals(epoch, wal2.metadata().epoch());
        // we can recover and reset the WAL
        recoverAndReset(wal2);
        // but we can't append to or trim it
        assertThrows(IllegalStateException.class, () -> wal2.append(TestUtils.random(4097)).future().join());
        assertThrows(IllegalStateException.class, () -> wal2.trim(0).join());
    }

    @Test
    public void testCapacityMismatchFileSize() throws IOException {
        testCapacityMismatchFileSize0(false);
    }

    @Test
    @EnabledOnOs({OS.LINUX})
    public void testCapacityMismatchFileSizeDirectIO() throws IOException {
        testCapacityMismatchFileSize0(true);
    }

    private void testCapacityMismatchFileSize0(boolean directIO) throws IOException {
        final long capacity1 = 1 << 20;
        final long capacity2 = 1 << 21;
        final String path = TestUtils.tempFilePath();

        // init a WAL with capacity1
        WriteAheadLog wal1 = BlockWALService.builder(path, capacity1)
            .direct(directIO)
            .build()
            .start();
        recoverAndReset(wal1);
        wal1.shutdownGracefully();

        // try to open it with capacity2
        WriteAheadLog wal2 = BlockWALService.builder(path, capacity2)
            .direct(directIO)
            .build();
        assertThrows(WALCapacityMismatchException.class, wal2::start);
    }

    @Test
    public void testCapacityMismatchInHeader() throws IOException {
        testCapacityMismatchInHeader0(false);
    }

    @Test
    @EnabledOnOs({OS.LINUX})
    public void testCapacityMismatchInHeaderDirectIO() throws IOException {
        testCapacityMismatchInHeader0(true);
    }

    private void testCapacityMismatchInHeader0(boolean directIO) throws IOException {
        final long capacity1 = 1 << 20;
        final long capacity2 = 1 << 21;
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, capacity1);
        }

        // init a WAL with capacity1
        WriteAheadLog wal1 = BlockWALService.builder(path, capacity1)
            .direct(directIO)
            .build()
            .start();
        recoverAndReset(wal1);
        wal1.shutdownGracefully();

        // overwrite the capacity in the header with capacity2
        WALChannel walChannel = WALChannel.builder(path)
            .capacity(capacity1)
            .direct(directIO)
            .build();
        walChannel.open();
        writeAndFlush(walChannel, new BlockWALHeader(capacity2, 42).marshal(), 0);
        walChannel.close();

        // try to open it with capacity1
        WriteAheadLog wal2 = BlockWALService.builder(path, capacity1)
            .direct(directIO)
            .build();
        assertThrows(WALCapacityMismatchException.class, wal2::start);
    }

    @Test
    public void testRecoveryModeWALFileNotExist() throws IOException {
        testRecoveryModeWALFileNotExist0(false);
    }

    @Test
    @EnabledOnOs({OS.LINUX})
    public void testRecoveryModeWALFileNotExistDirectIO() throws IOException {
        testRecoveryModeWALFileNotExist0(true);
    }

    private void testRecoveryModeWALFileNotExist0(boolean directIO) throws IOException {
        final String path = TestUtils.tempFilePath();

        WriteAheadLog wal = BlockWALService.recoveryBuilder(path)
            .direct(directIO)
            .build();
        assertThrows(WALNotInitializedException.class, wal::start);
    }

    @Test
    public void testRecoveryModeNoHeader() throws IOException {
        testRecoveryModeNoHeader0(false);
    }

    @Test
    @EnabledOnOs({OS.LINUX})
    public void testRecoveryModeNoHeaderDirectIO() throws IOException {
        testRecoveryModeNoHeader0(true);
    }

    private void testRecoveryModeNoHeader0(boolean directIO) throws IOException {
        final long capacity = 1 << 20;
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            resetBlockDevice(path, capacity);
        }

        // init a WAL
        WriteAheadLog wal1 = BlockWALService.builder(path, capacity)
            .direct(directIO)
            .build()
            .start();
        recoverAndReset(wal1);
        wal1.shutdownGracefully();

        // clear the WAL header
        WALChannel walChannel = WALChannel.builder(path)
            .capacity(capacity)
            .direct(directIO)
            .build();
        walChannel.open();
        writeAndFlush(walChannel, Unpooled.buffer(WAL_HEADER_TOTAL_CAPACITY).writeZero(WAL_HEADER_TOTAL_CAPACITY), 0);
        walChannel.close();

        // try to open it in recovery mode
        WriteAheadLog wal2 = BlockWALService.recoveryBuilder(path)
            .direct(directIO)
            .build();
        assertThrows(WALNotInitializedException.class, wal2::start);
    }

    @ParameterizedTest(name = "Test {index}: recordCount={0}")
    @ValueSource(ints = {10, 20, 30, 40})
    public void testResetWithoutRecover(int recordCount) throws IOException {
        testResetWithoutRecover0(recordCount, false);
    }

    @ParameterizedTest(name = "Test {index}: recordCount={0}")
    @ValueSource(ints = {10, 20, 30, 40})
    @EnabledOnOs({OS.LINUX})
    public void testResetWithoutRecoverDirectIO(int recordCount) throws IOException {
        testResetWithoutRecover0(recordCount, true);
    }

    private void testResetWithoutRecover0(int recordCount, boolean directIO) throws IOException {
        final int recordSize = 4096 + 1;
        long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordCount / 3 + WAL_HEADER_TOTAL_CAPACITY;
        String path = TestUtils.tempFilePath();

        if (directIO && TEST_BLOCK_DEVICE != null) {
            path = TEST_BLOCK_DEVICE;
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(blockDeviceCapacity);
            resetBlockDevice(path, blockDeviceCapacity);
        }

        // Append records
        final WriteAheadLog wal1 = BlockWALService.builder(path, blockDeviceCapacity)
            .direct(directIO)
            .build()
            .start();
        recoverAndReset(wal1);
        wal1.shutdownGracefully();

        // Reset WAL without recover
        final WriteAheadLog wal2 = BlockWALService.builder(path, blockDeviceCapacity)
            .direct(directIO)
            .build()
            .start();
        try {
            wal2.reset().join();
        } finally {
            wal2.shutdownGracefully();
        }

        // Try to recover records
        final WriteAheadLog wal3 = BlockWALService.builder(path, blockDeviceCapacity)
            .direct(directIO)
            .build()
            .start();
        try {
            Iterator<RecoverResult> recover = recover(wal3);
            assertFalse(recover.hasNext());
        } finally {
            wal3.shutdownGracefully();
        }
    }

    private void writeAndFlush(WALChannel channel, ByteBuf src, long position) throws IOException {
        channel.write(src, position);
        channel.flush();
    }

    private static class RecoverFromDisasterV0Param {
        int recordSize;
        long capacity;
        // WAL header
        long trimOffset;
        long maxLength;
        // WAL records
        List<Long> writeOffsets;
        List<Long> recoveredOffsets;

        public RecoverFromDisasterV0Param(
            int recordSize,
            long capacity,
            long trimOffset,
            long maxLength,
            List<Long> writeOffsets,
            List<Long> recoveredOffsets,
            int unit
        ) {
            this.recordSize = recordSize;
            this.capacity = capacity * unit + WAL_HEADER_TOTAL_CAPACITY;
            this.trimOffset = trimOffset * unit;
            this.maxLength = maxLength * unit;
            this.writeOffsets = writeOffsets.stream().map(offset -> offset * unit).collect(Collectors.toList());
            this.recoveredOffsets = recoveredOffsets.stream().map(offset -> offset * unit).collect(Collectors.toList());
        }

        public Arguments toArguments(String name) {
            return Arguments.of(name, recordSize, capacity, trimOffset, maxLength, writeOffsets, recoveredOffsets);
        }
    }

    @Test
    public void testRecoverFromDisasterV1() throws IOException {
        String path = TestUtils.tempFilePath();
        WALChannel walChannel = WALChannel.builder(path)
            .capacity(WAL_HEADER_TOTAL_CAPACITY + WALUtil.BLOCK_SIZE * 100L)
            .direct(false)
            .build();

        // Simulate disaster
        walChannel.open();
        writeWALHeader(walChannel, 1, WALUtil.BLOCK_SIZE * 190L, WALUtil.BLOCK_SIZE * 50L);
        // Write records
        // trimmed records
        write(walChannel, WALUtil.BLOCK_SIZE * 190L, WALUtil.BLOCK_SIZE + 1);
        // valid records
        write(walChannel, WALUtil.BLOCK_SIZE * 192L, WALUtil.BLOCK_SIZE + 1);
        write(walChannel, WALUtil.BLOCK_SIZE * 194L, WALUtil.BLOCK_SIZE + 1);
        // padding records
        writePadding(walChannel, WALUtil.BLOCK_SIZE * 196L, WALUtil.BLOCK_SIZE * 4);
        // valid records
        write(walChannel, WALUtil.BLOCK_SIZE * 200L, WALUtil.BLOCK_SIZE + 1);
        write(walChannel, WALUtil.BLOCK_SIZE * 202L, WALUtil.BLOCK_SIZE + 1);
        // invalid records
        write(walChannel, WALUtil.BLOCK_SIZE * 210L, WALUtil.BLOCK_SIZE + 1);
        write(walChannel, WALUtil.BLOCK_SIZE * 212L, WALUtil.BLOCK_SIZE + 1);
        walChannel.close();

        // Recover records
        final WriteAheadLog wal = BlockWALService.builder(path, walChannel.capacity())
            .direct(false)
            .build()
            .start();
        try {
            Iterator<RecoverResult> recover = recover(wal);
            List<Long> recovered = new ArrayList<>();
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                next.record().release();
                recovered.add(next.recordOffset());
            }
            assertEquals(Arrays.asList(
                WALUtil.BLOCK_SIZE * 192L,
                WALUtil.BLOCK_SIZE * 194L,
                WALUtil.BLOCK_SIZE * 200L,
                WALUtil.BLOCK_SIZE * 202L
            ), recovered);
            wal.reset().join();
        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    public void testUpgradeFromV0ToV1() throws IOException {
        String path = TestUtils.tempFilePath();
        WALChannel walChannel = WALChannel.builder(path)
            .capacity(WAL_HEADER_TOTAL_CAPACITY + WALUtil.BLOCK_SIZE * 100L)
            .direct(false)
            .build();

        walChannel.open();
        writeWALHeader(walChannel, 0, 0, 0);
        walChannel.close();

        final BlockWALService wal = (BlockWALService) BlockWALService.builder(path, walChannel.capacity())
            .direct(false)
            .build()
            .start();

        try {
            assertEquals(0, wal.header().version());

            for (Iterator<RecoverResult> it = recover(wal); it.hasNext(); ) {
                it.next().record().release();
            }
            assertEquals(0, wal.header().version());

            wal.reset().join();
            assertEquals(1, wal.header().version());
        } finally {
            wal.shutdownGracefully();
        }
    }
}
