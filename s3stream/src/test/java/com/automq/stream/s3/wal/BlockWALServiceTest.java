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
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.automq.stream.s3.wal.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;
import static com.automq.stream.s3.wal.WriteAheadLog.OverCapacityException;
import static com.automq.stream.s3.wal.WriteAheadLog.RecoverResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class BlockWALServiceTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testSingleThreadAppendBasic() throws IOException, OverCapacityException {
        final int recordSize = 4096 + 1;
        final int recordNums = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), blockDeviceCapacity)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
                .build()
                .start();
        try {
            for (int i = 0; i < recordNums; i++) {
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
    void testSingleThreadAppendWhenOverCapacity() throws IOException, InterruptedException {
        final int recordSize = 4096 + 1;
        final int recordNums = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums / 3 + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), blockDeviceCapacity)
                .slidingWindowInitialSize(0)
                .slidingWindowScaleUnit(4096)
                .build()
                .start();
        try {
            AtomicLong appendedOffset = new AtomicLong(-1);
            for (int i = 0; i < recordNums; i++) {
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
    void testMultiThreadAppend() throws InterruptedException, IOException {
        final int recordSize = 4096 + 1;
        final int recordNums = 10;
        final int nThreadNums = 8;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums * nThreadNums + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath(), blockDeviceCapacity)
                .build()
                .start();
        ExecutorService executorService = Executors.newFixedThreadPool(nThreadNums);
        try {
            for (int t = 0; t < nThreadNums; t++) {
                executorService.submit(() -> Assertions.assertDoesNotThrow(() -> {
                    for (int i = 0; i < recordNums; i++) {
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

    long append(WriteAheadLog wal, int recordSize) throws OverCapacityException {
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

    List<Long> append(WriteAheadLog wal, int recordSize, int recordNums) {
        List<Long> recordOffsets = new ArrayList<>(recordNums);
        long offset = 0;
        for (int i = 0; i < recordNums; i++) {
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

    @ParameterizedTest(name = "Test {index}: shutdown={0}, overCapacity={1}, recordNums={2}")
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
    void testSingleThreadRecover(boolean shutdown, boolean overCapacity, int recordNums) throws IOException {
        final int recordSize = 4096 + 1;
        long blockDeviceCapacity;
        if (overCapacity) {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums / 3 + WAL_HEADER_TOTAL_CAPACITY;
        } else {
            blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums + WAL_HEADER_TOTAL_CAPACITY;
        }
        final String tempFilePath = TestUtils.tempFilePath();

        // Append records
        final WriteAheadLog previousWAL = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .flushHeaderIntervalSeconds(1 << 20)
                .build()
                .start();
        List<Long> appended = append(previousWAL, recordSize, recordNums);
        if (shutdown) {
            previousWAL.shutdownGracefully();
        }

        // Recover records
        final WriteAheadLog wal = BlockWALService.builder(tempFilePath, blockDeviceCapacity)
                .build()
                .start();
        try {
            Iterator<RecoverResult> recover = wal.recover();
            Assertions.assertNotNull(recover);

            List<Long> recovered = new ArrayList<>(recordNums);
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
    void testAppendAfterRecover() throws IOException, OverCapacityException {
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
            Assertions.assertNotNull(recover);

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

    @Test
    void testTrimInvalidOffset() throws IOException, OverCapacityException {
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
    void testWindowGreaterThanCapacity() throws IOException, OverCapacityException {
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
