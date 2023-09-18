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

package kafka.log.s3.wal;

import io.netty.buffer.ByteBuf;
import kafka.log.s3.TestUtils;
import kafka.log.s3.wal.util.WALUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static kafka.log.s3.wal.BlockWALService.WAL_HEADER_INIT_WINDOW_MAX_LENGTH;
import static kafka.log.s3.wal.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
import static kafka.log.s3.wal.WriteAheadLog.AppendResult;
import static kafka.log.s3.wal.WriteAheadLog.OverCapacityException;
import static kafka.log.s3.wal.WriteAheadLog.RecoverResult;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class BlockWALServiceTest {

    private static final int IO_THREAD_NUMS = 4;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testSingleThreadAppendBasic() throws IOException, OverCapacityException {
        // Set to WAL_HEADER_INIT_WINDOW_MAX_LENGTH to trigger window scale
        final int recordSize = (int) WAL_HEADER_INIT_WINDOW_MAX_LENGTH + 1;
        final int recordNums = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath())
                .capacity(blockDeviceCapacity)
                .ioThreadNums(IO_THREAD_NUMS)
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
    void testSingleThreadAppendWhenOverCapacity() throws IOException {
        final int recordSize = 4096 + 1;
        final int recordNums = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums / 3 + WAL_HEADER_TOTAL_CAPACITY;

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath())
                .capacity(blockDeviceCapacity)
                .ioThreadNums(IO_THREAD_NUMS)
                .build()
                .start();
        try {
            AtomicLong appendedOffset = new AtomicLong(0);
            for (int i = 0; i < recordNums; i++) {
                ByteBuf data = TestUtils.random(recordSize);
                AppendResult appendResult;

                while (true) {
                    try {
                        appendResult = wal.append(data);
                    } catch (OverCapacityException e) {
                        Thread.yield();
                        wal.trim(appendedOffset.get()).join();
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

        final WriteAheadLog wal = BlockWALService.builder(TestUtils.tempFilePath())
                .capacity(blockDeviceCapacity)
                .ioThreadNums(IO_THREAD_NUMS)
                .build()
                .start();
        ExecutorService executorService = Executors.newFixedThreadPool(nThreadNums);
        try {
            for (int t = 0; t < nThreadNums; t++) {
                executorService.submit(() -> assertDoesNotThrow(() -> {
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
        final WriteAheadLog previousWAL = BlockWALService.builder(tempFilePath)
                .capacity(blockDeviceCapacity)
                .ioThreadNums(IO_THREAD_NUMS)
                .build()
                .start();
        List<Long> appended = append(previousWAL, recordSize, recordNums);
        if (shutdown) {
            previousWAL.shutdownGracefully();
        }

        // Recover records
        final WriteAheadLog wal = BlockWALService.builder(tempFilePath)
                .capacity(blockDeviceCapacity)
                .ioThreadNums(IO_THREAD_NUMS)
                .build()
                .start();
        try {
            Iterator<RecoverResult> recover = wal.recover();
            assertNotNull(recover);

            List<Long> recovered = new ArrayList<>(recordNums);
            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                recovered.add(next.recordOffset());
            }
            assertEquals(appended, recovered);
        } finally {
            wal.shutdownGracefully();
        }
    }
}
