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
import io.netty.buffer.Unpooled;
import kafka.log.s3.TestUtils;
import kafka.log.s3.wal.util.WALUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static kafka.log.s3.wal.BlockWALService.WAL_HEADER_CAPACITY_DOUBLE;
import static kafka.log.s3.wal.BlockWALService.WAL_HEADER_INIT_WINDOW_MAX_LENGTH;
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

    private static final int BLOCK_DEVICE_CAPACITY = 4096 * 4096;

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
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums + WAL_HEADER_CAPACITY_DOUBLE;

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(blockDeviceCapacity)
                .blockDevicePath(TestUtils.tempFilePath())
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
        try {
            for (int i = 0; i < recordNums; i++) {
                ByteBuf data = TestUtils.random(recordSize);

                final AppendResult appendResult = wal.append(data);

                final long expectedOffset = i * WALUtil.alignLargeByBlockSize(recordSize);
                assertEquals(expectedOffset, appendResult.recordOffset());
                appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                    assertNull(throwable);
                    assertTrue(callbackResult.flushedOffset() > expectedOffset);
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
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums / 3 + WAL_HEADER_CAPACITY_DOUBLE;

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(blockDeviceCapacity)
                .blockDevicePath(TestUtils.tempFilePath())
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
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
                        wal.trim(appendedOffset.get());
                        continue;
                    }
                    break;
                }

                final long recordOffset = appendResult.recordOffset();
                appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                    assertNull(throwable);
                    assertTrue(callbackResult.flushedOffset() > recordOffset);
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

    void appendManyRecord(int recordTotal, String blockDeviceName) throws IOException {
        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(blockDeviceName)
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
        try {
            // append 1024
            for (int i = 0; i < recordTotal; i++) {
                try {
                    String format = String.format("Hello World [%d]", i);
                    final AppendResult appendResult = wal.append(Unpooled.wrappedBuffer(ByteBuffer.wrap(format.getBytes())));
                    final int index = i;
                    System.out.printf("[APPEND AFTER %d] %s\n", index, appendResult);
                    appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {

                        System.out.printf("[APPEND CALLBACK %d] %s | %s \n", index, appendResult, callbackResult);

                        if (throwable != null) {
                            throwable.printStackTrace();
                        }
                    });
                } catch (OverCapacityException e) {
                    e.printStackTrace(System.err);
                    i--;
                }
            }
        } finally {
            wal.shutdownGracefully();
        }
    }

    long append(WriteAheadLog wal, int recordSize) throws OverCapacityException {
        final AppendResult appendResult = wal.append(TestUtils.random(recordSize));
        final long recordOffset = appendResult.recordOffset();
        appendResult.future().whenComplete((callbackResult, throwable) -> {
            assertNull(throwable);
            assertTrue(callbackResult.flushedOffset() > recordOffset);
            assertEquals(0, callbackResult.flushedOffset() % WALUtil.BLOCK_SIZE);
        });
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
                wal.trim(offset);
                i--;
            }
        }
        return recordOffsets;
    }

    @Test
    void testSingleThreadRecover() throws IOException {
        final int recordSize = 4096 + 1;
        final int recordNums = 10;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums + WAL_HEADER_CAPACITY_DOUBLE;
        final String tempFilePath = TestUtils.tempFilePath();

        final WriteAheadLog previousWAL = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(blockDeviceCapacity)
                .blockDevicePath(tempFilePath)
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
        List<Long> appended = append(previousWAL, recordSize, recordNums);
        previousWAL.shutdownGracefully();

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(tempFilePath)
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();

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

    @Test
    void testMultiThreadAppend() throws InterruptedException, IOException {
        final int recordSize = 4096 + 1;
        final int recordNums = 10;
        final int nThreadNums = 8;
        final long blockDeviceCapacity = WALUtil.alignLargeByBlockSize(recordSize) * recordNums * nThreadNums + WAL_HEADER_CAPACITY_DOUBLE;

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(blockDeviceCapacity)
                .blockDevicePath(TestUtils.tempFilePath())
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
        ExecutorService executorService = Executors.newFixedThreadPool(nThreadNums);
        try {
            for (int t = 0; t < nThreadNums; t++) {
                executorService.submit(() -> assertDoesNotThrow(() -> {
                    for (int i = 0; i < recordNums; i++) {
                        ByteBuf data = TestUtils.random(recordSize);

                        final AppendResult appendResult = wal.append(data);

                        appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                            assertNull(throwable);
                            assertTrue(callbackResult.flushedOffset() > appendResult.recordOffset());
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
}
