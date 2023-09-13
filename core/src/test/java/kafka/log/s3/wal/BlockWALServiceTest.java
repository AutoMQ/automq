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
import kafka.log.s3.wal.util.WALUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static kafka.log.s3.wal.BlockWALService.WAL_HEADER_CAPACITY_DOUBLE;
import static kafka.log.s3.wal.WriteAheadLog.AppendResult;
import static kafka.log.s3.wal.WriteAheadLog.OverCapacityException;
import static kafka.log.s3.wal.WriteAheadLog.RecoverResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("S3Unit")
class BlockWALServiceTest {

    private static final String BLOCK_DEVICE_PATH = String.format("%s/%d-", System.getProperty("java.io.tmpdir"), System.currentTimeMillis());

    private static final int BLOCK_DEVICE_CAPACITY = 4096 * 4096;

    private static final int RECORD_SIZE = 4096 + 1;

    private static final int IO_THREAD_NUMS = 4;

    private static final Random RANDOM = new Random();

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void singleThreadAppend() throws IOException, OverCapacityException {
        final int RECORD_SIZE = 4096 + 1;
        final int RECORD_NUMS = 10;
        final long BLOCK_DEVICE_CAPACITY = WALUtil.alignLargeByBlockSize(RECORD_SIZE) * RECORD_NUMS + WAL_HEADER_CAPACITY_DOUBLE;

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(BLOCK_DEVICE_PATH + "singleThreadAppend")
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
        try {
            for (int i = 0; i < RECORD_NUMS; i++) {
                byte[] bytes = new byte[RECORD_SIZE];
                RANDOM.nextBytes(bytes);
                ByteBuf data = Unpooled.wrappedBuffer(bytes);

                final AppendResult appendResult = wal.append(data);

                final long expectedOffset = i * WALUtil.alignLargeByBlockSize(RECORD_SIZE);
                final long expectedCRC = WALUtil.crc32(data.nioBuffer());
                assertEquals(expectedOffset, appendResult.recordBodyOffset());
                assertEquals(expectedCRC, appendResult.recordBodyCRC());
                assertEquals(RECORD_SIZE, appendResult.length());
                appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                    assertNull(throwable);
                    System.out.printf("%d\n", callbackResult.flushedOffset());
                    assertEquals(expectedCRC, callbackResult.appendResult().recordBodyCRC());
                    assertEquals(expectedOffset, callbackResult.appendResult().recordBodyOffset());
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
                        assertEquals(callbackResult.appendResult().recordBodyCRC(), appendResult.recordBodyCRC());
                        assertEquals(callbackResult.appendResult().recordBodyOffset(), appendResult.recordBodyOffset());

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


    @Test
    void singleThreadRecover() throws IOException, InterruptedException {
        appendManyRecord(1024, BLOCK_DEVICE_PATH + "singleThreadRecover");

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(BLOCK_DEVICE_PATH + "singleThreadRecover")
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();

        try {
            TimeUnit.SECONDS.sleep(3);
            Iterator<RecoverResult> recover = wal.recover();
            if (null == recover) {
                System.out.println("recover is null");
                return;
            }

            while (recover.hasNext()) {
                RecoverResult next = recover.next();
                System.out.println(next);
            }

        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    void multiThreadAppend() throws OverCapacityException, InterruptedException, IOException {

        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(BLOCK_DEVICE_PATH + "multiThreadAppend")
                .ioThreadNums(IO_THREAD_NUMS)
                .createBlockWALService().start();
        try {
            int nThreadNums = 8;
            ExecutorService executorService = Executors.newFixedThreadPool(nThreadNums);
            for (int t = 0; t < nThreadNums; t++) {
                executorService.submit(() -> {
                    try {
                        for (int i = 0; i < 10; i++) {
                            try {
                                String format = String.format("Hello World [%d]", i);
                                final AppendResult appendResult = wal.append(Unpooled.wrappedBuffer(ByteBuffer.wrap(format.getBytes())));
                                appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                                    assertEquals(callbackResult.appendResult().recordBodyCRC(), appendResult.recordBodyCRC());
                                    assertEquals(callbackResult.appendResult().recordBodyOffset(), appendResult.recordBodyOffset());
                                    if (throwable != null) {
                                        throwable.printStackTrace();
                                    }
                                });
                            } catch (OverCapacityException e) {
                                e.printStackTrace(System.err);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }


            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            wal.shutdownGracefully();
        }
    }

}
