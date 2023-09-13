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

import io.netty.buffer.Unpooled;
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

    private static final Random RANDOM = new Random();

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void singleThreadAppend() throws IOException {
        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(BLOCK_DEVICE_PATH + "singleThreadAppend")
                .ioThreadNums(4)
                .createBlockWALService().start();
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    byte[] data = new byte[RECORD_SIZE];
                    RANDOM.nextBytes(data);

                    final AppendResult appendResult = wal.append(Unpooled.wrappedBuffer(data));

                    assertEquals(RECORD_SIZE, appendResult.length());
                    appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                        assertEquals(callbackResult.appendResult().recordBodyCRC(), appendResult.recordBodyCRC());
                        assertEquals(callbackResult.appendResult().recordBodyOffset(), appendResult.recordBodyOffset());
                        assertNull(throwable);
                    });
                } catch (OverCapacityException e) {
                    e.printStackTrace(System.err);
                }
            }
        } finally {
            wal.shutdownGracefully();
        }
    }


    void appendManyRecord(int recordTotal, String blockDeviceName) throws IOException {
        final WriteAheadLog wal = BlockWALService.BlockWALServiceBuilder.build()
                .capacity(BLOCK_DEVICE_CAPACITY)
                .blockDevicePath(blockDeviceName)
                .ioThreadNums(4)
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
                .ioThreadNums(4)
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
                .ioThreadNums(4)
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
