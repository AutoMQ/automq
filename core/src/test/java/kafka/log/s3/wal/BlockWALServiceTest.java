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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockWALServiceTest {

    private static final String BLOCK_DEVICE_PATH = String.format("%s/Kafka.BlockWAL.UnitTest.SimuFile-", System.getenv("HOME"));

    private static final int BLOCK_DEVICE_CAPACITY = 1024 * 1024 * 4;

    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void singleThreadAppend() throws IOException {
        final WAL wal = BlockWALService.BlockWALServiceBuilder.build() //
                .setBlockDeviceCapacityWant(BLOCK_DEVICE_CAPACITY) //
                .setBlockDevicePath(BLOCK_DEVICE_PATH + "singleThreadAppend") //
                .setIoThreadNums(4) //
                .createBlockWALService().start();
        try {
            for (int i = 0; i < 10; i++) {
                try {
                    String format = String.format("Hello World [%d]", i);
                    final WAL.AppendResult appendResult = wal.append(ByteBuffer.wrap(format.getBytes()), 0);
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
                } catch (WAL.OverCapacityException e) {
                    e.printStackTrace(System.err);
                    wal.trim(e.flushedOffset());
                }
            }
        } finally {
            wal.shutdownGracefully();
        }
    }


    void appendManyRecord(int recordTotal, String blockDeviceName) throws IOException {
        final WAL wal = BlockWALService.BlockWALServiceBuilder.build() //
                .setBlockDeviceCapacityWant(BLOCK_DEVICE_CAPACITY) //
                .setBlockDevicePath(blockDeviceName) //
                .setIoThreadNums(4) //
                .createBlockWALService().start();
        try {
            // append 1024
            for (int i = 0; i < recordTotal; i++) {
                try {
                    String format = String.format("Hello World [%d]", i);
                    final WAL.AppendResult appendResult = wal.append(ByteBuffer.wrap(format.getBytes()), 0);
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
                } catch (WAL.OverCapacityException e) {
                    e.printStackTrace(System.err);
                    wal.trim(e.flushedOffset());
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

        final WAL wal = BlockWALService.BlockWALServiceBuilder.build() //
                .setBlockDeviceCapacityWant(BLOCK_DEVICE_CAPACITY) //
                .setBlockDevicePath(BLOCK_DEVICE_PATH + "singleThreadRecover")//
                .setIoThreadNums(4) //
                .createBlockWALService().start();

        try {
            TimeUnit.SECONDS.sleep(3);
            Iterator<WAL.RecoverResult> recover = wal.recover();
            if (null == recover) {
                System.out.println("recover is null");
                return;
            }

            while (recover.hasNext()) {
                WAL.RecoverResult next = recover.next();
                System.out.println(next);
            }

        } finally {
            wal.shutdownGracefully();
        }
    }

    @Test
    void multiThreadAppend() throws WAL.OverCapacityException, InterruptedException, IOException {

        final WAL wal = BlockWALService.BlockWALServiceBuilder.build() //
                .setBlockDeviceCapacityWant(BLOCK_DEVICE_CAPACITY) //
                .setBlockDevicePath(BLOCK_DEVICE_PATH + "multiThreadAppend") //
                .setIoThreadNums(4) //
                .createBlockWALService().start();
        try {
            int nThreadNums = 8;
            ExecutorService executorService = Executors.newFixedThreadPool(nThreadNums);
            for (int t = 0; t < nThreadNums; t++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (int i = 0; i < 10; i++) {
                                try {
                                    String format = String.format("Hello World [%d]", i);
                                    final WAL.AppendResult appendResult = wal.append(ByteBuffer.wrap(format.getBytes()), 0);
                                    appendResult.future().whenCompleteAsync((callbackResult, throwable) -> {
                                        assertTrue(callbackResult.appendResult().recordBodyCRC() == appendResult.recordBodyCRC());
                                        assertTrue(callbackResult.appendResult().recordBodyOffset() == appendResult.recordBodyOffset());
                                        if (throwable != null) {
                                            throwable.printStackTrace();
                                        }
                                    });
                                } catch (WAL.OverCapacityException e) {
                                    e.printStackTrace(System.err);
                                    wal.trim(e.flushedOffset());
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }


            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            wal.shutdownGracefully();
        }
    }

}