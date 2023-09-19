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

import com.automq.stream.s3.wal.util.ThreadFactoryImpl;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;
import static com.automq.stream.s3.wal.WriteAheadLog.OverCapacityException;


/**
 * 维护滑动窗口，AIO 线程池异步写入并通知调用方。
 * 滑动窗口容量不足时，单线程停止当前操作，执行同步扩容操作。
 * AIO 线程池返回时，同步更新滑动窗口的起始 Offset。
 */
public class SlidingWindowService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlidingWindowService.class.getSimpleName());
    private final int ioThreadNums;
    private final long upperLimit;
    private final long scaleUnit;
    private final int writeRecordTaskWorkQueueCapacity;
    private final WALChannel walChannel;
    private final WindowCoreData windowCoreData = new WindowCoreData();
    private ExecutorService executorService;

    public SlidingWindowService(WALChannel walChannel, int ioThreadNums, long upperLimit, long scaleUnit, int queueCapacity) {
        this.walChannel = walChannel;
        this.ioThreadNums = ioThreadNums;
        this.upperLimit = upperLimit;
        this.scaleUnit = scaleUnit;
        this.writeRecordTaskWorkQueueCapacity = queueCapacity;
    }

    private ExecutorService newCachedThreadPool(int nThreads) {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(writeRecordTaskWorkQueueCapacity);
        ThreadFactoryImpl threadFactory = new ThreadFactoryImpl("block-wal-io-thread-");
        return new ThreadPoolExecutor(1, nThreads, 1L, TimeUnit.MINUTES, workQueue, threadFactory);
    }

    public void resetWindowWhenRecoverOver(long startOffset, long nextWriteOffset, long maxLength) {
        windowCoreData.getWindowStartOffset().set(startOffset);
        windowCoreData.getWindowNextWriteOffset().set(nextWriteOffset);
        windowCoreData.getWindowMaxLength().set(maxLength);
    }

    public WindowCoreData getWindowCoreData() {
        return windowCoreData;
    }

    public void start() throws IOException {
        this.executorService = newCachedThreadPool(ioThreadNums);
    }

    public boolean shutdown(long timeout, TimeUnit unit) {
        this.executorService.shutdown();
        try {
            return this.executorService.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            this.executorService.shutdownNow();
            return false;
        }
    }

    public long allocateWriteOffset(final int recordBodyLength, final long trimOffset, final long recordSectionCapacity) throws OverCapacityException {
        // 计算要写入的总大小
        int totalWriteSize = BlockWALService.RECORD_HEADER_SIZE + recordBodyLength;

        // 计算写入 wal offset
        long lastWriteOffset;
        long expectedWriteOffset;
        do {
            lastWriteOffset = windowCoreData.getWindowNextWriteOffset().get();

            expectedWriteOffset = WALUtil.alignLargeByBlockSize(lastWriteOffset);

            // 如果物理设备末尾不足这次写入，则跳转到物理设备起始位置
            if ((recordSectionCapacity - expectedWriteOffset % recordSectionCapacity) < totalWriteSize) {
                expectedWriteOffset = expectedWriteOffset + recordSectionCapacity - expectedWriteOffset % recordSectionCapacity;
            }

            // 如果 trim 不及时，会导致写 RingBuffer 覆盖有效数据，抛异常
            if (expectedWriteOffset + totalWriteSize - trimOffset > recordSectionCapacity) {
                LOGGER.error("failed to allocate write offset as the ring buffer is full: expectedWriteOffset: {}, totalWriteSize: {}, trimOffset: {}, recordSectionCapacity: {}",
                        expectedWriteOffset, totalWriteSize, trimOffset, recordSectionCapacity);
                throw new OverCapacityException(String.format("failed to allocate write offset: ring buffer is full: expectedWriteOffset: %d, totalWriteSize: %d, trimOffset: %d, recordSectionCapacity: %d",
                        expectedWriteOffset, totalWriteSize, trimOffset, recordSectionCapacity));
            }

        } while (!windowCoreData.getWindowNextWriteOffset().compareAndSet(lastWriteOffset, expectedWriteOffset + totalWriteSize));

        return expectedWriteOffset;
    }

    public void submitWriteRecordTask(WriteRecordTask ioTask) {
        executorService.submit(new WriteRecordTaskProcessor(ioTask));
    }


    private void writeRecord(WriteRecordTask ioTask) throws IOException {
        final ByteBuffer totalRecord = ByteBuffer.allocate(ioTask.recordHeader().limit() + ioTask.recordBody().limit());

        totalRecord.put(ioTask.recordHeader());

        totalRecord.put(ioTask.recordBody());

        totalRecord.position(0);

        // TODO: make this beautiful
        long position = WALUtil.recordOffsetToPosition(ioTask.startOffset(), walChannel.capacity() - BlockWALService.WAL_HEADER_TOTAL_CAPACITY, BlockWALService.WAL_HEADER_TOTAL_CAPACITY);

        walChannel.write(totalRecord, position);
    }

    public boolean makeWriteOffsetMatchWindow(final WriteRecordTask writeRecordTask) throws IOException {
        long newWindowEndOffset = writeRecordTask.startOffset() + writeRecordTask.recordHeader().limit() + writeRecordTask.recordBody().limit();
        long windowStartOffset = windowCoreData.getWindowStartOffset().get();
        long windowMaxLength = windowCoreData.getWindowMaxLength().get();
        if (newWindowEndOffset > windowStartOffset + windowMaxLength) {
            long newWindowMaxLength = newWindowEndOffset - windowStartOffset + scaleUnit;
            if (newWindowMaxLength > upperLimit) {
                // exceed upper limit
                if (newWindowEndOffset - windowStartOffset >= upperLimit) {
                    // however, the new window length is still larger than upper limit, so we just set it to upper limit
                    newWindowMaxLength = upperLimit;
                } else {
                    // the new window length is bigger than upper limit, reject this write request
                    LOGGER.error("new windows size {} exceeds upper limit {}, reject this write request, window start offset: {}, new window end offset: {}",
                            newWindowMaxLength, upperLimit, windowStartOffset, newWindowEndOffset);
                    writeRecordTask.future().completeExceptionally(
                            new OverCapacityException(String.format("new windows size exceeds upper limit %d", upperLimit))
                    );
                    return false;
                }
            }
            windowCoreData.scaleOutWindow(writeRecordTask, newWindowMaxLength);
        }
        return true;
    }

    public static class RecordHeaderCoreData {
        private int magicCodePos0 = BlockWALService.RECORD_HEADER_MAGIC_CODE;
        private int recordBodyLengthPos1;
        private long recordBodyOffsetPos2;
        private int recordBodyCRCPos3;
        private int recordHeaderCRCPos4;

        public static RecordHeaderCoreData unmarshal(ByteBuffer byteBuffer) {
            RecordHeaderCoreData recordHeaderCoreData = new RecordHeaderCoreData();
            recordHeaderCoreData.magicCodePos0 = byteBuffer.getInt();
            recordHeaderCoreData.recordBodyLengthPos1 = byteBuffer.getInt();
            recordHeaderCoreData.recordBodyOffsetPos2 = byteBuffer.getLong();
            recordHeaderCoreData.recordBodyCRCPos3 = byteBuffer.getInt();
            recordHeaderCoreData.recordHeaderCRCPos4 = byteBuffer.getInt();
            return recordHeaderCoreData;
        }

        public int getMagicCode() {
            return magicCodePos0;
        }

        public RecordHeaderCoreData setMagicCode(int magicCode) {
            this.magicCodePos0 = magicCode;
            return this;
        }

        public int getRecordBodyLength() {
            return recordBodyLengthPos1;
        }

        public RecordHeaderCoreData setRecordBodyLength(int recordBodyLength) {
            this.recordBodyLengthPos1 = recordBodyLength;
            return this;
        }

        public long getRecordBodyOffset() {
            return recordBodyOffsetPos2;
        }

        public RecordHeaderCoreData setRecordBodyOffset(long recordBodyOffset) {
            this.recordBodyOffsetPos2 = recordBodyOffset;
            return this;
        }

        public int getRecordBodyCRC() {
            return recordBodyCRCPos3;
        }

        public RecordHeaderCoreData setRecordBodyCRC(int recordBodyCRC) {
            this.recordBodyCRCPos3 = recordBodyCRC;
            return this;
        }

        public int getRecordHeaderCRC() {
            return recordHeaderCRCPos4;
        }

        public RecordHeaderCoreData setRecordHeaderCRC(int recordHeaderCRC) {
            this.recordHeaderCRCPos4 = recordHeaderCRC;
            return this;
        }

        @Override
        public String toString() {
            return "RecordHeaderCoreData{" +
                    "magicCode=" + magicCodePos0 +
                    ", recordBodyLength=" + recordBodyLengthPos1 +
                    ", recordBodyOffset=" + recordBodyOffsetPos2 +
                    ", recordBodyCRC=" + recordBodyCRCPos3 +
                    ", recordHeaderCRC=" + recordHeaderCRCPos4 +
                    '}';
        }

        private ByteBuffer marshalHeaderExceptCRC() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(BlockWALService.RECORD_HEADER_SIZE);
            byteBuffer.putInt(magicCodePos0);
            byteBuffer.putInt(recordBodyLengthPos1);
            byteBuffer.putLong(recordBodyOffsetPos2);
            byteBuffer.putInt(recordBodyCRCPos3);
            return byteBuffer;
        }

        public ByteBuffer marshal() {
            ByteBuffer byteBuffer = marshalHeaderExceptCRC();
            byteBuffer.putInt(WALUtil.crc32(byteBuffer, BlockWALService.RECORD_HEADER_WITHOUT_CRC_SIZE));
            return byteBuffer.position(0);
        }
    }

    public class WindowCoreData {
        private final Lock treeMapIOTaskRequestLock = new ReentrantLock();
        private final TreeMap<Long, WriteRecordTask> treeMapWriteRecordTask = new TreeMap<>();
        private final AtomicLong windowMaxLength = new AtomicLong(0);
        private final AtomicLong windowNextWriteOffset = new AtomicLong(0);
        /**
         * Start offset of sliding window, always aligned to the {@link WALUtil#BLOCK_SIZE}.
         * The data before this offset has already been written to the disk.
         */
        private final AtomicLong windowStartOffset = new AtomicLong(0);

        public AtomicLong getWindowMaxLength() {
            return windowMaxLength;
        }

        public AtomicLong getWindowNextWriteOffset() {
            return windowNextWriteOffset;
        }

        public AtomicLong getWindowStartOffset() {
            return windowStartOffset;
        }

        public void putWriteRecordTask(WriteRecordTask writeRecordTask) {
            this.treeMapIOTaskRequestLock.lock();
            try {
                this.treeMapWriteRecordTask.put(writeRecordTask.startOffset(), writeRecordTask);
            } finally {
                this.treeMapIOTaskRequestLock.unlock();
            }
        }

        public void calculateStartOffset(long wroteOffset) {
            this.treeMapIOTaskRequestLock.lock();
            try {
                treeMapWriteRecordTask.remove(wroteOffset);

                if (treeMapWriteRecordTask.isEmpty()) {
                    windowStartOffset.set(WALUtil.alignLargeByBlockSize(windowNextWriteOffset.get()));
                } else {
                    windowStartOffset.set(treeMapWriteRecordTask.firstKey());
                }
            } finally {
                this.treeMapIOTaskRequestLock.unlock();
            }
        }

        public void scaleOutWindow(WriteRecordTask writeRecordTask, long newWindowMaxLength) throws IOException {
            boolean scaleWindowHappened = false;
            treeMapIOTaskRequestLock.lock();
            try {
                // 多线程同时扩容，只需要一个线程操作，其他线程快速返回
                if (newWindowMaxLength < windowMaxLength.get()) {
                    return;
                }

                writeRecordTask.flushWALHeader(newWindowMaxLength);
                windowMaxLength.set(newWindowMaxLength);
                scaleWindowHappened = true;
            } finally {
                treeMapIOTaskRequestLock.unlock();
                if (scaleWindowHappened) {
                    LOGGER.info("window scale out to {}", newWindowMaxLength);
                } else {
                    LOGGER.debug("window already scale out, ignore");
                }
            }
        }
    }

    class WriteRecordTaskProcessor implements Runnable {
        private final WriteRecordTask writeRecordTask;

        public WriteRecordTaskProcessor(WriteRecordTask writeRecordTask) {
            this.writeRecordTask = writeRecordTask;
        }

        @Override
        public void run() {
            try {
                if (makeWriteOffsetMatchWindow(writeRecordTask)) {

                    windowCoreData.putWriteRecordTask(writeRecordTask);

                    writeRecord(writeRecordTask);

                    // 更新滑动窗口的最小 Offset
                    windowCoreData.calculateStartOffset(writeRecordTask.startOffset());

                    writeRecordTask.future().complete(new AppendResult.CallbackResult() {
                        @Override
                        public long flushedOffset() {
                            return windowCoreData.getWindowStartOffset().get();
                        }

                        @Override
                        public String toString() {
                            return "CallbackResult{" + "flushedOffset=" + flushedOffset() + '}';
                        }
                    });
                }

            } catch (IOException e) {
                writeRecordTask.future().completeExceptionally(e);
                LOGGER.error(String.format("failed to write record, offset: %s", writeRecordTask.startOffset()), e);
            }
        }
    }
}
