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

import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_MAGIC_CODE;
import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.BlockWALService.RECORD_HEADER_WITHOUT_CRC_SIZE;
import static com.automq.stream.s3.wal.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
import static com.automq.stream.s3.wal.WriteAheadLog.AppendResult;
import static com.automq.stream.s3.wal.WriteAheadLog.OverCapacityException;

/**
 * The sliding window contains all records that have not been flushed to the disk yet.
 * All records are written to the disk asynchronously by the AIO thread pool.
 * When the sliding window is full, the current thread will be blocked until the sliding window is expanded.
 * When the asynchronous write is completed, the start offset of the sliding window will be updated.
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
        int totalWriteSize = RECORD_HEADER_SIZE + recordBodyLength;

        long lastWriteOffset;
        long newWriteOffset;
        long expectedWriteOffset;
        do {
            lastWriteOffset = windowCoreData.getWindowNextWriteOffset().get();

            expectedWriteOffset = WALUtil.alignLargeByBlockSize(lastWriteOffset);

            // If the end of the physical device is insufficient for this write, jump to the start of the physical device
            if ((recordSectionCapacity - expectedWriteOffset % recordSectionCapacity) < totalWriteSize) {
                expectedWriteOffset = expectedWriteOffset + recordSectionCapacity - expectedWriteOffset % recordSectionCapacity;
            }

            if (expectedWriteOffset + totalWriteSize - trimOffset > recordSectionCapacity) {
                // Not enough space for this write
                LOGGER.error("failed to allocate write offset as the ring buffer is full: expectedWriteOffset: {}, totalWriteSize: {}, trimOffset: {}, recordSectionCapacity: {}",
                        expectedWriteOffset, totalWriteSize, trimOffset, recordSectionCapacity);
                throw new OverCapacityException(String.format("failed to allocate write offset: ring buffer is full: expectedWriteOffset: %d, totalWriteSize: %d, trimOffset: %d, recordSectionCapacity: %d",
                        expectedWriteOffset, totalWriteSize, trimOffset, recordSectionCapacity));
            }

            newWriteOffset = WALUtil.alignLargeByBlockSize(expectedWriteOffset + totalWriteSize);
        } while (!windowCoreData.getWindowNextWriteOffset().compareAndSet(lastWriteOffset, newWriteOffset));

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
        long position = WALUtil.recordOffsetToPosition(ioTask.startOffset(), walChannel.capacity() - WAL_HEADER_TOTAL_CAPACITY, WAL_HEADER_TOTAL_CAPACITY);

        walChannel.write(totalRecord, position);
    }

    public boolean makeWriteOffsetMatchWindow(final WriteRecordTask writeRecordTask) throws IOException {
        long newWindowEndOffset = writeRecordTask.startOffset() + writeRecordTask.recordHeader().limit() + writeRecordTask.recordBody().limit();
        // align to block size
        newWindowEndOffset = WALUtil.alignLargeByBlockSize(newWindowEndOffset);
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
        private int magicCode0 = RECORD_HEADER_MAGIC_CODE;
        private int recordBodyLength1;
        private long recordBodyOffset2;
        private int recordBodyCRC3;
        private int recordHeaderCRC4;

        public static RecordHeaderCoreData unmarshal(ByteBuffer byteBuffer) {
            RecordHeaderCoreData recordHeaderCoreData = new RecordHeaderCoreData();
            recordHeaderCoreData.magicCode0 = byteBuffer.getInt();
            recordHeaderCoreData.recordBodyLength1 = byteBuffer.getInt();
            recordHeaderCoreData.recordBodyOffset2 = byteBuffer.getLong();
            recordHeaderCoreData.recordBodyCRC3 = byteBuffer.getInt();
            recordHeaderCoreData.recordHeaderCRC4 = byteBuffer.getInt();
            return recordHeaderCoreData;
        }

        public int getMagicCode() {
            return magicCode0;
        }

        public RecordHeaderCoreData setMagicCode(int magicCode) {
            this.magicCode0 = magicCode;
            return this;
        }

        public int getRecordBodyLength() {
            return recordBodyLength1;
        }

        public RecordHeaderCoreData setRecordBodyLength(int recordBodyLength) {
            this.recordBodyLength1 = recordBodyLength;
            return this;
        }

        public long getRecordBodyOffset() {
            return recordBodyOffset2;
        }

        public RecordHeaderCoreData setRecordBodyOffset(long recordBodyOffset) {
            this.recordBodyOffset2 = recordBodyOffset;
            return this;
        }

        public int getRecordBodyCRC() {
            return recordBodyCRC3;
        }

        public RecordHeaderCoreData setRecordBodyCRC(int recordBodyCRC) {
            this.recordBodyCRC3 = recordBodyCRC;
            return this;
        }

        public int getRecordHeaderCRC() {
            return recordHeaderCRC4;
        }

        public RecordHeaderCoreData setRecordHeaderCRC(int recordHeaderCRC) {
            this.recordHeaderCRC4 = recordHeaderCRC;
            return this;
        }

        @Override
        public String toString() {
            return "RecordHeaderCoreData{" +
                    "magicCode=" + magicCode0 +
                    ", recordBodyLength=" + recordBodyLength1 +
                    ", recordBodyOffset=" + recordBodyOffset2 +
                    ", recordBodyCRC=" + recordBodyCRC3 +
                    ", recordHeaderCRC=" + recordHeaderCRC4 +
                    '}';
        }

        private ByteBuffer marshalHeaderExceptCRC() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_HEADER_SIZE);
            byteBuffer.putInt(magicCode0);
            byteBuffer.putInt(recordBodyLength1);
            byteBuffer.putLong(recordBodyOffset2);
            byteBuffer.putInt(recordBodyCRC3);
            return byteBuffer;
        }

        public ByteBuffer marshal() {
            ByteBuffer byteBuffer = marshalHeaderExceptCRC();
            byteBuffer.putInt(WALUtil.crc32(byteBuffer, RECORD_HEADER_WITHOUT_CRC_SIZE));
            return byteBuffer.position(0);
        }
    }

    public static class WindowCoreData {
        private final Lock treeMapIOTaskRequestLock = new ReentrantLock();
        private final TreeMap<Long, WriteRecordTask> treeMapWriteRecordTask = new TreeMap<>();
        private final AtomicLong windowMaxLength = new AtomicLong(0);
        /**
         * Next write offset of sliding window, always aligned to the {@link WALUtil#BLOCK_SIZE}.
         */
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
                    windowStartOffset.set(windowNextWriteOffset.get());
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
                if (newWindowMaxLength < windowMaxLength.get()) {
                    // Another thread has already scaled out the window.
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

                    // Update the start offset of the sliding window after finishing writing the record.
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
