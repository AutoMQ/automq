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

import com.automq.stream.s3.Config;
import com.automq.stream.s3.wal.util.ThreadFactoryImpl;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * /**
 * BlockWALService provides an infinite WAL, which is implemented based on block devices.
 * The capacity of the block device is configured by the application and may be smaller than the system allocation.
 * <p>
 * Usage:
 * <p>
 * 1. Call {@link BlockWALService#start} to start the service. Any other methods will throw an
 * {@link IllegalStateException} if called before {@link BlockWALService#start}.
 * <p>
 * 2. Call {@link BlockWALService#recover} to recover all untrimmed records if any.
 * <p>
 * 3. Call {@link BlockWALService#reset} to reset the service. This will clear all records, so make sure
 * all recovered records are processed before calling this method.
 * 4. Call {@link BlockWALService#append} to append records. As records are written in a circular way similar to
 * RingBuffer, if the caller does not call {@link BlockWALService#trim} in time, an {@link OverCapacityException}
 * will be thrown when calling {@link BlockWALService#append}.
 * <p>
 * 5. Call {@link BlockWALService#shutdownGracefully} to shut down the service gracefully, which will wait for
 * all pending writes to complete.
 * <p>
 * Implementation:
 * <p>
 * WAL Header
 * <p>
 * There are {@link BlockWALService#WAL_HEADER_COUNT} WAL headers, each of which is {@link WALUtil#BLOCK_SIZE} bytes.
 * Every {@link BlockWALService#walHeaderFlushIntervalSeconds}, the service will flush the WAL header to the block
 * device. The WAL header is used to record the meta information of the WAL, and is used to recover the WAL when
 * the service is restarted.
 * <p>
 * Layout:
 * <p>
 * 0 - [4B] {@link WALHeaderCoreData#magicCode0} Magic code of the WAL header, used to verify the start of the WAL header
 * <p>
 * 1 - [8B] {@link WALHeaderCoreData#capacity1} Capacity of the block device, which is configured by the application
 * and should not be modified after the first start of the service
 * <p>
 * 2 - [8B] {@link WALHeaderCoreData#trimOffset2} The logical start offset of the WAL, records before which are
 * considered useless and have been deleted
 * <p>
 * 3 - [8B] {@link WALHeaderCoreData#lastWriteTimestamp3} The timestamp of the last write to the WAL header, used to
 * determine which WAL header is the latest when recovering
 * <p>
 * 4 - [8B] {@link WALHeaderCoreData#slidingWindowNextWriteOffset4} The offset of the next record to be written
 * in the sliding window
 * <p>
 * 5 - [8B] {@link WALHeaderCoreData#slidingWindowStartOffset5} The start offset of the sliding window, all records
 * before this offset have been successfully written to the block device
 * <p>
 * 6 - [8B] {@link WALHeaderCoreData#slidingWindowMaxLength6} The maximum size of the sliding window, which can be
 * scaled up when needed, and is used to determine when to stop recovering
 * <p>
 * 7 - [4B] {@link WALHeaderCoreData#shutdownType7} The shutdown type of the service, {@link ShutdownType#GRACEFULLY} or
 * {@link ShutdownType#UNGRACEFULLY}
 * <p>
 * 8 - [4B] {@link WALHeaderCoreData#crc8} CRC of the rest of the WAL header, used to verify the correctness of the
 * WAL header
 * <p>
 * Sliding Window
 * <p>
 * The sliding window contains all records that have not been successfully written to the block device.
 * So when recovering, we only need to try to recover the records in the sliding window.
 * <p>
 * Record Header
 * <p>
 * Layout:
 * <p>
 * 0 - [4B] {@link SlidingWindowService.RecordHeaderCoreData#getMagicCode} Magic code of the record header,
 * used to verify the start of the record header
 * <p>
 * 1 - [4B] {@link SlidingWindowService.RecordHeaderCoreData#getRecordBodyLength} The length of the record body
 * <p>
 * 2 - [8B] {@link SlidingWindowService.RecordHeaderCoreData#getRecordBodyOffset} The logical start offset of the record body
 * <p>
 * 3 - [4B] {@link SlidingWindowService.RecordHeaderCoreData#getRecordBodyCRC} CRC of the record body, used to verify
 * the correctness of the record body
 * <p>
 * 4 - [4B] {@link SlidingWindowService.RecordHeaderCoreData#getRecordHeaderCRC} CRC of the rest of the record header,
 * used to verify the correctness of the record header
 */
public class BlockWALService implements WriteAheadLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockWALService.class);
    public static final int RECORD_HEADER_SIZE = 4 + 4 + 8 + 4 + 4;
    public static final int RECORD_HEADER_WITHOUT_CRC_SIZE = RECORD_HEADER_SIZE - 4;
    public static final int RECORD_HEADER_MAGIC_CODE = 0x87654321;
    public static final int WAL_HEADER_MAGIC_CODE = 0x12345678;
    public static final int WAL_HEADER_SIZE = 4 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4;
    public static final int WAL_HEADER_WITHOUT_CRC_SIZE = WAL_HEADER_SIZE - 4;
    public static final int WAL_HEADER_COUNT = 2;
    public static final int WAL_HEADER_CAPACITY = WALUtil.BLOCK_SIZE;
    public static final int WAL_HEADER_TOTAL_CAPACITY = WAL_HEADER_CAPACITY * WAL_HEADER_COUNT;
    private int walHeaderFlushIntervalSeconds;
    private long initialWindowSize;
    private final AtomicBoolean readyToServe = new AtomicBoolean(false);
    private final AtomicLong writeHeaderRoundTimes = new AtomicLong(0);
    private ScheduledExecutorService flushWALHeaderScheduler;
    private WALChannel walChannel;
    private SlidingWindowService slidingWindowService;
    private WALHeaderCoreData walHeaderCoreData;

    private void startFlushWALHeaderScheduler() {
        this.flushWALHeaderScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("flush-wal-header-thread-"));
        this.flushWALHeaderScheduler.scheduleAtFixedRate(() -> {
            try {
                BlockWALService.this.flushWALHeader(
                        this.slidingWindowService.getWindowCoreData().getWindowStartOffset().get(),
                        this.slidingWindowService.getWindowCoreData().getWindowMaxLength().get(),
                        this.slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get(),
                        ShutdownType.UNGRACEFULLY);
            } catch (IOException ignored) {
            }
        }, walHeaderFlushIntervalSeconds, walHeaderFlushIntervalSeconds, TimeUnit.SECONDS);
    }

    @Deprecated
    private void flushWALHeader(long windowStartOffset,
                                long windowMaxLength,
                                long windowNextWriteOffset,
                                ShutdownType shutdownType
    ) throws IOException {
        walHeaderCoreData
                .setSlidingWindowStartOffset(windowStartOffset)
                .setSlidingWindowMaxLength(windowMaxLength)
                .setSlidingWindowNextWriteOffset(windowNextWriteOffset)
                .setShutdownType(shutdownType);
        flushWALHeader();
    }

    private synchronized void flushWALHeader() throws IOException {
        long position = writeHeaderRoundTimes.getAndIncrement() % WAL_HEADER_COUNT * WAL_HEADER_CAPACITY;
        try {
            walHeaderCoreData.setLastWriteTimestamp(System.nanoTime());
            long trimOffset = walHeaderCoreData.getTrimOffset();
            this.walChannel.write(walHeaderCoreData.marshal(), position);
            walHeaderCoreData.setFlushedTrimOffset(trimOffset);
            LOGGER.debug("WAL header flushed, position: {}, header: {}", position, walHeaderCoreData);
        } catch (IOException e) {
            LOGGER.error("failed to flush WAL header, position: {}, header: {}", position, walHeaderCoreData, e);
            throw e;
        }
    }

    private ByteBuffer readRecord(WALHeaderCoreData paramWALHeader, long recoverStartOffset) throws ReadRecordException {
        final ByteBuffer recordHeader = ByteBuffer.allocate(RECORD_HEADER_SIZE);
        final long position = WALUtil.recordOffsetToPosition(recoverStartOffset, paramWALHeader.recordSectionCapacity(), WAL_HEADER_TOTAL_CAPACITY);
        try {
            int read = walChannel.read(recordHeader, position);
            if (read != RECORD_HEADER_SIZE) {
                throw new ReadRecordException(
                        WALUtil.alignNextBlock(recoverStartOffset),
                        String.format("failed to read record header: expected %d bytes, actual %d bytes, recoverStartOffset: %d", RECORD_HEADER_SIZE, read, recoverStartOffset)
                );
            }
        } catch (IOException e) {
            LOGGER.error("failed to read record header, position: {}, recoverStartOffset: {}", position, recoverStartOffset, e);
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("failed to read record header, recoverStartOffset: %d", recoverStartOffset)
            );
        }

        SlidingWindowService.RecordHeaderCoreData readRecordHeader = SlidingWindowService.RecordHeaderCoreData.unmarshal(recordHeader.position(0).limit(RECORD_HEADER_SIZE));
        if (readRecordHeader.getMagicCode() != RECORD_HEADER_MAGIC_CODE) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("magic code mismatch: expected %d, actual %d, recoverStartOffset: %d", RECORD_HEADER_MAGIC_CODE, readRecordHeader.getMagicCode(), recoverStartOffset)
            );
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        int recordBodyCRC = readRecordHeader.getRecordBodyCRC();
        int recordHeaderCRC = readRecordHeader.getRecordHeaderCRC();

        int calculatedRecordHeaderCRC = WALUtil.crc32(recordHeader, RECORD_HEADER_WITHOUT_CRC_SIZE);
        if (recordHeaderCRC != calculatedRecordHeaderCRC) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("record header crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordHeaderCRC, recordHeaderCRC, recoverStartOffset)
            );
        }

        if (recordBodyLength <= 0) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("invalid record body length: %d, recoverStartOffset: %d", recordBodyLength, recoverStartOffset)
            );
        }

        if (recordBodyOffset != recoverStartOffset + RECORD_HEADER_SIZE) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("invalid record body offset: expected %d, actual %d, recoverStartOffset: %d", recoverStartOffset + RECORD_HEADER_SIZE, recordBodyOffset, recoverStartOffset)
            );
        }

        ByteBuffer recordBody = ByteBuffer.allocate(recordBodyLength);
        try {
            int read = walChannel.read(recordBody, WALUtil.recordOffsetToPosition(recordBodyOffset, paramWALHeader.recordSectionCapacity(), WAL_HEADER_TOTAL_CAPACITY));
            if (read != recordBodyLength) {
                throw new ReadRecordException(
                        WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                        String.format("failed to read record body: expected %d bytes, actual %d bytes, recoverStartOffset: %d", recordBodyLength, read, recoverStartOffset)
                );
            }
        } catch (IOException e) {
            LOGGER.error("failed to read record body, position: {}, recoverStartOffset: {}", recordBodyOffset, recoverStartOffset, e);
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                    String.format("failed to read record body, recoverStartOffset: %d", recoverStartOffset)
            );
        }

        recordBody.position(0).limit(recordBodyLength);

        int calculatedRecordBodyCRC = WALUtil.crc32(recordBody);
        if (recordBodyCRC != calculatedRecordBodyCRC) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                    String.format("record body crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordBodyCRC, recordBodyCRC, recoverStartOffset)
            );
        }

        return recordBody.position(0);
    }

    private WALHeaderCoreData recoverEntireWALAndCorrectWALHeader(WALHeaderCoreData paramWALHeader) {
        // initialize flushTrimOffset
        paramWALHeader.setFlushedTrimOffset(paramWALHeader.getTrimOffset());

        // graceful shutdown, no need to correct the header
        if (paramWALHeader.getShutdownType().equals(ShutdownType.GRACEFULLY)) {
            LOGGER.info("recovered from graceful shutdown, WALHeader: {}", paramWALHeader);
            return paramWALHeader;
        }

        // ungraceful shutdown, need to correct the header
        long recoverStartOffset = WALUtil.alignLargeByBlockSize(paramWALHeader.getSlidingWindowStartOffset());
        long recoverRemainingBytes = paramWALHeader.recordSectionCapacity();

        long meetIllegalRecordTimes = 0;
        LOGGER.info("start to recover from ungraceful shutdown, recoverStartOffset: {}, recoverRemainingBytes: {}", recoverStartOffset, recoverRemainingBytes);
        do {
            long nextRecoverStartOffset;

            try {
                ByteBuffer body = readRecord(paramWALHeader, recoverStartOffset);
                nextRecoverStartOffset = WALUtil.alignLargeByBlockSize(recoverStartOffset + RECORD_HEADER_SIZE + body.limit());
            } catch (ReadRecordException e) {
                nextRecoverStartOffset = e.getJumpNextRecoverOffset();
                LOGGER.debug("failed to read record, try next, recoverStartOffset: {}, meetIllegalRecordTimes: {}, recoverRemainingBytes: {}, error: {}",
                        recoverStartOffset, meetIllegalRecordTimes, recoverRemainingBytes, e.getMessage());
                meetIllegalRecordTimes++;
            }

            recoverRemainingBytes -= nextRecoverStartOffset - recoverStartOffset;
            recoverStartOffset = nextRecoverStartOffset;
            paramWALHeader.setSlidingWindowStartOffset(nextRecoverStartOffset).setSlidingWindowNextWriteOffset(nextRecoverStartOffset);
        } while (recoverRemainingBytes > 0);

        LOGGER.info("recovered from ungraceful shutdown, WALHeader: {}", paramWALHeader);
        return paramWALHeader;
    }

    private void recoverWALHeader() throws IOException {
        WALHeaderCoreData walHeaderCoreDataAvailable = null;

        for (int i = 0; i < WAL_HEADER_COUNT; i++) {
            try {
                final ByteBuffer byteBuffer = ByteBuffer.allocate(WAL_HEADER_SIZE);
                walChannel.read(byteBuffer, i * WAL_HEADER_CAPACITY);
                WALHeaderCoreData walHeaderCoreData = WALHeaderCoreData.unmarshal(byteBuffer.position(0).limit(WAL_HEADER_SIZE));
                if (walHeaderCoreDataAvailable == null || walHeaderCoreDataAvailable.lastWriteTimestamp3 < walHeaderCoreData.lastWriteTimestamp3) {
                    walHeaderCoreDataAvailable = walHeaderCoreData;
                }
            } catch (IOException | UnmarshalException ignored) {
                // failed to parse WALHeader, ignore
            }
        }

        if (walHeaderCoreDataAvailable != null) {
            walHeaderCoreData = recoverEntireWALAndCorrectWALHeader(walHeaderCoreDataAvailable);
        } else {
            walHeaderCoreData = new WALHeaderCoreData()
                    .setCapacity(walChannel.capacity())
                    .setSlidingWindowMaxLength(initialWindowSize)
                    .setShutdownType(ShutdownType.UNGRACEFULLY);
            LOGGER.info("no valid WALHeader found, create new WALHeader: {}", walHeaderCoreData);
        }
        flushWALHeader();
        slidingWindowService.resetWindowWhenRecoverOver(
                walHeaderCoreData.getSlidingWindowStartOffset(),
                walHeaderCoreData.getSlidingWindowNextWriteOffset(),
                walHeaderCoreData.getSlidingWindowMaxLength()
        );
    }

    @Override
    public WriteAheadLog start() throws IOException {
        StopWatch stopWatch = StopWatch.createStarted();

        walChannel.open();
        recoverWALHeader();
        startFlushWALHeaderScheduler();
        slidingWindowService.start();
        readyToServe.set(true);

        LOGGER.info("block WAL service started, cost: {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
        return this;
    }

    @Override
    public void shutdownGracefully() {
        StopWatch stopWatch = StopWatch.createStarted();

        readyToServe.set(false);
        flushWALHeaderScheduler.shutdown();
        try {
            if (!flushWALHeaderScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushWALHeaderScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushWALHeaderScheduler.shutdownNow();
        }

        boolean gracefulShutdown = slidingWindowService.shutdown(1, TimeUnit.DAYS);
        try {
            flushWALHeader(
                    slidingWindowService.getWindowCoreData().getWindowStartOffset().get(),
                    slidingWindowService.getWindowCoreData().getWindowMaxLength().get(),
                    slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get(),
                    gracefulShutdown ? ShutdownType.GRACEFULLY : ShutdownType.UNGRACEFULLY
            );
        } catch (IOException e) {
            LOGGER.error("failed to flush WALHeader when shutdown gracefully", e);
        }

        walChannel.close();

        LOGGER.info("block WAL service shutdown gracefully: {}, cost: {} ms", gracefulShutdown, stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public AppendResult append(ByteBuf buf, int crc) throws OverCapacityException {
        checkReadyToServe();

        ByteBuffer record = buf.nioBuffer();

        final int recordBodyCRC = 0 == crc ? WALUtil.crc32(record) : crc;
        final long expectedWriteOffset = slidingWindowService.allocateWriteOffset(record.limit(), walHeaderCoreData.getFlushedTrimOffset(), walHeaderCoreData.getCapacity() - WAL_HEADER_TOTAL_CAPACITY);
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        final AppendResult appendResult = new AppendResultImpl(expectedWriteOffset, appendResultFuture);

        // submit write task
        slidingWindowService.submitWriteRecordTask(new WriteRecordTask() {
            @Override
            public long startOffset() {
                return expectedWriteOffset;
            }

            @Override
            public CompletableFuture<AppendResult.CallbackResult> future() {
                return appendResultFuture;
            }

            @Override
            public ByteBuffer recordHeader() {
                SlidingWindowService.RecordHeaderCoreData recordHeaderCoreData = new SlidingWindowService.RecordHeaderCoreData();
                recordHeaderCoreData
                        .setMagicCode(RECORD_HEADER_MAGIC_CODE)
                        .setRecordBodyLength(record.limit())
                        .setRecordBodyOffset(expectedWriteOffset + RECORD_HEADER_SIZE)
                        .setRecordBodyCRC(recordBodyCRC);
                return recordHeaderCoreData.marshal();
            }

            @Override
            public ByteBuffer recordBody() {
                return record;
            }

            @Override
            public void flushWALHeader(long windowMaxLength) throws IOException {
                BlockWALService.this.flushWALHeader(
                        slidingWindowService.getWindowCoreData().getWindowStartOffset().get(),
                        windowMaxLength,
                        slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get(),
                        ShutdownType.UNGRACEFULLY
                );
            }
        });

        return appendResult;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        checkReadyToServe();

        long trimmedOffset = walHeaderCoreData.getTrimOffset();
        if (trimmedOffset != 0) {
            // As the offset in {@link this#trim(long)} is an inclusive offset, we need to skip the first record.
            return recover(trimmedOffset, true);
        } else {
            return recover(trimmedOffset, false);
        }
    }

    /**
     * Recover from the given offset.
     */
    private Iterator<RecoverResult> recover(long startOffset, boolean skipFirstRecord) {
        long recoverStartOffset = WALUtil.alignSmallByBlockSize(startOffset);
        RecoverIterator iterator = new RecoverIterator(recoverStartOffset);
        if (skipFirstRecord) {
            if (iterator.hasNext()) {
                iterator.next();
            }
        }
        return iterator;
    }

    @Override
    public CompletableFuture<Void> reset() {
        checkReadyToServe();

        long previousNextWriteOffset = slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get();
        slidingWindowService.getWindowCoreData().getWindowStartOffset().set(previousNextWriteOffset + WALUtil.BLOCK_SIZE);
        slidingWindowService.getWindowCoreData().getWindowStartOffset().set(previousNextWriteOffset + WALUtil.BLOCK_SIZE);
        return trim(previousNextWriteOffset);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        checkReadyToServe();

        if (offset >= slidingWindowService.getWindowCoreData().getWindowStartOffset().get()) {
            throw new IllegalArgumentException("failed to trim: record at offset " + offset + " has not been flushed yet");
        }

        walHeaderCoreData.updateTrimOffset(offset);
        return CompletableFuture.runAsync(() -> {
            // TODO: more beautiful
            this.walHeaderCoreData.setSlidingWindowStartOffset(slidingWindowService.getWindowCoreData().getWindowStartOffset().get());
            this.walHeaderCoreData.setSlidingWindowNextWriteOffset(slidingWindowService.getWindowCoreData().getWindowNextWriteOffset().get());
            this.walHeaderCoreData.setSlidingWindowMaxLength(slidingWindowService.getWindowCoreData().getWindowMaxLength().get());
            try {
                flushWALHeader();
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, flushWALHeaderScheduler);
    }

    private void checkReadyToServe() {
        if (!readyToServe.get()) {
            throw new IllegalStateException("WriteAheadLog is not ready to serve");
        }
    }

    static class WALHeaderCoreData {
        private final AtomicLong trimOffset2 = new AtomicLong(0);
        private final AtomicLong flushedTrimOffset = new AtomicLong(0);
        private final AtomicLong slidingWindowStartOffset5 = new AtomicLong(0);
        private final AtomicLong slidingWindowNextWriteOffset4 = new AtomicLong(0);
        private final AtomicLong slidingWindowMaxLength6 = new AtomicLong(0);
        private int magicCode0 = WAL_HEADER_MAGIC_CODE;
        private long capacity1;
        private long lastWriteTimestamp3 = System.nanoTime();
        private ShutdownType shutdownType7 = ShutdownType.UNGRACEFULLY;
        private int crc8;

        public static WALHeaderCoreData unmarshal(ByteBuffer byteBuffer) throws UnmarshalException {
            WALHeaderCoreData walHeaderCoreData = new WALHeaderCoreData();
            walHeaderCoreData.magicCode0 = byteBuffer.getInt();
            walHeaderCoreData.capacity1 = byteBuffer.getLong();
            walHeaderCoreData.trimOffset2.set(byteBuffer.getLong());
            walHeaderCoreData.lastWriteTimestamp3 = byteBuffer.getLong();
            walHeaderCoreData.slidingWindowNextWriteOffset4.set(byteBuffer.getLong());
            walHeaderCoreData.slidingWindowStartOffset5.set(byteBuffer.getLong());
            walHeaderCoreData.slidingWindowMaxLength6.set(byteBuffer.getLong());
            walHeaderCoreData.shutdownType7 = ShutdownType.fromCode(byteBuffer.getInt());
            walHeaderCoreData.crc8 = byteBuffer.getInt();

            if (walHeaderCoreData.magicCode0 != WAL_HEADER_MAGIC_CODE) {
                throw new UnmarshalException(String.format("WALHeader MagicCode not match, Recovered: [%d] expect: [%d]", walHeaderCoreData.magicCode0, WAL_HEADER_MAGIC_CODE));
            }

            ByteBuffer headerExceptCRC = walHeaderCoreData.marshalHeaderExceptCRC();
            int crc = WALUtil.crc32(headerExceptCRC, WAL_HEADER_WITHOUT_CRC_SIZE);
            if (crc != walHeaderCoreData.crc8) {
                throw new UnmarshalException(String.format("WALHeader CRC not match, Recovered: [%d] expect: [%d]", walHeaderCoreData.crc8, crc));
            }

            return walHeaderCoreData;
        }

        public long recordSectionCapacity() {
            return capacity1 - WAL_HEADER_TOTAL_CAPACITY;
        }

        public long getCapacity() {
            return capacity1;
        }

        public WALHeaderCoreData setCapacity(long capacity) {
            this.capacity1 = capacity;
            return this;
        }

        public long getSlidingWindowStartOffset() {
            return slidingWindowStartOffset5.get();
        }

        public WALHeaderCoreData setSlidingWindowStartOffset(long slidingWindowStartOffset) {
            this.slidingWindowStartOffset5.set(slidingWindowStartOffset);
            return this;
        }

        public long getTrimOffset() {
            return trimOffset2.get();
        }

        // Update the trim offset if the given trim offset is larger than the current one.
        public WALHeaderCoreData updateTrimOffset(long trimOffset) {
            long currentTrimOffset;
            do {
                currentTrimOffset = trimOffset2.get();
                if (trimOffset <= currentTrimOffset) {
                    return this;
                }
            } while (!trimOffset2.compareAndSet(currentTrimOffset, trimOffset));
            return this;
        }

        public long getFlushedTrimOffset() {
            return flushedTrimOffset.get();
        }

        public void setFlushedTrimOffset(long flushedTrimOffset) {
            this.flushedTrimOffset.set(flushedTrimOffset);
        }

        public long getLastWriteTimestamp() {
            return lastWriteTimestamp3;
        }

        public WALHeaderCoreData setLastWriteTimestamp(long lastWriteTimestamp) {
            this.lastWriteTimestamp3 = lastWriteTimestamp;
            return this;
        }

        public long getSlidingWindowNextWriteOffset() {
            return slidingWindowNextWriteOffset4.get();
        }

        public WALHeaderCoreData setSlidingWindowNextWriteOffset(long slidingWindowNextWriteOffset) {
            this.slidingWindowNextWriteOffset4.set(slidingWindowNextWriteOffset);
            return this;
        }

        public long getSlidingWindowMaxLength() {
            return slidingWindowMaxLength6.get();
        }

        public WALHeaderCoreData setSlidingWindowMaxLength(long slidingWindowMaxLength) {
            this.slidingWindowMaxLength6.set(slidingWindowMaxLength);
            return this;
        }

        public ShutdownType getShutdownType() {
            return shutdownType7;
        }

        public WALHeaderCoreData setShutdownType(ShutdownType shutdownType) {
            this.shutdownType7 = shutdownType;
            return this;
        }

        @Override
        public String toString() {
            return "WALHeader{"
                    + "magicCode=" + magicCode0
                    + ", capacity=" + capacity1
                    + ", trimOffset=" + trimOffset2
                    + ", lastWriteTimestamp=" + lastWriteTimestamp3
                    + ", nextWriteOffset=" + slidingWindowNextWriteOffset4
                    + ", slidingWindowStartOffset=" + slidingWindowStartOffset5
                    + ", slidingWindowMaxLength=" + slidingWindowMaxLength6
                    + ", shutdownType=" + shutdownType7
                    + ", crc=" + crc8
                    + '}';
        }

        private ByteBuffer marshalHeaderExceptCRC() {
            ByteBuffer byteBuffer = ByteBuffer.allocate(WAL_HEADER_SIZE);
            byteBuffer.putInt(magicCode0);
            byteBuffer.putLong(capacity1);
            byteBuffer.putLong(trimOffset2.get());
            byteBuffer.putLong(lastWriteTimestamp3);
            byteBuffer.putLong(slidingWindowNextWriteOffset4.get());
            byteBuffer.putLong(slidingWindowStartOffset5.get());
            byteBuffer.putLong(slidingWindowMaxLength6.get());
            byteBuffer.putInt(shutdownType7.getCode());

            return byteBuffer;
        }

        ByteBuffer marshal() {
            ByteBuffer byteBuffer = marshalHeaderExceptCRC();
            this.crc8 = WALUtil.crc32(byteBuffer, WAL_HEADER_WITHOUT_CRC_SIZE);
            byteBuffer.putInt(crc8);
            return byteBuffer.position(0);
        }
    }

    public static BlockWALServiceBuilder builder(String blockDevicePath, long capacity) {
        return new BlockWALServiceBuilder(blockDevicePath, capacity);
    }

    public static class BlockWALServiceBuilder {
        private final String blockDevicePath;
        private long blockDeviceCapacityWant;
        private int flushHeaderIntervalSeconds = 10;
        private int ioThreadNums = 8;
        private long slidingWindowInitialSize = 1 << 20;
        private long slidingWindowUpperLimit = 512 << 20;
        private long slidingWindowScaleUnit = 4 << 20;
        private int writeQueueCapacity = 10000;

        BlockWALServiceBuilder(String blockDevicePath, long capacity) {
            this.blockDevicePath = blockDevicePath;
            this.blockDeviceCapacityWant = capacity;
        }

        public BlockWALServiceBuilder config(Config config) {
            return this
                    .flushHeaderIntervalSeconds(config.s3WALHeaderFlushIntervalSeconds())
                    .ioThreadNums(config.s3WALThread())
                    .slidingWindowInitialSize(config.s3WALWindowInitial())
                    .slidingWindowScaleUnit(config.s3WALWindowIncrement())
                    .slidingWindowUpperLimit(config.s3WALWindowMax())
                    .writeQueueCapacity(config.s3WALQueue());
        }

        public BlockWALServiceBuilder flushHeaderIntervalSeconds(int flushHeaderIntervalSeconds) {
            this.flushHeaderIntervalSeconds = flushHeaderIntervalSeconds;
            return this;
        }

        public BlockWALServiceBuilder ioThreadNums(int ioThreadNums) {
            this.ioThreadNums = ioThreadNums;
            return this;
        }

        public BlockWALServiceBuilder slidingWindowInitialSize(long slidingWindowInitialSize) {
            this.slidingWindowInitialSize = slidingWindowInitialSize;
            return this;
        }

        public BlockWALServiceBuilder slidingWindowUpperLimit(long slidingWindowUpperLimit) {
            this.slidingWindowUpperLimit = slidingWindowUpperLimit;
            return this;
        }

        public BlockWALServiceBuilder slidingWindowScaleUnit(long slidingWindowScaleUnit) {
            this.slidingWindowScaleUnit = slidingWindowScaleUnit;
            return this;
        }

        public BlockWALServiceBuilder writeQueueCapacity(int writeQueueCapacity) {
            this.writeQueueCapacity = writeQueueCapacity;
            return this;
        }

        public BlockWALService build() {
            BlockWALService blockWALService = new BlockWALService();

            // make blockDeviceCapacityWant align to BLOCK_SIZE
            blockDeviceCapacityWant = blockDeviceCapacityWant / WALUtil.BLOCK_SIZE * WALUtil.BLOCK_SIZE;

            // make sure window size is less than capacity
            slidingWindowInitialSize = Math.min(slidingWindowInitialSize, blockDeviceCapacityWant - WAL_HEADER_TOTAL_CAPACITY);
            slidingWindowUpperLimit = Math.min(slidingWindowUpperLimit, blockDeviceCapacityWant - WAL_HEADER_TOTAL_CAPACITY);

            blockWALService.walHeaderFlushIntervalSeconds = flushHeaderIntervalSeconds;
            blockWALService.initialWindowSize = slidingWindowInitialSize;

            blockWALService.walChannel = WALChannel.WALChannelBuilder.build(blockDevicePath, blockDeviceCapacityWant);

            blockWALService.slidingWindowService = new SlidingWindowService(
                    blockWALService.walChannel,
                    ioThreadNums,
                    slidingWindowUpperLimit,
                    slidingWindowScaleUnit,
                    writeQueueCapacity
            );

            LOGGER.info("build BlockWALService: {}", this);

            return blockWALService;
        }

        @Override
        public String toString() {
            return "BlockWALServiceBuilder{"
                    + "blockDevicePath='" + blockDevicePath
                    + ", blockDeviceCapacityWant=" + blockDeviceCapacityWant
                    + ", flushHeaderIntervalSeconds=" + flushHeaderIntervalSeconds
                    + ", ioThreadNums=" + ioThreadNums
                    + ", slidingWindowInitialSize=" + slidingWindowInitialSize
                    + ", slidingWindowUpperLimit=" + slidingWindowUpperLimit
                    + ", slidingWindowScaleUnit=" + slidingWindowScaleUnit
                    + ", writeQueueCapacity=" + writeQueueCapacity
                    + '}';
        }
    }

    static class AppendResultImpl implements AppendResult {
        private final long recordOffset;
        private final CompletableFuture<CallbackResult> future;

        public AppendResultImpl(long recordOffset, CompletableFuture<CallbackResult> future) {
            this.recordOffset = recordOffset;
            this.future = future;
        }

        @Override
        public long recordOffset() {
            return recordOffset;
        }

        @Override
        public CompletableFuture<CallbackResult> future() {
            return future;
        }

        @Override
        public String toString() {
            return "AppendResultImpl{" + "recordOffset=" + recordOffset + '}';
        }
    }

    static class RecoverResultImpl implements RecoverResult {
        private final ByteBuffer record;
        private final long recordOffset;

        public RecoverResultImpl(ByteBuffer record, long recordOffset) {
            this.record = record;
            this.recordOffset = recordOffset;
        }

        @Override
        public ByteBuffer record() {
            return record;
        }

        @Override
        public long recordOffset() {
            return recordOffset;
        }

        @Override
        public String toString() {
            return "RecoverResultImpl{"
                    + "record=" + record
                    + ", recordOffset=" + recordOffset
                    + '}';
        }
    }

    class RecoverIterator implements Iterator<RecoverResult> {
        private long nextRecoverOffset;
        private RecoverResult next;

        public RecoverIterator(long nextRecoverOffset) {
            this.nextRecoverOffset = nextRecoverOffset;
        }

        @Override
        public boolean hasNext() {
            return tryReadNextRecord();
        }

        @Override
        public RecoverResult next() {
            if (!tryReadNextRecord()) {
                throw new NoSuchElementException();
            }

            RecoverResult rst = next;
            this.next = null;
            return rst;
        }

        /**
         * Try to read next record.
         *
         * @return true if read success, false if no more record. {@link #next} will be null if and only if return false.
         */
        private boolean tryReadNextRecord() {
            if (next != null) {
                return true;
            }
            do {
                try {
                    ByteBuffer nextRecordBody = readRecord(walHeaderCoreData, nextRecoverOffset);
                    next = new RecoverResultImpl(nextRecordBody, nextRecoverOffset);
                    nextRecoverOffset = WALUtil.alignLargeByBlockSize(nextRecoverOffset + RECORD_HEADER_SIZE + nextRecordBody.limit());
                    return true;
                } catch (ReadRecordException e) {
                    nextRecoverOffset = e.getJumpNextRecoverOffset();
                }
            } while (nextRecoverOffset < walHeaderCoreData.getSlidingWindowNextWriteOffset());
            return false;
        }
    }

    static class ReadRecordException extends Exception {
        long jumpNextRecoverOffset;

        public ReadRecordException(long offset, String message) {
            super(message);
            this.jumpNextRecoverOffset = offset;
        }

        public long getJumpNextRecoverOffset() {
            return jumpNextRecoverOffset;
        }
    }

    static class UnmarshalException extends Exception {
        public UnmarshalException(String message) {
            super(message);
        }
    }
}
