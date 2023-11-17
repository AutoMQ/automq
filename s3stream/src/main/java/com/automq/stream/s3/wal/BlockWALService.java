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
import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.operations.S3Operation;
import com.automq.stream.s3.metrics.stats.OperationMetricsStats;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static com.automq.stream.s3.Constants.NOOP_EPOCH;
import static com.automq.stream.s3.Constants.NOOP_NODE_ID;

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
 * <p>
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
    public static final int RECORD_HEADER_SIZE = 4 + 4 + 8 + 4 + 4;
    public static final int RECORD_HEADER_WITHOUT_CRC_SIZE = RECORD_HEADER_SIZE - 4;
    public static final int RECORD_HEADER_MAGIC_CODE = 0x87654321;
    public static final int WAL_HEADER_COUNT = 2;
    public static final int WAL_HEADER_CAPACITY = WALUtil.BLOCK_SIZE;
    public static final int WAL_HEADER_TOTAL_CAPACITY = WAL_HEADER_CAPACITY * WAL_HEADER_COUNT;
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockWALService.class);
    private final AtomicBoolean readyToServe = new AtomicBoolean(false);
    private final AtomicLong writeHeaderRoundTimes = new AtomicLong(0);
    private int walHeaderFlushIntervalSeconds;
    private long initialWindowSize;
    private ScheduledExecutorService flushWALHeaderScheduler;
    private WALChannel walChannel;
    private SlidingWindowService slidingWindowService;
    private WALHeader walHeader;
    private int nodeId = NOOP_NODE_ID;
    private long epoch = NOOP_EPOCH;

    public static BlockWALServiceBuilder builder(String blockDevicePath, long capacity) {
        return new BlockWALServiceBuilder(blockDevicePath, capacity);
    }

    private void startFlushWALHeaderScheduler() {
        this.flushWALHeaderScheduler = Threads.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("flush-wal-header-thread-%d", true), LOGGER);
        this.flushWALHeaderScheduler.scheduleAtFixedRate(() -> {
            try {
                BlockWALService.this.flushWALHeader(
                        this.slidingWindowService.getWindowCoreData().getWindowStartOffset(),
                        this.slidingWindowService.getWindowCoreData().getWindowMaxLength(),
                        this.slidingWindowService.getWindowCoreData().getWindowNextWriteOffset(),
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
        walHeader
                .setSlidingWindowStartOffset(windowStartOffset)
                .setSlidingWindowMaxLength(windowMaxLength)
                .setSlidingWindowNextWriteOffset(windowNextWriteOffset)
                .setShutdownType(shutdownType);
        flushWALHeader();
    }

    private synchronized void flushWALHeader() throws IOException {
        long position = writeHeaderRoundTimes.getAndIncrement() % WAL_HEADER_COUNT * WAL_HEADER_CAPACITY;
        try {
            walHeader.setLastWriteTimestamp(System.nanoTime());
            long trimOffset = walHeader.getTrimOffset();
            ByteBuf buf = walHeader.marshal();
            this.walChannel.writeAndFlush(buf, position);
            buf.release();
            walHeader.setFlushedTrimOffset(trimOffset);
            LOGGER.debug("WAL header flushed, position: {}, header: {}", position, walHeader);
        } catch (IOException e) {
            LOGGER.error("failed to flush WAL header, position: {}, header: {}", position, walHeader, e);
            throw e;
        }
    }

    /**
     * Try to read a record at the given offset.
     * The returned record should be released by the caller.
     *
     * @throws ReadRecordException if the record is not found or the record is corrupted
     */
    private ByteBuf readRecord(long recordSectionCapacity, long recoverStartOffset) throws ReadRecordException {
        final ByteBuf recordHeader = DirectByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        SlidingWindowService.RecordHeaderCoreData readRecordHeader;
        try {
            readRecordHeader = parseRecordHeader(recordSectionCapacity, recoverStartOffset, recordHeader);
        } finally {
            recordHeader.release();
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        ByteBuf recordBody = DirectByteBufAlloc.byteBuffer(recordBodyLength);
        try {
            parseRecordBody(recordSectionCapacity, recoverStartOffset, readRecordHeader, recordBody);
        } catch (ReadRecordException e) {
            recordBody.release();
            throw e;
        }

        return recordBody;
    }

    private SlidingWindowService.RecordHeaderCoreData parseRecordHeader(long recordSectionCapacity, long recoverStartOffset, ByteBuf recordHeader) throws ReadRecordException {
        final long position = WALUtil.recordOffsetToPosition(recoverStartOffset, recordSectionCapacity, WAL_HEADER_TOTAL_CAPACITY);
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

        SlidingWindowService.RecordHeaderCoreData readRecordHeader = SlidingWindowService.RecordHeaderCoreData.unmarshal(recordHeader);
        if (readRecordHeader.getMagicCode() != RECORD_HEADER_MAGIC_CODE) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("magic code mismatch: expected %d, actual %d, recoverStartOffset: %d", RECORD_HEADER_MAGIC_CODE, readRecordHeader.getMagicCode(), recoverStartOffset)
            );
        }

        int recordHeaderCRC = readRecordHeader.getRecordHeaderCRC();
        int calculatedRecordHeaderCRC = WALUtil.crc32(recordHeader, RECORD_HEADER_WITHOUT_CRC_SIZE);
        if (recordHeaderCRC != calculatedRecordHeaderCRC) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("record header crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordHeaderCRC, recordHeaderCRC, recoverStartOffset)
            );
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        if (recordBodyLength <= 0) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("invalid record body length: %d, recoverStartOffset: %d", recordBodyLength, recoverStartOffset)
            );
        }

        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        if (recordBodyOffset != recoverStartOffset + RECORD_HEADER_SIZE) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset),
                    String.format("invalid record body offset: expected %d, actual %d, recoverStartOffset: %d", recoverStartOffset + RECORD_HEADER_SIZE, recordBodyOffset, recoverStartOffset)
            );
        }
        return readRecordHeader;
    }

    private void parseRecordBody(long recordSectionCapacity, long recoverStartOffset, SlidingWindowService.RecordHeaderCoreData readRecordHeader, ByteBuf recordBody) throws ReadRecordException {
        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        try {
            int read = walChannel.read(recordBody, WALUtil.recordOffsetToPosition(recordBodyOffset, recordSectionCapacity, WAL_HEADER_TOTAL_CAPACITY));
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

        int recordBodyCRC = readRecordHeader.getRecordBodyCRC();
        int calculatedRecordBodyCRC = WALUtil.crc32(recordBody);
        if (recordBodyCRC != calculatedRecordBodyCRC) {
            throw new ReadRecordException(
                    WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                    String.format("record body crc mismatch: expected %d, actual %d, recoverStartOffset: %d", calculatedRecordBodyCRC, recordBodyCRC, recoverStartOffset)
            );
        }
    }

    private WALHeader recoverEntireWALAndCorrectWALHeader(WALHeader paramWALHeader) {
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
        final long recordSectionCapacity = paramWALHeader.recordSectionCapacity();

        long nextRecoverStartOffset;
        long meetIllegalRecordTimes = 0;
        LOGGER.info("start to recover from ungraceful shutdown, recoverStartOffset: {}, recoverRemainingBytes: {}", recoverStartOffset, recoverRemainingBytes);
        do {
            try {
                ByteBuf body = readRecord(recordSectionCapacity, recoverStartOffset);
                nextRecoverStartOffset = recoverStartOffset + RECORD_HEADER_SIZE + body.readableBytes();
                body.release();
            } catch (ReadRecordException e) {
                nextRecoverStartOffset = e.getJumpNextRecoverOffset();
                LOGGER.debug("failed to read record, try next, recoverStartOffset: {}, meetIllegalRecordTimes: {}, recoverRemainingBytes: {}, error: {}",
                        recoverStartOffset, meetIllegalRecordTimes, recoverRemainingBytes, e.getMessage());
                meetIllegalRecordTimes++;
            }

            recoverRemainingBytes -= nextRecoverStartOffset - recoverStartOffset;
            recoverStartOffset = nextRecoverStartOffset;
        } while (recoverRemainingBytes > 0);
        long windowInitOffset = WALUtil.alignLargeByBlockSize(nextRecoverStartOffset);
        paramWALHeader.setSlidingWindowStartOffset(windowInitOffset).setSlidingWindowNextWriteOffset(windowInitOffset);

        LOGGER.info("recovered from ungraceful shutdown, WALHeader: {}", paramWALHeader);
        return paramWALHeader;
    }

    private void recoverWALHeader() throws IOException {
        WALHeader walHeaderAvailable = null;

        for (int i = 0; i < WAL_HEADER_COUNT; i++) {
            ByteBuf buf = DirectByteBufAlloc.byteBuffer(WALHeader.WAL_HEADER_SIZE);
            try {
                int read = walChannel.read(buf, i * WAL_HEADER_CAPACITY);
                if (read != WALHeader.WAL_HEADER_SIZE) {
                    continue;
                }
                WALHeader walHeader = WALHeader.unmarshal(buf);
                if (walHeaderAvailable == null || walHeaderAvailable.getLastWriteTimestamp() < walHeader.getLastWriteTimestamp()) {
                    walHeaderAvailable = walHeader;
                }
            } catch (IOException | UnmarshalException ignored) {
                // failed to parse WALHeader, ignore
            } finally {
                buf.release();
            }
        }

        if (walHeaderAvailable != null) {
            walHeader = recoverEntireWALAndCorrectWALHeader(walHeaderAvailable);
        } else {
            walHeader = new WALHeader()
                    .setCapacity(walChannel.capacity())
                    .setSlidingWindowMaxLength(initialWindowSize)
                    .setShutdownType(ShutdownType.UNGRACEFULLY);
            LOGGER.info("no valid WALHeader found, create new WALHeader: {}", walHeader);
        }
        if (nodeId != NOOP_NODE_ID) {
            walHeader.setNodeId(nodeId);
            walHeader.setEpoch(epoch);
        }

        flushWALHeader();
        slidingWindowService.resetWindowWhenRecoverOver(
                walHeader.getSlidingWindowStartOffset(),
                walHeader.getSlidingWindowNextWriteOffset(),
                walHeader.getSlidingWindowMaxLength()
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
                    slidingWindowService.getWindowCoreData().getWindowStartOffset(),
                    slidingWindowService.getWindowCoreData().getWindowMaxLength(),
                    slidingWindowService.getWindowCoreData().getWindowNextWriteOffset(),
                    gracefulShutdown ? ShutdownType.GRACEFULLY : ShutdownType.UNGRACEFULLY
            );
        } catch (IOException e) {
            LOGGER.error("failed to flush WALHeader when shutdown gracefully", e);
        }

        walChannel.close();

        LOGGER.info("block WAL service shutdown gracefully: {}, cost: {} ms", gracefulShutdown, stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public WALMetadata metadata() {
        return new WALMetadata(walHeader.getNodeId(), walHeader.getEpoch());
    }

    @Override
    public AppendResult append(ByteBuf buf, int crc) throws OverCapacityException {
        try {
            return append0(buf, crc);
        } catch (OverCapacityException ex) {
            buf.release();
            OperationMetricsStats.getCounter(S3Operation.APPEND_STORAGE_WAL_FULL).inc();
            throw ex;
        }
    }

    public AppendResult append0(ByteBuf body, int crc) throws OverCapacityException {
        TimerUtil timerUtil = new TimerUtil();
        checkReadyToServe();

        final long recordSize = RECORD_HEADER_SIZE + body.readableBytes();
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        long expectedWriteOffset;

        Lock lock = slidingWindowService.getBlockLock();
        lock.lock();
        try {
            Block block = slidingWindowService.getCurrentBlockLocked();
            expectedWriteOffset = block.addRecord(recordSize, (offset) -> record(body, crc, offset), appendResultFuture);
            if (expectedWriteOffset < 0) {
                // this block is full, create a new one
                block = slidingWindowService.sealAndNewBlockLocked(block, recordSize, walHeader.getFlushedTrimOffset(), walHeader.getCapacity() - WAL_HEADER_TOTAL_CAPACITY);
                expectedWriteOffset = block.addRecord(recordSize, (offset) -> record(body, crc, offset), appendResultFuture);
            }
        } finally {
            lock.unlock();
        }
        slidingWindowService.tryWriteBlock();

        final AppendResult appendResult = new AppendResultImpl(expectedWriteOffset, appendResultFuture);
        appendResult.future().whenComplete((nil, ex) -> OperationMetricsStats.getHistogram(S3Operation.APPEND_STORAGE_WAL).update(timerUtil.elapsedAs(TimeUnit.NANOSECONDS)));
        return appendResult;
    }

    private ByteBuf recordHeader(ByteBuf body, int crc, long start) {
        return new SlidingWindowService.RecordHeaderCoreData()
                .setMagicCode(RECORD_HEADER_MAGIC_CODE)
                .setRecordBodyLength(body.readableBytes())
                .setRecordBodyOffset(start + RECORD_HEADER_SIZE)
                .setRecordBodyCRC(crc)
                .marshal();
    }

    private ByteBuf record(ByteBuf body, int crc, long start) {
        CompositeByteBuf record = DirectByteBufAlloc.compositeByteBuffer();
        crc = 0 == crc ? WALUtil.crc32(body) : crc;
        record.addComponents(true, recordHeader(body, crc, start), body);
        return record;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        checkReadyToServe();

        long trimmedOffset = walHeader.getTrimOffset();
        long recoverStartOffset = trimmedOffset;
        if (recoverStartOffset < 0) {
            recoverStartOffset = 0;
        }
        return new RecoverIterator(recoverStartOffset, trimmedOffset);
    }

    @Override
    public CompletableFuture<Void> reset() {
        checkReadyToServe();

        long previousNextWriteOffset = slidingWindowService.getWindowCoreData().getWindowNextWriteOffset();
        slidingWindowService.resetWindow(previousNextWriteOffset + WALUtil.BLOCK_SIZE);
        LOGGER.info("reset sliding window and trim WAL to offset: {}", previousNextWriteOffset);
        return trim(previousNextWriteOffset);
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        checkReadyToServe();

        if (offset >= slidingWindowService.getWindowCoreData().getWindowStartOffset()) {
            throw new IllegalArgumentException("failed to trim: record at offset " + offset + " has not been flushed yet");
        }

        walHeader.updateTrimOffset(offset);
        return CompletableFuture.runAsync(() -> {
            // TODO: more beautiful
            this.walHeader.setSlidingWindowStartOffset(slidingWindowService.getWindowCoreData().getWindowStartOffset());
            this.walHeader.setSlidingWindowNextWriteOffset(slidingWindowService.getWindowCoreData().getWindowNextWriteOffset());
            this.walHeader.setSlidingWindowMaxLength(slidingWindowService.getWindowCoreData().getWindowMaxLength());
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

    private SlidingWindowService.WALHeaderFlusher flusher() {
        return windowMaxLength -> flushWALHeader(
                slidingWindowService.getWindowCoreData().getWindowStartOffset(),
                windowMaxLength,
                slidingWindowService.getWindowCoreData().getWindowNextWriteOffset(),
                ShutdownType.UNGRACEFULLY
        );
    }

    public static BlockWALServiceBuilder builder(String path) {
        return new BlockWALServiceBuilder(path);
    }

    public static class BlockWALServiceBuilder {
        private final String blockDevicePath;
        private long blockDeviceCapacityWant;
        private boolean direct = false;
        private int flushHeaderIntervalSeconds = 10;
        private int ioThreadNums = 8;
        private long slidingWindowInitialSize = 1 << 20;
        private long slidingWindowUpperLimit = 512 << 20;
        private long slidingWindowScaleUnit = 4 << 20;
        private long blockSoftLimit = 1 << 18; // 256KiB
        private int writeRateLimit = 3000;
        private int nodeId = NOOP_NODE_ID;
        private long epoch = NOOP_EPOCH;
        private boolean readOnly;

        public BlockWALServiceBuilder(String blockDevicePath, long capacity) {
            this.blockDevicePath = blockDevicePath;
            this.blockDeviceCapacityWant = capacity;
        }

        public BlockWALServiceBuilder(String blockDevicePath) {
            this.blockDevicePath = blockDevicePath;
        }

        public BlockWALServiceBuilder config(Config config) {
            return this
                    .flushHeaderIntervalSeconds(config.walHeaderFlushIntervalSeconds())
                    .ioThreadNums(config.walThread())
                    .slidingWindowInitialSize(config.walWindowInitial())
                    .slidingWindowScaleUnit(config.walWindowIncrement())
                    .slidingWindowUpperLimit(config.walWindowMax())
                    .blockSoftLimit(config.walBlockSoftLimit())
                    .writeRateLimit(config.walWriteRateLimit())
                    .nodeId(config.nodeId())
                    .epoch(config.nodeEpoch());
        }

        public BlockWALServiceBuilder direct(boolean direct) {
            this.direct = direct;
            return this;
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

        public BlockWALServiceBuilder blockSoftLimit(long blockSoftLimit) {
            this.blockSoftLimit = blockSoftLimit;
            return this;
        }

        public BlockWALServiceBuilder writeRateLimit(int writeRateLimit) {
            this.writeRateLimit = writeRateLimit;
            return this;
        }

        public BlockWALServiceBuilder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public BlockWALServiceBuilder epoch(long epoch) {
            this.epoch = epoch;
            return this;
        }

        public BlockWALServiceBuilder readOnly() {
            this.readOnly = true;
            return this;
        }

        public BlockWALService build() {
            // TODO: BlockWALService support readOnly mode
            // TODO: readOnly mode: throw WALNotInitializedException when WAL is not initialized
            BlockWALService blockWALService = new BlockWALService();

            // make blockDeviceCapacityWant align to BLOCK_SIZE
            blockDeviceCapacityWant = blockDeviceCapacityWant / WALUtil.BLOCK_SIZE * WALUtil.BLOCK_SIZE;

            // make sure window size is less than capacity
            slidingWindowInitialSize = Math.min(slidingWindowInitialSize, blockDeviceCapacityWant - WAL_HEADER_TOTAL_CAPACITY);
            slidingWindowUpperLimit = Math.min(slidingWindowUpperLimit, blockDeviceCapacityWant - WAL_HEADER_TOTAL_CAPACITY);

            blockWALService.walHeaderFlushIntervalSeconds = flushHeaderIntervalSeconds;
            blockWALService.initialWindowSize = slidingWindowInitialSize;

            blockWALService.walChannel = WALChannel.builder(blockDevicePath).capacity(blockDeviceCapacityWant).direct(direct).readOnly(readOnly).build();

            blockWALService.slidingWindowService = new SlidingWindowService(
                    blockWALService.walChannel,
                    ioThreadNums,
                    slidingWindowUpperLimit,
                    slidingWindowScaleUnit,
                    blockSoftLimit,
                    writeRateLimit,
                    blockWALService.flusher()
            );

            if (nodeId != NOOP_NODE_ID) {
                blockWALService.nodeId = nodeId;
                blockWALService.epoch = epoch;
            }

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
                    + ", blockSoftLimit=" + blockSoftLimit
                    + ", writeRateLimit=" + writeRateLimit
                    + '}';
        }
    }

    record AppendResultImpl(long recordOffset, CompletableFuture<CallbackResult> future) implements AppendResult {

        @Override
        public String toString() {
            return "AppendResultImpl{" + "recordOffset=" + recordOffset + '}';
        }
    }

    record RecoverResultImpl(ByteBuf record, long recordOffset) implements RecoverResult {

        @Override
        public String toString() {
            return "RecoverResultImpl{"
                    + "record=" + record
                    + ", recordOffset=" + recordOffset
                    + '}';
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

    class RecoverIterator implements Iterator<RecoverResult> {
        private final long skipRecordAtOffset;
        private long nextRecoverOffset;
        private RecoverResult next;

        public RecoverIterator(long nextRecoverOffset, long skipRecordAtOffset) {
            this.nextRecoverOffset = nextRecoverOffset;
            this.skipRecordAtOffset = skipRecordAtOffset;
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
                    boolean skip = nextRecoverOffset == skipRecordAtOffset;
                    ByteBuf nextRecordBody = readRecord(walHeader.recordSectionCapacity(), nextRecoverOffset);
                    RecoverResultImpl recoverResult = new RecoverResultImpl(nextRecordBody, nextRecoverOffset);
                    nextRecoverOffset += RECORD_HEADER_SIZE + nextRecordBody.readableBytes();
                    if (skip) {
                        nextRecordBody.release();
                        continue;
                    }
                    next = recoverResult;
                    return true;
                } catch (ReadRecordException e) {
                    nextRecoverOffset = e.getJumpNextRecoverOffset();
                }
            } while (nextRecoverOffset < walHeader.getSlidingWindowNextWriteOffset());
            return false;
        }
    }
}
