/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.trace.TraceUtils;
import com.automq.stream.s3.trace.context.TraceContext;
import com.automq.stream.s3.wal.util.WALCachedChannel;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.Constants.CAPACITY_NOT_SET;
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
 * The WAL header is used to record the meta information of the WAL, and is used to recover the WAL when the service is restarted.
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
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean resetFinished = new AtomicBoolean(false);
    private final AtomicLong writeHeaderRoundTimes = new AtomicLong(0);
    private final ExecutorService walHeaderFlusher = Threads.newFixedThreadPool(1, ThreadUtils.createThreadFactory("flush-wal-header-thread-%d", true), LOGGER);
    private long initialWindowSize;
    private WALCachedChannel walChannel;
    private SlidingWindowService slidingWindowService;
    private WALHeader walHeader;
    private boolean recoveryMode;
    private boolean firstStart;
    private int nodeId = NOOP_NODE_ID;
    private long epoch = NOOP_EPOCH;
    /**
     * The offset at which the recovery is complete. It is safe to write records at this offset.
     * It is always aligned to the {@link WALUtil#BLOCK_SIZE}.
     */
    private long recoveryCompleteOffset = -1;

    private BlockWALService() {
    }

    /**
     * A protected constructor for testing purpose.
     */
    protected BlockWALService(BlockWALServiceBuilder builder) {
        BlockWALService that = builder.build();
        this.initialWindowSize = that.initialWindowSize;
        this.walChannel = that.walChannel;
        this.slidingWindowService = that.slidingWindowService;
        this.walHeader = that.walHeader;
        this.recoveryMode = that.recoveryMode;
        this.nodeId = that.nodeId;
        this.epoch = that.epoch;
    }

    public static BlockWALServiceBuilder builder(String path, long capacity) {
        return new BlockWALServiceBuilder(path, capacity);
    }

    public static BlockWALServiceBuilder recoveryBuilder(String path) {
        return new BlockWALServiceBuilder(path);
    }

    private void flushWALHeader(ShutdownType shutdownType) {
        walHeader.setShutdownType(shutdownType);
        flushWALHeader();
    }

    private synchronized void flushWALHeader() {
        long position = writeHeaderRoundTimes.getAndIncrement() % WAL_HEADER_COUNT * WAL_HEADER_CAPACITY;
        walHeader.setLastWriteTimestamp(System.nanoTime());
        long trimOffset = walHeader.getTrimOffset();
        ByteBuf buf = walHeader.marshal();
        this.walChannel.retryWriteAndFlush(buf, position);
        buf.release();
        walHeader.updateFlushedTrimOffset(trimOffset);
        LOGGER.debug("WAL header flushed, position: {}, header: {}", position, walHeader);
    }

    /**
     * Try to read a record at the given offset.
     * The returned record should be released by the caller.
     *
     * @throws ReadRecordException if the record is not found or the record is corrupted
     */
    private ByteBuf readRecord(long recoverStartOffset,
        Function<Long, Long> logicalToPhysical) throws ReadRecordException {
        final ByteBuf recordHeader = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
        SlidingWindowService.RecordHeaderCoreData readRecordHeader;
        try {
            readRecordHeader = parseRecordHeader(recoverStartOffset, recordHeader, logicalToPhysical);
        } finally {
            recordHeader.release();
        }

        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        ByteBuf recordBody = ByteBufAlloc.byteBuffer(recordBodyLength);
        try {
            parseRecordBody(recoverStartOffset, readRecordHeader, recordBody, logicalToPhysical);
        } catch (ReadRecordException e) {
            recordBody.release();
            throw e;
        }

        return recordBody;
    }

    private SlidingWindowService.RecordHeaderCoreData parseRecordHeader(long recoverStartOffset, ByteBuf recordHeader,
        Function<Long, Long> logicalToPhysical) throws ReadRecordException {
        final long position = logicalToPhysical.apply(recoverStartOffset);
        int read = walChannel.retryRead(recordHeader, position);
        if (read != RECORD_HEADER_SIZE) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset),
                String.format("failed to read record header: expected %d bytes, actual %d bytes, recoverStartOffset: %d", RECORD_HEADER_SIZE, read, recoverStartOffset)
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

    private void parseRecordBody(long recoverStartOffset, SlidingWindowService.RecordHeaderCoreData readRecordHeader,
        ByteBuf recordBody, Function<Long, Long> logicalToPhysical) throws ReadRecordException {
        long recordBodyOffset = readRecordHeader.getRecordBodyOffset();
        int recordBodyLength = readRecordHeader.getRecordBodyLength();
        long position = logicalToPhysical.apply(recordBodyOffset);
        int read = walChannel.retryRead(recordBody, position);
        if (read != recordBodyLength) {
            throw new ReadRecordException(
                WALUtil.alignNextBlock(recoverStartOffset + RECORD_HEADER_SIZE + recordBodyLength),
                String.format("failed to read record body: expected %d bytes, actual %d bytes, recoverStartOffset: %d", recordBodyLength, read, recoverStartOffset)
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

    @Override
    public WriteAheadLog start() throws IOException {
        if (started.get()) {
            LOGGER.warn("block WAL service already started");
            return this;
        }
        StopWatch stopWatch = StopWatch.createStarted();

        walChannel.open(channel -> Optional.ofNullable(tryReadWALHeader(walChannel))
            .map(WALHeader::getCapacity)
            .orElse(null));

        WALHeader header = tryReadWALHeader(walChannel);
        if (null == header) {
            assert !recoveryMode;
            header = newWALHeader();
            firstStart = true;
            LOGGER.info("no available WALHeader, create a new one: {}", header);
        } else {
            LOGGER.info("read WALHeader from WAL: {}", header);
        }
        walHeaderReady(header);

        started.set(true);
        LOGGER.info("block WAL service started, cost: {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
        return this;
    }

    private void registerMetrics() {
        S3StreamMetricsManager.registerDeltaWalOffsetSupplier(() -> {
            try {
                return this.getCurrentStartOffset();
            } catch (Exception e) {
                LOGGER.error("failed to get current start offset", e);
                return 0L;
            }
        }, () -> walHeader.getFlushedTrimOffset());
    }

    private long getCurrentStartOffset() {
        Lock lock = slidingWindowService.getBlockLock();
        lock.lock();
        try {
            Block block = slidingWindowService.getCurrentBlockLocked();
            return block.startOffset() + block.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Protected method for testing purpose.
     */
    protected WALHeader tryReadWALHeader() {
        return tryReadWALHeader(walChannel);
    }

    /**
     * Try to read the header from WAL, return the latest one.
     */
    private WALHeader tryReadWALHeader(WALChannel walChannel) {
        WALHeader header = null;
        for (int i = 0; i < WAL_HEADER_COUNT; i++) {
            ByteBuf buf = ByteBufAlloc.byteBuffer(WALHeader.WAL_HEADER_SIZE);
            try {
                int read = walChannel.retryRead(buf, i * WAL_HEADER_CAPACITY);
                if (read != WALHeader.WAL_HEADER_SIZE) {
                    continue;
                }
                WALHeader tmpHeader = WALHeader.unmarshal(buf);
                if (header == null || header.getLastWriteTimestamp() < tmpHeader.getLastWriteTimestamp()) {
                    header = tmpHeader;
                }
            } catch (UnmarshalException ignored) {
                // failed to parse WALHeader, ignore
            } finally {
                buf.release();
            }
        }
        return header;
    }

    private WALHeader newWALHeader() {
        return new WALHeader(walChannel.capacity(), initialWindowSize);
    }

    private void walHeaderReady(WALHeader header) {
        if (nodeId != NOOP_NODE_ID) {
            header.setNodeId(nodeId);
            header.setEpoch(epoch);
        }
        this.walHeader = header;
        flushWALHeader();
    }

    @Override
    public void shutdownGracefully() {
        StopWatch stopWatch = StopWatch.createStarted();

        if (!started.getAndSet(false)) {
            LOGGER.warn("block WAL service already shutdown or not started yet");
            return;
        }
        walHeaderFlusher.shutdown();
        try {
            if (!walHeaderFlusher.awaitTermination(5, TimeUnit.SECONDS)) {
                walHeaderFlusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            walHeaderFlusher.shutdownNow();
        }

        boolean gracefulShutdown = Optional.ofNullable(slidingWindowService)
            .map(s -> s.shutdown(1, TimeUnit.DAYS))
            .orElse(true);
        flushWALHeader(gracefulShutdown ? ShutdownType.GRACEFULLY : ShutdownType.UNGRACEFULLY);

        walChannel.close();

        LOGGER.info("block WAL service shutdown gracefully: {}, cost: {} ms", gracefulShutdown, stopWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public WALMetadata metadata() {
        checkStarted();
        return new WALMetadata(walHeader.getNodeId(), walHeader.getEpoch());
    }

    @Override
    public AppendResult append(TraceContext context, ByteBuf buf, int crc) throws OverCapacityException {
        // get current method name
        TraceContext.Scope scope = TraceUtils.createAndStartSpan(context, "BlockWALService::append");
        final long startTime = System.nanoTime();
        try {
            AppendResult result = append0(buf, crc);
            result.future().whenComplete((nil, ex) -> TraceUtils.endSpan(scope, ex));
            return result;
        } catch (OverCapacityException ex) {
            buf.release();
            StorageOperationStats.getInstance().appendWALFullStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
            TraceUtils.endSpan(scope, ex);
            throw ex;
        }
    }

    private AppendResult append0(ByteBuf body, int crc) throws OverCapacityException {
        final long startTime = System.nanoTime();
        checkStarted();
        checkWriteMode();
        checkResetFinished();

        final long recordSize = RECORD_HEADER_SIZE + body.readableBytes();
        final CompletableFuture<AppendResult.CallbackResult> appendResultFuture = new CompletableFuture<>();
        long expectedWriteOffset;

        Lock lock = slidingWindowService.getBlockLock();
        lock.lock();
        try {
            Block block = slidingWindowService.getCurrentBlockLocked();
            expectedWriteOffset = block.addRecord(recordSize, offset -> record(body, crc, offset), appendResultFuture);
            if (expectedWriteOffset < 0) {
                // this block is full, create a new one
                block = slidingWindowService.sealAndNewBlockLocked(block, recordSize, walHeader.getFlushedTrimOffset(), walHeader.getCapacity() - WAL_HEADER_TOTAL_CAPACITY);
                expectedWriteOffset = block.addRecord(recordSize, offset -> record(body, crc, offset), appendResultFuture);
            }
        } finally {
            lock.unlock();
        }
        slidingWindowService.tryWriteBlock();

        final AppendResult appendResult = new AppendResultImpl(expectedWriteOffset, appendResultFuture);
        appendResult.future().whenComplete((nil, ex) -> StorageOperationStats.getInstance().appendWALCompleteStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS)));
        StorageOperationStats.getInstance().appendWALBeforeStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
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
        CompositeByteBuf record = ByteBufAlloc.compositeByteBuffer();
        crc = 0 == crc ? WALUtil.crc32(body) : crc;
        record.addComponents(true, recordHeader(body, crc, start), body);
        return record;
    }

    @Override
    public Iterator<RecoverResult> recover() {
        checkStarted();
        if (firstStart) {
            recoveryCompleteOffset = 0;
            return Collections.emptyIterator();
        }

        long trimmedOffset = walHeader.getTrimOffset();
        long recoverStartOffset = trimmedOffset;
        if (recoverStartOffset < 0) {
            recoverStartOffset = 0;
        }
        long windowLength = walHeader.getSlidingWindowMaxLength();
        return new RecoverIterator(recoverStartOffset, windowLength, trimmedOffset);
    }

    @Override
    public CompletableFuture<Void> reset() {
        checkStarted();
        checkRecoverFinished();

        if (!recoveryMode) {
            // in recovery mode, no need to start sliding window service
            slidingWindowService.start(walHeader.getAtomicSlidingWindowMaxLength(), recoveryCompleteOffset);
        }
        LOGGER.info("reset sliding window to offset: {}", recoveryCompleteOffset);
        CompletableFuture<Void> cf = trim(recoveryCompleteOffset - 1, true)
            .thenRun(() -> resetFinished.set(true));

        if (!recoveryMode) {
            // Only register metrics when not in recovery mode
            return cf.thenRun(this::registerMetrics);
        }
        return cf;
    }

    @Override
    public CompletableFuture<Void> trim(long offset) {
        return trim(offset, false);
    }

    private CompletableFuture<Void> trim(long offset, boolean internal) {
        checkStarted();
        if (!internal) {
            checkWriteMode();
            checkResetFinished();
            if (offset >= slidingWindowService.getWindowCoreData().getStartOffset()) {
                throw new IllegalArgumentException("failed to trim: record at offset " + offset + " has not been flushed yet");
            }
        }

        walHeader.updateTrimOffset(offset);
        return CompletableFuture.runAsync(this::flushWALHeader, walHeaderFlusher);
    }

    private void checkStarted() {
        if (!started.get()) {
            throw new IllegalStateException("WriteAheadLog has not been started yet");
        }
    }

    private void checkWriteMode() {
        if (recoveryMode) {
            throw new IllegalStateException("WriteAheadLog is in recovery mode");
        }
    }

    private void checkRecoverFinished() {
        if (recoveryCompleteOffset < 0) {
            throw new IllegalStateException("WriteAheadLog has not been completely recovered yet");
        }
    }

    private void checkResetFinished() {
        if (!resetFinished.get()) {
            throw new IllegalStateException("WriteAheadLog has not been reset yet");
        }
    }

    private SlidingWindowService.WALHeaderFlusher flusher() {
        return () -> flushWALHeader(ShutdownType.UNGRACEFULLY);
    }

    public static class BlockWALServiceBuilder {
        private final String blockDevicePath;
        private long blockDeviceCapacityWant = CAPACITY_NOT_SET;
        private Boolean direct = null;
        private int initBufferSize = 1 << 20; // 1MiB
        private int maxBufferSize = 1 << 24; // 16MiB
        private int ioThreadNums = 8;
        private long slidingWindowInitialSize = 1 << 20; // 1MiB
        private long slidingWindowUpperLimit = 1 << 29; // 512MiB
        private long slidingWindowScaleUnit = 1 << 22; // 4MiB
        private long blockSoftLimit = 1 << 18; // 256KiB
        private int writeRateLimit = 3000;
        private int nodeId = NOOP_NODE_ID;
        private long epoch = NOOP_EPOCH;
        private boolean recoveryMode = false;

        public BlockWALServiceBuilder(String blockDevicePath, long capacity) {
            this.blockDevicePath = blockDevicePath;
            this.blockDeviceCapacityWant = capacity;
        }

        public BlockWALServiceBuilder(String blockDevicePath) {
            this.blockDevicePath = blockDevicePath;
            this.recoveryMode = true;
        }

        public BlockWALServiceBuilder capacity(long capacity) {
            this.blockDeviceCapacityWant = capacity;
            return this;
        }

        public BlockWALServiceBuilder config(Config config) {
            return this
                .capacity(config.walCapacity())
                .initBufferSize(config.walInitBufferSize())
                .maxBufferSize(config.walMaxBufferSize())
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

        public BlockWALServiceBuilder initBufferSize(int initBufferSize) {
            this.initBufferSize = initBufferSize;
            return this;
        }

        public BlockWALServiceBuilder maxBufferSize(int maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
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

        public BlockWALService build() {
            if (recoveryMode) {
                assert blockDeviceCapacityWant == CAPACITY_NOT_SET;
                assert nodeId == NOOP_NODE_ID;
                assert epoch == NOOP_EPOCH;
            } else {
                // make blockDeviceCapacityWant align to BLOCK_SIZE
                blockDeviceCapacityWant = blockDeviceCapacityWant / WALUtil.BLOCK_SIZE * WALUtil.BLOCK_SIZE;
            }

            BlockWALService blockWALService = new BlockWALService();

            WALChannel.WALChannelBuilder walChannelBuilder = WALChannel.builder(blockDevicePath)
                .capacity(blockDeviceCapacityWant)
                .initBufferSize(initBufferSize)
                .maxBufferSize(maxBufferSize)
                .recoveryMode(recoveryMode);
            if (direct != null) {
                walChannelBuilder.direct(direct);
            }
            WALChannel channel = walChannelBuilder.build();
            blockWALService.walChannel = WALCachedChannel.of(channel);
            if (!blockWALService.walChannel.useDirectIO()) {
                LOGGER.warn("block wal not using direct IO");
            }

            if (!recoveryMode) {
                // in recovery mode, no need to create sliding window service
                // make sure window size is less than capacity
                slidingWindowInitialSize = Math.min(slidingWindowInitialSize, blockDeviceCapacityWant - WAL_HEADER_TOTAL_CAPACITY);
                slidingWindowUpperLimit = Math.min(slidingWindowUpperLimit, blockDeviceCapacityWant - WAL_HEADER_TOTAL_CAPACITY);
                blockWALService.initialWindowSize = slidingWindowInitialSize;
                blockWALService.slidingWindowService = new SlidingWindowService(
                    channel,
                    ioThreadNums,
                    slidingWindowUpperLimit,
                    slidingWindowScaleUnit,
                    blockSoftLimit,
                    writeRateLimit,
                    blockWALService.flusher()
                );
            }

            blockWALService.recoveryMode = recoveryMode;

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
                + ", direct=" + direct
                + ", initBufferSize=" + initBufferSize
                + ", maxBufferSize=" + maxBufferSize
                + ", ioThreadNums=" + ioThreadNums
                + ", slidingWindowInitialSize=" + slidingWindowInitialSize
                + ", slidingWindowUpperLimit=" + slidingWindowUpperLimit
                + ", slidingWindowScaleUnit=" + slidingWindowScaleUnit
                + ", blockSoftLimit=" + blockSoftLimit
                + ", writeRateLimit=" + writeRateLimit
                + ", nodeId=" + nodeId
                + ", epoch=" + epoch
                + ", recoveryMode=" + recoveryMode
                + '}';
        }
    }

    static final class AppendResultImpl implements AppendResult {
        private final long recordOffset;
        private final CompletableFuture<CallbackResult> future;

        AppendResultImpl(long recordOffset, CompletableFuture<CallbackResult> future) {
            this.recordOffset = recordOffset;
            this.future = future;
        }

        @Override
        public String toString() {
            return "AppendResultImpl{" + "recordOffset=" + recordOffset + '}';
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
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (AppendResultImpl) obj;
            return this.recordOffset == that.recordOffset &&
                Objects.equals(this.future, that.future);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordOffset, future);
        }

    }

    static class RecoverResultImpl implements RecoverResult {
        private final ByteBuf record;
        private final long recordOffset;

        RecoverResultImpl(ByteBuf record, long recordOffset) {
            this.record = record;
            this.recordOffset = recordOffset;
        }

        @Override
        public String toString() {
            return "RecoverResultImpl{"
                + "record=" + record
                + ", recordOffset=" + recordOffset
                + '}';
        }

        @Override
        public ByteBuf record() {
            return record;
        }

        @Override
        public long recordOffset() {
            return recordOffset;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (RecoverResultImpl) obj;
            return Objects.equals(this.record, that.record) &&
                this.recordOffset == that.recordOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(record, recordOffset);
        }

    }

    /**
     * Only used for testing purpose.
     */
    protected static class InvalidRecoverResult extends RecoverResultImpl {
        private final String detail;

        InvalidRecoverResult(long recordOffset, String detail) {
            super(ByteBufAlloc.byteBuffer(0), recordOffset);
            this.detail = detail;
        }

        public String detail() {
            return detail;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (InvalidRecoverResult) obj;
            return Objects.equals(this.detail, that.detail) &&
                super.equals(obj);
        }

        @Override
        public int hashCode() {
            return Objects.hash(detail, super.hashCode());
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

    /**
     * Protected for testing purpose.
     */
    protected class RecoverIterator implements Iterator<RecoverResult> {
        private final long windowLength;
        private final long skipRecordAtOffset;
        private long nextRecoverOffset;
        private long maybeFirstInvalidCycle = -1;
        private long maybeFirstInvalidOffset = -1;
        private RecoverResult next;
        private boolean strictMode = false;
        private long lastValidOffset = -1;
        private boolean reportError = false;

        public RecoverIterator(long nextRecoverOffset, long windowLength, long skipRecordAtOffset) {
            this.nextRecoverOffset = nextRecoverOffset;
            this.skipRecordAtOffset = skipRecordAtOffset;
            this.windowLength = windowLength;
        }

        /**
         * Only used for testing purpose.
         */
        public void strictMode() {
            this.strictMode = true;
        }

        /**
         * Only used for testing purpose.
         */
        public void reportError() {
            this.reportError = true;
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = tryReadNextRecord();
            if (!hasNext) {
                // recovery complete
                recoveryCompleteOffset = WALUtil.alignLargeByBlockSize(nextRecoverOffset);
                walChannel.releaseCache();
            }
            return hasNext;
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
            while (shouldContinue()) {
                long cycle = WALUtil.calculateCycle(nextRecoverOffset, walHeader.getCapacity(), WAL_HEADER_TOTAL_CAPACITY);
                boolean skip = nextRecoverOffset == skipRecordAtOffset;
                try {
                    ByteBuf nextRecordBody = readRecord(nextRecoverOffset, offset -> WALUtil.recordOffsetToPosition(offset, walHeader.getCapacity(), WAL_HEADER_TOTAL_CAPACITY));
                    if (isOutOfWindow(nextRecoverOffset)) {
                        // should never happen, log it
                        LOGGER.error("[BUG] record offset out of window, offset: {}, firstInvalidOffset: {}, window: {}",
                            nextRecoverOffset, maybeFirstInvalidOffset, windowLength);
                    }
                    RecoverResultImpl recoverResult = new RecoverResultImpl(nextRecordBody, nextRecoverOffset);
                    lastValidOffset = nextRecoverOffset;

                    nextRecoverOffset += RECORD_HEADER_SIZE + nextRecordBody.readableBytes();

                    if (maybeFirstInvalidCycle != -1 && maybeFirstInvalidCycle != cycle) {
                        // we meet a valid record in the next cycle, so the "invalid" record we met before is not really invalid
                        maybeFirstInvalidOffset = -1;
                        maybeFirstInvalidCycle = -1;
                    }

                    if (skip) {
                        nextRecordBody.release();
                        continue;
                    }

                    next = recoverResult;
                    return true;
                } catch (ReadRecordException e) {
                    if (maybeFirstInvalidOffset == -1 && WALUtil.isAligned(nextRecoverOffset) && !skip) {
                        maybeFirstInvalidCycle = cycle;
                        maybeFirstInvalidOffset = nextRecoverOffset;
                        // maybe the first invalid offset
                        LOGGER.info("maybe meet the first invalid offset during recovery. cycle: {}, offset: {}, window: {}, detail: '{}'",
                            maybeFirstInvalidCycle, maybeFirstInvalidOffset, windowLength, e.getMessage());
                    }

                    if (reportError) {
                        next = new InvalidRecoverResult(nextRecoverOffset, e.getMessage());
                    }
                    nextRecoverOffset = e.getJumpNextRecoverOffset();
                    if (reportError) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean shouldContinue() {
            if (!isOutOfWindow(nextRecoverOffset)) {
                // within the window
                return true;
            }
            if (strictMode) {
                // not in the window, and in strict mode, so we should stop
                return false;
            }
            // allow to try to recover a little more records (no more than 4MiB)
            return nextRecoverOffset < lastValidOffset + Math.min(windowLength, 1 << 22);
        }

        private boolean isOutOfWindow(long offset) {
            if (maybeFirstInvalidOffset == -1) {
                return false;
            }
            return offset >= maybeFirstInvalidOffset + windowLength;
        }
    }
}
