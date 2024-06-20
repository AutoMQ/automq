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
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /**
     * The minimum interval between two scheduled write operations. At most 1000 per second.
     *
     * @see this#pollBlockScheduler
     */
    private static final long MIN_SCHEDULED_WRITE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1) / 1000;
    private final int ioThreadNums;
    private final long upperLimit;
    private final long scaleUnit;
    private final long blockSoftLimit;
    private final long minWriteIntervalNanos;
    private final WALChannel walChannel;
    private final WALHeaderFlusher walHeaderFlusher;

    /**
     * The lock of {@link #pendingBlocks}, {@link #writingBlocks}, {@link #currentBlock}.
     */
    private final Lock blockLock = new ReentrantLock();
    /**
     * Blocks that are being written.
     */
    private final Queue<Long> writingBlocks = new PriorityQueue<>();
    /**
     * Whether the service is initialized.
     * After the service is initialized, data in {@link #windowCoreData} is valid.
     */
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * The core data of the sliding window. Initialized when the service is started.
     */
    private WindowCoreData windowCoreData;
    /**
     * Blocks that are waiting to be written.
     * All blocks in this queue are ordered by the start offset.
     */
    private Queue<Block> pendingBlocks = new LinkedList<>();
    /**
     * The current block, records are added to this block.
     */
    private Block currentBlock;

    /**
     * The thread pool for write operations.
     */
    private ExecutorService ioExecutor;
    /**
     * The scheduler for polling blocks and sending them to @{@link #ioExecutor}.
     */
    private ScheduledExecutorService pollBlockScheduler;

    /**
     * The last time when a batch of blocks is written to the disk.
     */
    private long lastWriteTimeNanos = 0;

    public SlidingWindowService(WALChannel walChannel, int ioThreadNums, long upperLimit, long scaleUnit,
        long blockSoftLimit, int writeRateLimit, WALHeaderFlusher flusher) {
        this.walChannel = walChannel;
        this.ioThreadNums = ioThreadNums;
        this.upperLimit = upperLimit;
        this.scaleUnit = scaleUnit;
        this.blockSoftLimit = blockSoftLimit;
        this.minWriteIntervalNanos = TimeUnit.SECONDS.toNanos(1) / writeRateLimit;
        this.walHeaderFlusher = flusher;
    }

    public WindowCoreData getWindowCoreData() {
        assert initialized();
        return windowCoreData;
    }

    public void start(AtomicLong windowMaxLength, long windowStartOffset) {
        this.windowCoreData = new WindowCoreData(windowMaxLength, windowStartOffset, windowStartOffset);
        this.ioExecutor = Threads.newFixedThreadPoolWithMonitor(ioThreadNums,
            "block-wal-io-thread", false, LOGGER);

        long scheduledInterval = Math.max(MIN_SCHEDULED_WRITE_INTERVAL_NANOS, minWriteIntervalNanos);
        this.pollBlockScheduler = Threads.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("wal-poll-block-thread-%d", false), LOGGER);
        pollBlockScheduler.scheduleAtFixedRate(this::tryWriteBlock, 0, scheduledInterval, TimeUnit.NANOSECONDS);

        initialized.set(true);
    }

    public boolean initialized() {
        return initialized.get();
    }

    public boolean shutdown(long timeout, TimeUnit unit) {
        if (this.ioExecutor == null) {
            return true;
        }

        boolean gracefulShutdown;
        this.ioExecutor.shutdown();
        this.pollBlockScheduler.shutdownNow();
        try {
            gracefulShutdown = this.ioExecutor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            this.ioExecutor.shutdownNow();
            gracefulShutdown = false;
        }
        return gracefulShutdown;
    }

    /**
     * Try to write a block. If it exceeds the rate limit, it will return immediately.
     */
    public void tryWriteBlock() {
        assert initialized();
        if (!tryAcquireWriteRateLimit()) {
            return;
        }
        BlockBatch blocks = pollBlocks();
        if (blocks != null) {
            blocks.blocks().forEach(Block::polled);
            ioExecutor.submit(new WriteBlockProcessor(blocks));
        }
    }

    /**
     * Try to acquire the write rate limit.
     */
    synchronized private boolean tryAcquireWriteRateLimit() {
        long now = System.nanoTime();
        if (now - lastWriteTimeNanos < minWriteIntervalNanos) {
            return false;
        }
        lastWriteTimeNanos = now;
        return true;
    }

    public Lock getBlockLock() {
        assert initialized();
        return blockLock;
    }

    /**
     * Seal and create a new block. It
     * - puts the previous block to the write queue
     * - creates a new block, sets it as the current block and returns it
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    public Block sealAndNewBlockLocked(Block previousBlock, long minSize, long trimOffset,
        long recordSectionCapacity) throws OverCapacityException {
        assert initialized();
        long startOffset = nextBlockStartOffset(previousBlock);

        // If the end of the physical device is insufficient for this block, jump to the start of the physical device
        if ((recordSectionCapacity - startOffset % recordSectionCapacity) < minSize) {
            startOffset = startOffset + recordSectionCapacity - startOffset % recordSectionCapacity;
        }

        // Not enough space for this block
        if (startOffset + minSize - trimOffset > recordSectionCapacity) {
            LOGGER.warn("failed to allocate write offset as the ring buffer is full: startOffset: {}, minSize: {}, trimOffset: {}, recordSectionCapacity: {}",
                startOffset, minSize, trimOffset, recordSectionCapacity);
            throw new OverCapacityException(String.format("failed to allocate write offset: ring buffer is full: startOffset: %d, minSize: %d, trimOffset: %d, recordSectionCapacity: %d",
                startOffset, minSize, trimOffset, recordSectionCapacity));
        }

        long maxSize = upperLimit;
        // The size of the block should not be larger than writable size of the ring buffer
        // Let capacity=100, start=148, trim=49, then maxSize=100-148+49=1
        maxSize = Math.min(recordSectionCapacity - startOffset + trimOffset, maxSize);
        // The size of the block should not be larger than the end of the physical device
        // Let capacity=100, start=198, trim=198, then maxSize=100-198%100=2
        maxSize = Math.min(recordSectionCapacity - startOffset % recordSectionCapacity, maxSize);

        Block newBlock = new BlockImpl(startOffset, maxSize, blockSoftLimit);
        if (!previousBlock.isEmpty()) {
            // There are some records to be written in the previous block
            pendingBlocks.add(previousBlock);
        } else {
            // The previous block is empty, so it can be released directly
            previousBlock.release();
        }
        setCurrentBlockLocked(newBlock);
        return newBlock;
    }

    /**
     * Get the current block.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    public Block getCurrentBlockLocked() {
        assert initialized();
        // The current block is null only when no record has been written
        if (null == currentBlock) {
            currentBlock = nextBlock(windowCoreData.getNextWriteOffset());
        }
        return currentBlock;
    }

    /**
     * Set the current block.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    private void setCurrentBlockLocked(Block block) {
        this.currentBlock = block;
    }

    /**
     * Get the start offset of the next block.
     */
    private long nextBlockStartOffset(Block block) {
        return block.startOffset() + WALUtil.alignLargeByBlockSize(block.size());
    }

    /**
     * Create a new block with the given start offset.
     * This method is only used when we don't know the maximum length of the new block.
     */
    private Block nextBlock(long startOffset) {
        // Trick: we cannot determine the maximum length of the block here, so we set it to 0 first.
        // When we try to write a record, this block will be found full, and then a new block will be created.
        return new BlockImpl(startOffset, 0, 0);
    }

    /**
     * Create a new block with the given previous block.
     * This method is only used when we don't know the maximum length of the new block.
     */
    private Block nextBlock(Block previousBlock) {
        return nextBlock(nextBlockStartOffset(previousBlock));
    }

    /**
     * Get all blocks to be written. If there is no non-empty block, return null.
     */
    private BlockBatch pollBlocks() {
        blockLock.lock();
        try {
            return pollBlocksLocked();
        } finally {
            blockLock.unlock();
        }
    }

    /**
     * Get all blocks to be written. If there is no non-empty block, return null.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    private BlockBatch pollBlocksLocked() {
        Block currentBlock = getCurrentBlockLocked();

        boolean isPendingBlockEmpty = pendingBlocks.isEmpty();
        boolean isCurrentBlockEmpty = currentBlock == null || currentBlock.isEmpty();
        if (isPendingBlockEmpty && isCurrentBlockEmpty) {
            // No record to be written
            return null;
        }

        Collection<Block> blocks;
        if (!isPendingBlockEmpty) {
            blocks = pendingBlocks;
            pendingBlocks = new LinkedList<>();
        } else {
            blocks = new LinkedList<>();
        }
        if (!isCurrentBlockEmpty) {
            blocks.add(currentBlock);
            setCurrentBlockLocked(nextBlock(currentBlock));
        }

        BlockBatch blockBatch = new BlockBatch(blocks);
        writingBlocks.add(blockBatch.startOffset());

        return blockBatch;
    }

    /**
     * Finish the given block batch, and return the start offset of the first block which has not been flushed yet.
     */
    private long wroteBlocks(BlockBatch wroteBlocks) {
        blockLock.lock();
        try {
            return wroteBlocksLocked(wroteBlocks);
        } finally {
            blockLock.unlock();
        }
    }

    /**
     * Finish the given block batch, and return the start offset of the first block which has not been flushed yet.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    private long wroteBlocksLocked(BlockBatch wroteBlocks) {
        boolean removed = writingBlocks.remove(wroteBlocks.startOffset());
        assert removed;
        if (writingBlocks.isEmpty()) {
            return getCurrentBlockLocked().startOffset();
        }
        return writingBlocks.peek();
    }

    private void writeBlockData(BlockBatch blocks) {
        final long start = System.nanoTime();
        for (Block block : blocks.blocks()) {
            long position = WALUtil.recordOffsetToPosition(block.startOffset(), walChannel.capacity(), WAL_HEADER_TOTAL_CAPACITY);
            walChannel.retryWrite(block.data(), position);
        }
        walChannel.retryFlush();
        StorageOperationStats.getInstance().appendWALWriteStats.record(TimerUtil.durationElapsedAs(start, TimeUnit.NANOSECONDS));
    }

    private void makeWriteOffsetMatchWindow(long newWindowEndOffset) {
        // align to block size
        newWindowEndOffset = WALUtil.alignLargeByBlockSize(newWindowEndOffset);
        long windowStartOffset = windowCoreData.getStartOffset();
        long windowMaxLength = windowCoreData.getMaxLength();
        if (newWindowEndOffset > windowStartOffset + windowMaxLength) {
            // endOffset - startOffset <= block.maxSize <= upperLimit in {@link #sealAndNewBlockLocked}
            assert newWindowEndOffset - windowStartOffset <= upperLimit;
            long newWindowMaxLength = Math.min(newWindowEndOffset - windowStartOffset + scaleUnit, upperLimit);
            windowCoreData.scaleOutWindow(walHeaderFlusher, newWindowMaxLength);
        }
    }

    public interface WALHeaderFlusher {
        void flush();
    }

    public static class RecordHeaderCoreData {
        private int magicCode0 = RECORD_HEADER_MAGIC_CODE;
        private int recordBodyLength1;
        private long recordBodyOffset2;
        private int recordBodyCRC3;
        private int recordHeaderCRC4;

        public static RecordHeaderCoreData unmarshal(ByteBuf byteBuf) {
            RecordHeaderCoreData recordHeaderCoreData = new RecordHeaderCoreData();
            byteBuf.markReaderIndex();
            recordHeaderCoreData.magicCode0 = byteBuf.readInt();
            recordHeaderCoreData.recordBodyLength1 = byteBuf.readInt();
            recordHeaderCoreData.recordBodyOffset2 = byteBuf.readLong();
            recordHeaderCoreData.recordBodyCRC3 = byteBuf.readInt();
            recordHeaderCoreData.recordHeaderCRC4 = byteBuf.readInt();
            byteBuf.resetReaderIndex();
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

        private ByteBuf marshalHeaderExceptCRC() {
            ByteBuf buf = ByteBufAlloc.byteBuffer(RECORD_HEADER_SIZE);
            buf.writeInt(magicCode0);
            buf.writeInt(recordBodyLength1);
            buf.writeLong(recordBodyOffset2);
            buf.writeInt(recordBodyCRC3);
            return buf;
        }

        public ByteBuf marshal() {
            ByteBuf buf = marshalHeaderExceptCRC();
            buf.writeInt(WALUtil.crc32(buf, RECORD_HEADER_WITHOUT_CRC_SIZE));
            return buf;
        }
    }

    public static class WindowCoreData {
        private final Lock scaleOutLock = new ReentrantLock();
        private final AtomicLong maxLength;
        /**
         * Next write offset of sliding window, always aligned to the {@link WALUtil#BLOCK_SIZE}.
         */
        private final AtomicLong nextWriteOffset;
        /**
         * Start offset of sliding window, always aligned to the {@link WALUtil#BLOCK_SIZE}.
         * The data before this offset has already been written to the disk.
         */
        private final AtomicLong startOffset;

        public WindowCoreData(AtomicLong maxLength, long nextWriteOffset, long startOffset) {
            this.maxLength = maxLength;
            this.nextWriteOffset = new AtomicLong(nextWriteOffset);
            this.startOffset = new AtomicLong(startOffset);
        }

        public long getMaxLength() {
            return maxLength.get();
        }

        public void setMaxLength(long maxLength) {
            this.maxLength.set(maxLength);
        }

        public long getNextWriteOffset() {
            return nextWriteOffset.get();
        }

        public long getStartOffset() {
            return startOffset.get();
        }

        public void updateWindowStartOffset(long offset) {
            this.startOffset.accumulateAndGet(offset, Math::max);
        }

        public void scaleOutWindow(WALHeaderFlusher flusher, long newMaxLength) {
            boolean scaleWindowHappened = false;
            scaleOutLock.lock();
            try {
                if (newMaxLength < getMaxLength()) {
                    // Another thread has already scaled out the window.
                    return;
                }

                setMaxLength(newMaxLength);
                flusher.flush();
                scaleWindowHappened = true;
            } finally {
                scaleOutLock.unlock();
                if (scaleWindowHappened) {
                    LOGGER.info("window scale out to {}", newMaxLength);
                } else {
                    LOGGER.debug("window already scale out, ignore");
                }
            }
        }
    }

    class WriteBlockProcessor implements Runnable {
        private final BlockBatch blocks;
        private final long startTime;

        public WriteBlockProcessor(BlockBatch blocks) {
            this.blocks = blocks;
            this.startTime = System.nanoTime();
        }

        @Override
        public void run() {
            StorageOperationStats.getInstance().appendWALAwaitStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
            try {
                writeBlock(this.blocks);
            } catch (Exception e) {
                // should not happen, but just in case
                FutureUtil.completeExceptionally(blocks.futures(), e);
                LOGGER.error(String.format("failed to write blocks, startOffset: %s", blocks.startOffset()), e);
            } finally {
                blocks.release();
            }
        }

        private void writeBlock(BlockBatch blocks) {
            makeWriteOffsetMatchWindow(blocks.endOffset());
            writeBlockData(blocks);

            final long startTime = System.nanoTime();
            // Update the start offset of the sliding window after finishing writing the record.
            windowCoreData.updateWindowStartOffset(wroteBlocks(blocks));

            FutureUtil.complete(blocks.futures(), new AppendResult.CallbackResult() {
                @Override
                public long flushedOffset() {
                    return windowCoreData.getStartOffset();
                }

                @Override
                public String toString() {
                    return "CallbackResult{" + "flushedOffset=" + flushedOffset() + '}';
                }
            });
            StorageOperationStats.getInstance().appendWALAfterStats.record(TimerUtil.durationElapsedAs(startTime, TimeUnit.NANOSECONDS));
        }
    }
}
