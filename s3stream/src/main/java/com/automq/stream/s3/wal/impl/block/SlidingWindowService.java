/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALShutdownException;
import com.automq.stream.s3.wal.util.WALChannel;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.ThreadUtils;
import com.automq.stream.utils.Threads;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.github.bucket4j.BlockingBucket;
import io.github.bucket4j.Bucket;
import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.wal.impl.block.BlockWALService.WAL_HEADER_TOTAL_CAPACITY;

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
    /**
     * The maximum rate of refilling the Bucker4j bucket, which is 1 token per nanosecond.
     */
    private static final long MAX_BUCKET_TOKENS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    /**
     * Number of threads for writing blocks.
     */
    private final int ioThreadNums;

    /**
     * The upper limit of the sliding window.
     */
    private final long upperLimit;
    /**
     * The unit to scale out the sliding window.
     */
    private final long scaleUnit;

    /**
     * The soft limit of a block.
     * "Soft limit" means that the block can exceed this limit there is only one large record in this block.
     */
    private final long blockSoftLimit;

    /**
     * The rate limit of write operations.
     */
    private final long writeRateLimit;
    /**
     * The bucket for rate limiting the write operations.
     *
     * @see #writeRateLimit
     */
    private final Bucket writeRateBucket;
    /**
     * The bucket to limit the write bandwidth.
     * Note: one token represents {@link WALUtil#BLOCK_SIZE} bytes. As the max rate in Bucket4j is 1 token per nanosecond,
     * for a block size of 4KiB, the max rate is 3,814 GiB/s; for a block size of 512B, the max rate is 476 GiB/s.
     */
    private final BlockingBucket writeBandwidthBucket;

    /**
     * The channel to write data to the disk.
     */
    private final WALChannel walChannel;

    /**
     * The flusher used to flush the WAL header.
     */
    private final WALHeaderFlusher walHeaderFlusher;

    /**
     * The lock of {@link #pendingBlocks}, {@link #writingBlocks}, {@link #currentBlock}.
     */
    private final Lock blockLock = new ReentrantLock();
    /**
     * Blocks that are waiting to be written.
     * All blocks in this queue are ordered by the start offset.
     */
    private final Queue<Block> pendingBlocks = new ArrayDeque<>();
    /**
     * Blocks that are being written.
     * All blocks in this queue are ordered by the start offset.
     */
    private final Queue<Block> writingBlocks = new ArrayDeque<>();
    /**
     * The current block, records are added to this block.
     */
    private volatile Block currentBlock;

    /**
     * The lock to make sure that there is only at most one callback thread at a time.
     */
    private final Lock callbackLock = new ReentrantLock();

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
     * The thread pool for write operations.
     */
    private ExecutorService ioExecutor;
    /**
     * The scheduler for polling blocks and sending them to @{@link #ioExecutor}.
     */
    private ScheduledExecutorService pollBlockScheduler;

    public SlidingWindowService(WALChannel walChannel, int ioThreadNums, long upperLimit, long scaleUnit,
        long blockSoftLimit, int writeRateLimit, long writeBandwidthLimit, WALHeaderFlusher flusher) {
        this.walChannel = walChannel;
        this.ioThreadNums = ioThreadNums;
        this.upperLimit = upperLimit;
        this.scaleUnit = scaleUnit;
        this.blockSoftLimit = blockSoftLimit;
        this.writeRateLimit = writeRateLimit;
        this.writeRateBucket = Bucket.builder()
            .addLimit(limit -> limit
                .capacity(Math.max(writeRateLimit / 10, 1))
                .refillGreedy(writeRateLimit, Duration.ofSeconds(1))
            ).build();
        long writeBandwidthLimitByBlock = Math.min(WALUtil.bytesToBlocks(writeBandwidthLimit), MAX_BUCKET_TOKENS_PER_SECOND);
        this.writeBandwidthBucket = Bucket.builder()
            .addLimit(limit -> limit
                .capacity(Math.max(writeBandwidthLimitByBlock / 10, 1))
                .refillGreedy(writeBandwidthLimitByBlock, Duration.ofSeconds(1))
            ).build()
            .asBlocking();
        this.walHeaderFlusher = flusher;
    }

    public WindowCoreData getWindowCoreData() {
        assert initialized();
        return windowCoreData;
    }

    public void start(AtomicLong windowMaxLength, long windowStartOffset) {
        this.windowCoreData = new WindowCoreData(windowMaxLength, windowStartOffset, windowStartOffset);
        this.ioExecutor = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(ioThreadNums,
            "block-wal-io-thread", false, LOGGER);

        long scheduledInterval = Math.max(MIN_SCHEDULED_WRITE_INTERVAL_NANOS, TimeUnit.SECONDS.toNanos(1) / writeRateLimit);
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
        List<Runnable> tasks = new LinkedList<>();
        try {
            gracefulShutdown = this.ioExecutor.awaitTermination(timeout, unit);
        } catch (InterruptedException e) {
            tasks = this.ioExecutor.shutdownNow();
            gracefulShutdown = false;
        }

        notifyWriteFuture(tasks);

        return gracefulShutdown;
    }

    private void notifyWriteFuture(List<Runnable> tasks) {
        Collection<CompletableFuture<AppendResult.CallbackResult>> futures = new LinkedList<>();
        for (Runnable task : tasks) {
            if (task instanceof WriteBlockProcessor) {
                WriteBlockProcessor processor = (WriteBlockProcessor) task;
                futures.addAll(processor.block.futures());
            }
        }
        for (Block block : this.pendingBlocks) {
            futures.addAll(block.futures());
        }
        if (currentBlock != null && !currentBlock.isEmpty()) {
            futures.addAll(currentBlock.futures());
        }

        doNotify(futures);
    }

    private void doNotify(Collection<CompletableFuture<AppendResult.CallbackResult>> futures) {
        for (CompletableFuture<AppendResult.CallbackResult> future : futures) {
            future.completeExceptionally(new WALShutdownException("failed to write: ring buffer is shutdown"));
        }
    }

    /**
     * Try to write a block. If it exceeds the rate limit, it will return immediately.
     */
    public void tryWriteBlock() {
        assert initialized();
        if (!tryAcquireWriteRateLimit()) {
            return;
        }
        Block block = pollBlock();
        if (block != null) {
            block.polled();
            ioExecutor.submit(new WriteBlockProcessor(block));
        }
    }

    /**
     * Try to acquire the write rate limit.
     */
    private boolean tryAcquireWriteRateLimit() {
        return writeRateBucket.tryConsume(1);
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

        int remainingSize = (int) remainingSizeOfDevice(startOffset, recordSectionCapacity);
        if (remainingSize < minSize) {
            // If the end of the physical device is insufficient for this block, create a padding block and jump to the start of the physical device
            Block paddingBlock = createPaddingBlock(remainingSize, startOffset, trimOffset, recordSectionCapacity);
            updateCurrentBlockLocked(previousBlock, paddingBlock);
            previousBlock = paddingBlock;

            startOffset = startOffset + remainingSize;
            assert startOffset == nextBlockStartOffset(paddingBlock);
            assert startOffset % recordSectionCapacity == 0;
        }

        Block newBlock = createNewBlock(minSize, startOffset, trimOffset, recordSectionCapacity);
        updateCurrentBlockLocked(previousBlock, newBlock);
        return newBlock;
    }

    private Block createPaddingBlock(int remainingSize, long startOffset, long trimOffset, long recordSectionCapacity) throws OverCapacityException {
        Block paddingBlock = createNewBlock(remainingSize, startOffset, trimOffset, recordSectionCapacity);
        paddingBlock.addRecord(
            remainingSize,
            (offset, header) -> WALUtil.generatePaddingRecord(header, offset, remainingSize),
            new CompletableFuture<>()
        );
        return paddingBlock;
    }

    private void updateCurrentBlockLocked(Block previousBlock, Block newBlock) {
        assert previousBlock == currentBlock;
        if (previousBlock.isEmpty()) {
            // The previous block is empty, so it can be released directly
            previousBlock.release();
        } else {
            // There are some records to be written in the previous block
            pendingBlocks.add(previousBlock);
        }
        setCurrentBlockLocked(newBlock);
    }

    private Block createNewBlock(long minSize, long startOffset, long trimOffset, long recordSectionCapacity) throws OverCapacityException {
        long remainingSizeOfDevice = remainingSizeOfDevice(startOffset, recordSectionCapacity);
        long remainingSizeOfRingBuffer = remainingSizeOfRingBuffer(startOffset, trimOffset, recordSectionCapacity);

        if (remainingSizeOfRingBuffer < minSize) {
            LOGGER.warn("failed to allocate write offset as the ring buffer is full: startOffset: {}, minSize: {}, trimOffset: {}, recordSectionCapacity: {}",
                startOffset, minSize, trimOffset, recordSectionCapacity);
            throw new OverCapacityException(String.format("failed to allocate write offset: ring buffer is full: startOffset: %d, minSize: %d, trimOffset: %d, recordSectionCapacity: %d",
                startOffset, minSize, trimOffset, recordSectionCapacity));
        }

        long maxSize = NumberUtils.min(upperLimit, remainingSizeOfDevice, remainingSizeOfRingBuffer);
        return new BlockImpl(startOffset, maxSize, blockSoftLimit);
    }

    /**
     * The remaining size of the physical device.
     * Let capacity=100, start=198, trim=197, then remainingSize=100-198%100=2
     */
    private static long remainingSizeOfDevice(long startOffset, long recordSectionCapacity) {
        return recordSectionCapacity - startOffset % recordSectionCapacity;
    }

    /**
     * The remaining size of the ring buffer.
     * Let capacity=100, start=148, trim=49, then remainingSize=100-148+49=1
     */
    private static long remainingSizeOfRingBuffer(long startOffset, long trimOffset, long recordSectionCapacity) {
        return recordSectionCapacity - startOffset + trimOffset;
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
     * Get a block to be written. If there is no non-empty block, return null.
     */
    private Block pollBlock() {
        blockLock.lock();
        try {
            return pollBlockLocked();
        } finally {
            blockLock.unlock();
        }
    }

    /**
     * Get a block to be written. If there is no non-empty block, return null.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    private Block pollBlockLocked() {
        Block polled = null;

        Block currentBlock = getCurrentBlockLocked();
        if (!pendingBlocks.isEmpty()) {
            polled = pendingBlocks.poll();
        } else if (currentBlock != null && !currentBlock.isEmpty()) {
            polled = currentBlock;
            setCurrentBlockLocked(nextBlock(currentBlock));
        }

        if (polled != null) {
            writingBlocks.add(polled);
        }

        return polled;
    }

    /**
     * Mark the given block as written and remove all continuous blocks that have been written.
     */
    private WroteBlockResult wroteBlock(Block wroteBlock) {
        blockLock.lock();
        try {
            return wroteBlockLocked(wroteBlock);
        } finally {
            blockLock.unlock();
        }
    }

    /**
     * Mark the given block as written and remove all continuous blocks that have been written.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    private WroteBlockResult wroteBlockLocked(Block wroteBlock) {
        wroteBlock.markWritten();

        assert writingBlocks.contains(wroteBlock);
        List<Block> writtenBlocks = removeWrittenBlocksLocked();
        long unflushedOffset = getFirstUnflushedOffsetLocked();

        return new WroteBlockResult(writtenBlocks, unflushedOffset);
    }

    /**
     * Remove all continuous blocks that have been written from the writing queue.
     * Note: this method is NOT thread safe, and it should be called with {@link #blockLock} locked.
     */
    private List<Block> removeWrittenBlocksLocked() {
        List<Block> removedBlocks = new ArrayList<>();
        while (!writingBlocks.isEmpty()) {
            Block block = writingBlocks.peek();
            if (block.isWritten()) {
                removedBlocks.add(writingBlocks.poll());
            } else {
                break;
            }
        }
        return removedBlocks;
    }

    /**
     * Get the start offset of the first block which has not been flushed yet.
     */
    private long getFirstUnflushedOffsetLocked() {
        if (writingBlocks.isEmpty()) {
            return getCurrentBlockLocked().startOffset();
        }
        return writingBlocks.peek().startOffset();
    }

    private static class WroteBlockResult {
        private final List<Block> writtenBlocks;
        private final long unflushedOffset;

        public WroteBlockResult(List<Block> writtenBlocks, long unflushedOffset) {
            this.writtenBlocks = writtenBlocks;
            this.unflushedOffset = unflushedOffset;
        }
    }

    private void writeBlockData(Block block) throws IOException {
        long position = WALUtil.recordOffsetToPosition(block.startOffset(), walChannel.capacity(), WAL_HEADER_TOTAL_CAPACITY);
        ByteBuf data = block.data();
        writeBandwidthBucket.consumeUninterruptibly(WALUtil.bytesToBlocks(data.readableBytes()));
        walChannel.retryWrite(data, position);
        walChannel.retryFlush();
    }

    private void makeWriteOffsetMatchWindow(long newWindowEndOffset) throws IOException {
        // align to block size
        newWindowEndOffset = WALUtil.alignLargeByBlockSize(newWindowEndOffset);
        long windowStartOffset = windowCoreData.getStartOffset();
        long windowMaxLength = windowCoreData.getMaxLength();
        if (newWindowEndOffset > windowStartOffset + windowMaxLength) {
            // endOffset - startOffset <= block.maxSize <= upperLimit in {@link #sealAndNewBlockLocked}
            assert newWindowEndOffset - windowStartOffset <= upperLimit;
            long newWindowMaxLength = Math.min(remainingSizeOfRingBuffer(windowStartOffset, scaleUnit, newWindowEndOffset), upperLimit);
            windowCoreData.scaleOutWindow(walHeaderFlusher, newWindowMaxLength);
        }
    }

    public interface WALHeaderFlusher {
        void flush() throws IOException;
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

        public void scaleOutWindow(WALHeaderFlusher flusher, long newMaxLength) throws IOException {
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
        private final Block block;
        private final long startTime;

        public WriteBlockProcessor(Block block) {
            this.block = block;
            this.startTime = System.nanoTime();
        }

        @Override
        public void run() {
            StorageOperationStats.getInstance().appendWALAwaitStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
            try {
                writeBlock(block);
                completeWriteAndMaybeCallback(block);
            } catch (Exception e) {
                // should not happen, but just in case
                FutureUtil.completeExceptionally(block.futures().iterator(), e);
                LOGGER.error(String.format("failed to write blocks, startOffset: %s", block.startOffset()), e);
            } finally {
                block.release();
            }
        }

        private void writeBlock(Block block) throws IOException {
            final long startTime = System.nanoTime();
            makeWriteOffsetMatchWindow(block.endOffset());
            writeBlockData(block);
            StorageOperationStats.getInstance().appendWALWriteStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
        }

        private void completeWriteAndMaybeCallback(Block block) {
            final long startTime = System.nanoTime();
            callbackLock.lock();
            try {
                WroteBlockResult wroteBlockResult = wroteBlock(block);
                // Update the start offset of the sliding window after finishing writing the record.
                windowCoreData.updateWindowStartOffset(wroteBlockResult.unflushedOffset);
                // Notify the futures of blocks that have been written.
                for (Block writtenBlock : wroteBlockResult.writtenBlocks) {
                    callback(writtenBlock);
                    StorageOperationStats.getInstance().appendWALAfterStats.record(TimerUtil.timeElapsedSince(startTime, TimeUnit.NANOSECONDS));
                }
            } finally {
                callbackLock.unlock();
            }
        }

        private void callback(Block block) {
            FutureUtil.complete(block.futures().iterator(), new AppendResult.CallbackResult() {
                @Override
                public long flushedOffset() {
                    return windowCoreData.getStartOffset();
                }

                @Override
                public String toString() {
                    return "CallbackResult{" + "flushedOffset=" + flushedOffset() + '}';
                }
            });
        }
    }
}
