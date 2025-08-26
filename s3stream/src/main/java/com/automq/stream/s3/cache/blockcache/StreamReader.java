/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.exceptions.AutoMQException;
import com.automq.stream.s3.exceptions.BlockNotContinuousException;
import com.automq.stream.s3.exceptions.ObjectNotExistException;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.LogSuppressor;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.automq.stream.utils.threads.EventLoopSafe;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static com.automq.stream.s3.cache.CacheAccessType.BLOCK_CACHE_HIT;
import static com.automq.stream.s3.cache.CacheAccessType.BLOCK_CACHE_MISS;
import static com.automq.stream.utils.FutureUtil.exec;

@EventLoopSafe public class StreamReader {
    public static final int GET_OBJECT_STEP = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReader.class);
    static final int READAHEAD_SIZE_UNIT = 1024 * 1024 / 2;
    private static final int MAX_READAHEAD_SIZE = Systems.getEnvInt("AUTOMQ_MAX_READAHEAD_SIZE", 32 * 1024 * 1024);
    private static final long READAHEAD_RESET_COLD_DOWN_MILLS = TimeUnit.MINUTES.toMillis(1);
    private static final long READAHEAD_AVAILABLE_BYTES_THRESHOLD = 32L * 1024 * 1024;
    private static final LogSuppressor READAHEAD_RESET_LOG_SUPPRESSOR = new LogSuppressor(LOGGER, 30000);
    private static final LogSuppressor BLOCKS_RESET_LOG_SUPPRESSOR = new LogSuppressor(LOGGER, 30000);
    // visible to test
    final NavigableMap<Long, Block> blocksMap = new TreeMap<>();
    Block lastBlock = null;
    long loadedBlockIndexEndOffset = 0L;
    // When the blocks reset, the epoch will increase. It's used to prevent the loadMoreBlocksWithoutData add invalid blocks.
    long blocksEpoch = 0L;

    final Readahead readahead;
    private final long streamId;
    private final EventLoop eventLoop;
    private final ObjectManager objectManager;
    private final ObjectReaderFactory objectReaderFactory;
    private final DataBlockCache dataBlockCache;
    private final Time time;
    long nextReadOffset;
    private CompletableFuture<Void> inflightLoadIndexCf;
    private volatile CompletableFuture<Void> afterReadTryReadaheadCf;
    private long lastAccessTimestamp;
    private boolean reading = false;

    private boolean closed = false;

    public StreamReader(long streamId, long nextReadOffset, EventLoop eventLoop, ObjectManager objectManager,
        ObjectReaderFactory objectReaderFactory, DataBlockCache dataBlockCache) {
        this(streamId, nextReadOffset, eventLoop, objectManager, objectReaderFactory, dataBlockCache, Time.SYSTEM);
    }

    public StreamReader(long streamId, long nextReadOffset, EventLoop eventLoop, ObjectManager objectManager,
        ObjectReaderFactory objectReaderFactory, DataBlockCache dataBlockCache, Time time) {
        this.streamId = streamId;
        this.nextReadOffset = nextReadOffset;
        this.readahead = new Readahead();
        this.time = time;
        this.lastAccessTimestamp = time.milliseconds();

        this.eventLoop = eventLoop;
        this.objectManager = objectManager;
        this.objectReaderFactory = objectReaderFactory;
        this.dataBlockCache = dataBlockCache;
    }

    public CompletableFuture<ReadDataBlock> read(long startOffset, long endOffset, int maxBytes) {
        if (startOffset != nextReadOffset) {
            return FutureUtil.failedFuture(new AutoMQException(String.format("[BUG] %s read offset not match, expect %d but %d", this, nextReadOffset, startOffset)));
        }
        if (reading) {
            return FutureUtil.failedFuture(new IllegalStateException(String.format("%s is in reading state, can't read again", this)));
        }
        reading = true;
        try {
            return read(startOffset, endOffset, maxBytes, 2).whenComplete((rst, ex) -> reading = false);
        } catch (Throwable e) {
            reading = false;
            return FutureUtil.failedFuture(e);
        }
    }

    CompletableFuture<ReadDataBlock> read(long startOffset, long endOffset, int maxBytes, int leftRetries) {
        lastAccessTimestamp = time.milliseconds();
        ReadContext readContext = new ReadContext();
        read0(readContext, startOffset, endOffset, maxBytes);
        CompletableFuture<ReadDataBlock> retCf = new CompletableFuture<>();
        readContext.cf.whenComplete((rst, ex) -> exec(() -> {
            Throwable cause = FutureUtil.cause(ex);
            if (cause != null) {
                readContext.records.forEach(StreamRecordBatch::release);
                for (Block block : readContext.blocks) {
                    block.release();
                }
                if (leftRetries > 0 && isRecoverable(cause)) {
                    // The cached blocks maybe invalid after object compaction, so we need to reset the blocks and retry read
                    resetBlocks();
                    // use async to prevent recursive call cause stack overflow
                    eventLoop.execute(() -> FutureUtil.propagate(read(startOffset, endOffset, maxBytes, leftRetries - 1), retCf));
                } else {
                    retCf.completeExceptionally(cause);
                }
            } else {
                afterRead(rst, readContext);
                StorageOperationStats.getInstance().blockCacheReadStreamThroughput.add(MetricsLevel.INFO, rst.sizeInBytes());
                retCf.complete(rst);
            }
        }, retCf, LOGGER, "read"));
        return retCf;
    }

    public long nextReadOffset() {
        return nextReadOffset;
    }

    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    public void close() {
        closed = true;
        List<Block> blocks = new ArrayList<>(blocksMap.values());
        // The Block#markRead will immediately invoke after the Block is removed.
        blocksMap.clear();
        blocks.forEach(Block::markReadCompleted);
    }

    void read0(ReadContext ctx, final long startOffset, final long endOffset, final int maxBytes) {
        // 1. get blocks
        CompletableFuture<List<Block>> getBlocksCf = getBlocks(startOffset, endOffset, maxBytes, false);

        // 2. wait block's data loaded
        List<Block> blocks = new ArrayList<>();
        CompletableFuture<Void> loadBlocksCf = getBlocksCf.thenCompose(blockList -> {
            blocks.addAll(blockList);
            return CompletableFuture.allOf(blockList.stream().map(block -> block.loadCf).toArray(CompletableFuture[]::new));
        });

        // if the cache is hit, the loadBlocks will be done immediately
        if (!loadBlocksCf.isDone()) {
            ctx.accessType = BLOCK_CACHE_MISS;
        }

        // 3. extract records from blocks
        loadBlocksCf.thenAccept(nil -> {
            ctx.blocks.addAll(blocks);
            Optional<Block> failedBlock = blocks.stream().filter(block -> block.exception != null).findAny();
            if (failedBlock.isPresent()) {
                ctx.cf.completeExceptionally(failedBlock.get().exception);
                return;
            }
            if (blocks.isEmpty()) {
                ctx.cf.completeExceptionally(new AutoMQException(String.format("[UNEXPECTED] streamId=%d Get empty blocks [%s, %s) %s", streamId, startOffset, endOffset, maxBytes)));
                return;
            }
            int remainingSize = maxBytes;
            long nextStartOffset = startOffset;
            long nextEndOffset;
            boolean fulfill = false;
            for (Block block : blocks) {
                DataBlockIndex index = block.index;
                if (nextStartOffset < index.startOffset() || nextStartOffset >= index.endOffset()) {
                    String msg = String.format("[BUG] nextStartOffset:%d is not in the range of index:%d-%d", nextStartOffset, index.startOffset(), index.endOffset());
                    LOGGER.error(msg);
                    ctx.cf.completeExceptionally(new RuntimeException(msg));
                    return;
                }
                nextEndOffset = Math.min(endOffset, index.endOffset());
                List<StreamRecordBatch> newRecords = block.data.getRecords(nextStartOffset, nextEndOffset, remainingSize);
                nextStartOffset = nextEndOffset;
                remainingSize -= newRecords.stream().mapToInt(StreamRecordBatch::size).sum();
                ctx.records.addAll(newRecords);
                if (nextStartOffset >= endOffset || remainingSize <= 0) {
                    fulfill = true;
                    break;
                }
            }
            if (fulfill) {
                ctx.cf.complete(new ReadDataBlock(ctx.records, ctx.accessType));
                StorageOperationStats.getInstance().readBlockCacheStats(ctx.accessType == BLOCK_CACHE_HIT).record(ctx.start.elapsedAs(TimeUnit.NANOSECONDS));
            } else {
                if (nextStartOffset == startOffset) {
                    // The nextStartOffset is not changed. It means we can't read any more records from the blocks.
                    // So we should fast fail to prevent infinite loop.
                    ctx.cf.completeExceptionally(new AutoMQException("[UNEXPECTED] Can't read any record from the blocks"));
                    return;
                }
                // The DataBlockIndex#size is not precise cause of the data block contains record header and data block header.
                // So we may need to retry read to fulfill the endOffset or maxBytes
                long finalNextStartOffset = nextStartOffset;
                int finalRemainingSize = remainingSize;
                // use async to prevent recursive call cause stack overflow
                eventLoop.execute(() -> read0(ctx, finalNextStartOffset, endOffset, finalRemainingSize));
            }
        }).whenComplete((nil, ex) -> {
            if (ex != null) {
                ctx.cf.completeExceptionally(ex);
            }
        });
    }

    /**
     * This method is only for unit testing.
     * AfterReadTryReadaheadCf is empty, the task has been completed
     * AfterReadTryReadaheadCf is not empty, the task may be completed
     *
     * @return afterReadTryReadaheadCf
     */
    @VisibleForTesting
    CompletableFuture<Void> getAfterReadTryReadaheadCf() {
        return afterReadTryReadaheadCf;
    }

    /**
     * This method is only for unit testing.
     * inflightReadaheadCf is empty, the task has been completed
     * inflightReadaheadCf is not empty, the task may be completed
     *
     * @return readahead.inflightReadaheadCf
     */
    @VisibleForTesting
    CompletableFuture<Void> getReadaheadInflightReadaheadCf() {
        return readahead.inflightReadaheadCf;
    }

    void afterRead(ReadDataBlock readDataBlock, ReadContext ctx) {
        List<StreamRecordBatch> records = readDataBlock.getRecords();
        if (!records.isEmpty()) {
            nextReadOffset = records.get(records.size() - 1).getLastOffset();
        }
        // clear unused blocks
        Iterator<Map.Entry<Long, Block>> it = blocksMap.entrySet().iterator();
        while (it.hasNext()) {
            Block block = it.next().getValue();
            if (block.index.endOffset() <= nextReadOffset) {
                it.remove();
                block.markReadCompleted();
            } else {
                break;
            }
        }
        for (Block block : ctx.blocks) {
            block.release();
        }
        // try readahead to speed up the next read
        afterReadTryReadaheadCf = eventLoop.submit(() -> readahead.tryReadahead(readDataBlock.getCacheAccessType() == BLOCK_CACHE_MISS));
        afterReadTryReadaheadCf.whenComplete((nil, ex) -> {
            Throwable cause = FutureUtil.cause(ex);
            if (cause != null && !isRecoverable(cause)) {
                LOGGER.error("AfterRead failed", ex);
            }
            // help gc
            afterReadTryReadaheadCf = null;
        });
    }

    private CompletableFuture<List<Block>> getBlocks(long startOffset, long endOffset, int maxBytes,
        boolean readahead) {
        GetBlocksContext context = new GetBlocksContext(readahead);
        try {
            getBlocks0(context, startOffset, endOffset, maxBytes);
        } catch (Throwable ex) {
            context.cf.completeExceptionally(ex);
        }
        context.cf.exceptionally(ex -> {
            context.blocks.forEach(Block::release);
            return null;
        });
        return context.cf;
    }

    private void getBlocks0(GetBlocksContext ctx, long startOffset, long endOffset, int maxBytes) {
        Long floorKey = blocksMap.floorKey(startOffset);
        if (floorKey == null && !blocksMap.isEmpty()) {
            if (ctx.readahead) {
                // The user read may be faster than the readahead, and clear the read completed blocks.
                ctx.cf.complete(ctx.blocks);
                return;
            } else {
                LOGGER.error("[BUG] {} cannot find floor block for startOffset={}, the first block={}", this, startOffset, blocksMap.firstEntry());
                throw new AutoMQException("[BUG] cannot find floor block");
            }
        }
        CompletableFuture<Boolean> loadMoreBlocksCf;
        int remainingSize = maxBytes;
        if (floorKey == null || startOffset >= loadedBlockIndexEndOffset) {
            loadMoreBlocksCf = loadMoreBlocksWithoutData(endOffset);
        } else {
            boolean firstBlock = true;
            boolean fulfill = false;
            for (Map.Entry<Long, Block> entry : blocksMap.tailMap(floorKey).entrySet()) {
                Block block = entry.getValue();
                long objectId = block.metadata.objectId();
                if (!objectManager.isObjectExist(objectId)) {
                    // The cached block's object maybe deleted by the compaction. So we need to check the object exist.
                    ctx.cf.completeExceptionally(new ObjectNotExistException(objectId));
                    return;
                }
                DataBlockIndex index = block.index;
                if (!firstBlock || index.startOffset() == startOffset) {
                    remainingSize -= index.size();
                }
                if (firstBlock) {
                    firstBlock = false;
                }
                // after read the data will be return to the cache, so we need to reload the data every time
                block = block.newBlockWithData(ctx.readahead);
                ctx.blocks.add(block);
                if ((endOffset != -1L && index.endOffset() >= endOffset) || remainingSize <= 0) {
                    fulfill = true;
                    break;
                }
            }
            if (fulfill) {
                ctx.cf.complete(ctx.blocks);
                return;
            } else {
                loadMoreBlocksCf = loadMoreBlocksWithoutData(endOffset);
            }
        }
        int finalRemainingSize = remainingSize;
        // use async to prevent recursive call cause stack overflow
        loadMoreBlocksCf.thenAcceptAsync(moreBlocks -> {
            if (ctx.readahead) {
                // If #loadMoreBlocksWithoutData result is empty, it means the stream is already loads to the end.
                if (!moreBlocks) {
                    ctx.cf.complete(ctx.blocks);
                    return;
                }
            } else {
                if (!moreBlocks && endOffset > loadedBlockIndexEndOffset) {
                    String errMsg = String.format("[BUG] streamId=%s expect load blocks to endOffset=%s, " + "current loadedBlockIndexEndOffset=%s", streamId, endOffset, loadedBlockIndexEndOffset);
                    ctx.cf.completeExceptionally(new AutoMQException(errMsg));
                    return;
                }
            }
            long nextStartOffset = ctx.blocks.isEmpty() ? startOffset : ctx.blocks.get(ctx.blocks.size() - 1).index.endOffset();
            getBlocks0(ctx, nextStartOffset, endOffset, finalRemainingSize);
        }, eventLoop).exceptionally(ex -> {
            ctx.cf.completeExceptionally(ex);
            return null;
        });
    }

    /**
     * Load more block indexes
     *
     * @return whether load more blocks
     */
    private CompletableFuture<Boolean> loadMoreBlocksWithoutData(long endOffset) {
        long oldLoadedBlockIndexEndOffset = loadedBlockIndexEndOffset;
        return loadMoreBlocksWithoutData0(endOffset).thenApply(nil -> loadedBlockIndexEndOffset != oldLoadedBlockIndexEndOffset);
    }

    private CompletableFuture<Void> loadMoreBlocksWithoutData0(long endOffset) {
        if (inflightLoadIndexCf != null) {
            return inflightLoadIndexCf.thenCompose(rst -> loadMoreBlocksWithoutData0(endOffset));
        }
        if (endOffset != -1L && endOffset <= loadedBlockIndexEndOffset) {
            return CompletableFuture.completedFuture(null);
        }
        long currentBlocksEpoch = blocksEpoch;
        inflightLoadIndexCf = new CompletableFuture<>();
        long nextLoadingOffset = calWindowBlocksEndOffset();
        AtomicLong nextFindStartOffset = new AtomicLong(nextLoadingOffset);
        TimerUtil time = new TimerUtil();
        // 1. get objects
        CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, nextLoadingOffset, endOffset, GET_OBJECT_STEP);
        // 2. get block indexes from objects
        CompletableFuture<Void> findBlockIndexesCf = getObjectsCf.whenComplete((rst, ex) -> {
            StorageOperationStats.getInstance().getIndicesTimeGetObjectStats.record(time.elapsedAndResetAs(TimeUnit.NANOSECONDS));
        }).thenComposeAsync(objects -> {
            CompletableFuture<Void> prevCf = CompletableFuture.completedFuture(null);
            for (S3ObjectMetadata objectMetadata : objects) {
                // the object reader will be release in the whenComplete
                @SuppressWarnings("resource") ObjectReader objectReader = objectReaderFactory.get(objectMetadata);
                // invoke basicObjectInfo to warm up the objectReader
                objectReader.basicObjectInfo();
                prevCf = prevCf.thenCompose(nil -> {
                    if (currentBlocksEpoch != blocksEpoch) {
                        // The blocks are reset, we need to stop the load
                        return CompletableFuture.completedFuture(null);
                    }
                    return objectReader.find(streamId, nextFindStartOffset.get(), -1L, Integer.MAX_VALUE).thenAcceptAsync(findRst -> {
                        if (currentBlocksEpoch != blocksEpoch) {
                            // The blocks are reset, we need to stop the load
                            return;
                        }
                        findRst.streamDataBlocks().forEach(streamDataBlock -> {
                            DataBlockIndex index = streamDataBlock.dataBlockIndex();
                            Block block = new Block(objectMetadata, index);
                            if (!putBlock(block)) {
                                // After object compaction, the blocks get from different objectManager#getObjects maybe not continuous.
                                throw new BlockNotContinuousException();
                            }
                            nextFindStartOffset.set(streamDataBlock.getEndOffset());
                        });
                    }, eventLoop);
                }).whenComplete((nil, ex) -> objectReader.release());
            }
            return prevCf;
        }, eventLoop);
        findBlockIndexesCf.whenCompleteAsync((nil, ex) -> {
            if (ex != null) {
                inflightLoadIndexCf.completeExceptionally(ex);
                return;
            }
            StorageOperationStats.getInstance().getIndicesTimeFindIndexStats.record(time.elapsedAs(TimeUnit.NANOSECONDS));
            CompletableFuture<Void> cf = inflightLoadIndexCf;
            inflightLoadIndexCf = null;
            cf.complete(null);
        }, eventLoop);
        return inflightLoadIndexCf;
    }

    private long calWindowBlocksEndOffset() {
        Map.Entry<Long, Block> lastBlockIndex = blocksMap.lastEntry();
        if (lastBlockIndex != null) {
            return Math.max(lastBlockIndex.getValue().index.endOffset(), nextReadOffset);
        }
        return nextReadOffset;
    }

    private void handleBlockFree(Block block) {
        if (closed) {
            return;
        }
        Block blockInMap = blocksMap.get(block.index.startOffset());
        if (block == blockInMap) {
            // The unread block is evicted; It means the cache is full, we need to reset the readahead.
            readahead.reset();
            READAHEAD_RESET_LOG_SUPPRESSOR.warn("The unread block is evicted, please increase the block cache size");
        }
    }

    private void resetBlocks() {
        List<Block> blocks = new ArrayList<>(blocksMap.values());
        // The Block#markRead will immediately invoke after the Block is removed.
        blocksMap.clear();
        blocks.forEach(Block::markReadCompleted);
        lastBlock = null;
        loadedBlockIndexEndOffset = 0L;
        blocksEpoch++;
        BLOCKS_RESET_LOG_SUPPRESSOR.info("The stream reader's blocks are reset, cause of the object compaction");
    }

    /**
     * Put block into the blocks
     *
     * @param block {@link Block}
     * @return if the block is continuous to the last block, it will return true
     */
    private boolean putBlock(Block block) {
        if (lastBlock == null) {
            // The first block should contain the nextReadOffset
            if (!(block.index.startOffset() <= nextReadOffset && block.index.endOffset() > nextReadOffset)) {
                LOGGER.error("[BUG] The first block should contain the nextReadOffset, block={} nextReadOffset={}", block.index, nextReadOffset);
                return false;
            }
        } else if (lastBlock.index.endOffset() != block.index.startOffset()) {
            return false;
        }
        lastBlock = block;
        blocksMap.put(block.index.startOffset(), block);
        loadedBlockIndexEndOffset = block.index.endOffset();
        return true;
    }

    @Override
    public String toString() {
        return "StreamReader{" + "reading=" + reading + ", nextReadOffset=" + nextReadOffset + '}';
    }

    private static boolean isRecoverable(Throwable cause) {
        return cause instanceof ObjectNotExistException || cause instanceof NoSuchKeyException || cause instanceof BlockNotContinuousException;
    }

    static class GetBlocksContext {
        final List<Block> blocks = new ArrayList<>();
        final CompletableFuture<List<Block>> cf = new CompletableFuture<>();
        final boolean readahead;

        public GetBlocksContext(boolean readahead) {
            this.readahead = readahead;
        }
    }

    static class ReadContext {
        final List<StreamRecordBatch> records = new LinkedList<>();
        final List<Block> blocks = new ArrayList<>();
        final CompletableFuture<ReadDataBlock> cf = new CompletableFuture<>();
        CacheAccessType accessType = BLOCK_CACHE_HIT;
        final TimerUtil start = new TimerUtil();
    }

    class Block {
        final S3ObjectMetadata metadata;
        final DataBlockIndex index;
        DataBlock data;
        DataBlock.FreeListenerHandle freeListenerHandle;

        CompletableFuture<Void> loadCf;
        Throwable exception;
        boolean released = false;
        boolean readCompleted = false;

        public Block(S3ObjectMetadata metadata, DataBlockIndex index) {
            this.metadata = metadata;
            this.index = index;
        }

        // TODO: use different Block type, cause of the returned Block shouldn't have markReadCompleted method
        public Block newBlockWithData(boolean readahead) {
            // We need to create a new block with consistent data to avoid duplicated release or leak,
            // cause of the loaded data maybe evicted and reloaded.
            Block newBlock = new Block(metadata, index);
            ObjectReader objectReader = objectReaderFactory.get(metadata);
            DataBlockCache.GetOptions getOptions = DataBlockCache.GetOptions.builder().readahead(readahead).build();
            loadCf = dataBlockCache.getBlock(getOptions, objectReader, index).thenAccept(newData -> {
                newBlock.data = newData;
                if (!readCompleted && data != newData) {
                    // the data block is first loaded or evict & reload
                    if (data != null) {
                        freeListenerHandle.close();
                    }
                    data = newData;
                    newData.markUnread();
                    freeListenerHandle = data.registerFreeListener(b -> handleBlockFree(this));
                }
            }).exceptionally(ex -> {
                exception = ex;
                newBlock.exception = ex;
                return null;
            }).whenComplete((nil, ex) -> objectReader.release());
            newBlock.loadCf = loadCf;
            return newBlock;
        }

        public void release() {
            if (released) {
                return;
            }
            released = true;
            loadCf.whenComplete((nil, ex) -> {
                if (data != null) {
                    data.release();
                }
            });
        }

        /**
         * The <code>Block#markReadCompleted</code> should be invoked after the Block was removed from <code>blocksMap</code>.
         */
        public void markReadCompleted() {
            readCompleted = true;
            if (data != null) {
                data.markRead();
                freeListenerHandle.close();
            }
        }

        @Override
        public String toString() {
            return "Block{" +
                "metadata=" + metadata +
                ", index=" + index +
                ", data=" + data +
                ", exception=" + exception +
                ", released=" + released +
                '}';
        }
    }

    class Readahead {
        long nextReadaheadOffset;
        int nextReadaheadSize = READAHEAD_SIZE_UNIT;
        long readaheadMarkOffset;
        long resetTimestamp;
        boolean requireReset;
        volatile CompletableFuture<Void> inflightReadaheadCf;
        private int cacheMissCount;

        public void tryReadahead(boolean cacheMiss) {
            if (time.milliseconds() - resetTimestamp < READAHEAD_RESET_COLD_DOWN_MILLS) {
                // skip readahead when readahead is in cold down
                return;
            }
            cacheMissCount += cacheMiss ? 1 : 0;
            if (inflightReadaheadCf != null) {
                return;
            }
            nextReadaheadSize = Math.min(nextReadaheadSize + cacheMissCount * READAHEAD_SIZE_UNIT, MAX_READAHEAD_SIZE);
            cacheMissCount = 0;
            if (requireReset) {
                nextReadaheadOffset = 0L;
                nextReadaheadSize = READAHEAD_SIZE_UNIT;
                readaheadMarkOffset = 0L;
                requireReset = false;
            }
            if (nextReadOffset >= nextReadaheadOffset) {
                nextReadaheadOffset = nextReadOffset;
            } else if (nextReadOffset <= readaheadMarkOffset) {
                // if the user read doesn't reach the readahead mark, we don't need to readahead
                return;
            }
            if (dataBlockCache.available() < nextReadaheadSize + READAHEAD_AVAILABLE_BYTES_THRESHOLD) {
                return;
            }
            readaheadMarkOffset = nextReadaheadOffset;
            inflightReadaheadCf = getBlocks(nextReadaheadOffset, -1L, nextReadaheadSize, true).thenAccept(blocks -> {
                nextReadaheadOffset = blocks.isEmpty() ? nextReadaheadOffset : blocks.get(blocks.size() - 1).index.endOffset();
                blocks.forEach(Block::release);
            });
            // For get block indexes and load data block are sync success,
            // the whenComplete will invoke first before assign CompletableFuture to inflightReadaheadCf
            inflightReadaheadCf.whenComplete((nil, ex) -> {
                Throwable cause = FutureUtil.cause(ex);
                if (cause != null && !isRecoverable(cause)) {
                    LOGGER.error("Readahead failed", ex);
                }
                inflightReadaheadCf = null;
            });
        }

        public void reset() {
            requireReset = true;
            resetTimestamp = time.milliseconds();
        }
    }

}
