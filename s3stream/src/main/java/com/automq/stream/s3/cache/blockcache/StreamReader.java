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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.cache.CacheAccessType;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.utils.LogSuppressor;
import io.netty.channel.EventLoop;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EventLoopSafe
public class StreamReader {
    public static final int GET_OBJECT_STEP = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamReader.class);
    private static final int DEFAULT_READAHEAD_SIZE = 1024 * 1024 / 2;
    private static final int MAX_READAHEAD_SIZE = 32 * 1024 * 1024;
    private static final LogSuppressor LOG_SUPPRESSOR = new LogSuppressor(LOGGER, 30000);
    // visible to test
    final NavigableMap<Long, Block> blocksMap = new TreeMap<>();
    final Readahead readahead;
    private final long streamId;
    private final EventLoop eventLoop;
    private final ObjectManager objectManager;
    private final Function<S3ObjectMetadata, ObjectReader> objectReaderFactory;
    private final DataBlockCache dataBlockCache;
    long nextReadOffset;
    long loadedBlockIndexEndOffset = 0L;
    private CompletableFuture<Map<Long, Block>> inflightLoadIndexCf;
    private long lastAccessTimestamp = System.currentTimeMillis();

    public StreamReader(
        long streamId, long nextReadOffset, EventLoop eventLoop,
        ObjectManager objectManager,
        Function<S3ObjectMetadata, ObjectReader> objectReaderFactory,
        DataBlockCache dataBlockCache
    ) {
        this.streamId = streamId;
        this.nextReadOffset = nextReadOffset;
        this.readahead = new Readahead();

        this.eventLoop = eventLoop;
        this.objectManager = objectManager;
        this.objectReaderFactory = objectReaderFactory;
        this.dataBlockCache = dataBlockCache;
    }

    public CompletableFuture<ReadDataBlock> read(long startOffset, long endOffset, int maxBytes) {
        lastAccessTimestamp = System.currentTimeMillis();
        ReadContext readContext = new ReadContext();
        read0(readContext, startOffset, endOffset, maxBytes);
        return readContext.cf.whenComplete((rst, ex) -> {
            if (ex != null) {
                readContext.records.forEach(StreamRecordBatch::release);
            } else {
                afterRead(rst);
            }
        });
    }

    public long nextReadOffset() {
        return nextReadOffset;
    }

    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    public void close() {
        blocksMap.forEach((k, v) -> {
            if (v.data != null) {
                v.data.markRead();
            }
        });
    }

    void read0(ReadContext ctx, long startOffset, long endOffset, int maxBytes) {
        // 1. get blocks
        CompletableFuture<List<Block>> getBlocksCf = getBlocks(startOffset, endOffset, maxBytes);

        // 2. wait block's data loaded
        List<Block> blocks = new ArrayList<>();
        CompletableFuture<Void> loadBlocksCf = getBlocksCf
            .thenCompose(
                blockList -> {
                    blocks.addAll(blockList);
                    return CompletableFuture.allOf(blockList.stream()
                        .map(block -> block.loadCf)
                        .toArray(CompletableFuture[]::new));
                }
            );

        // 3. extract records from blocks
        loadBlocksCf.thenAccept(nil -> {
            Optional<Block> failedBlock = blocks.stream().filter(block -> block.exception != null).findAny();
            if (failedBlock.isPresent()) {
                ctx.cf.completeExceptionally(failedBlock.get().exception);
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
                if (nextStartOffset >= index.endOffset()) {
                    // #getDataBlock will invoke DataBlock#markUnread
                    block.data.markRead();
                }
                ctx.records.addAll(newRecords);
                if (nextStartOffset >= endOffset || remainingSize <= 0) {
                    fulfill = true;
                    break;
                }
            }
            if (fulfill) {
                // TODO: propagate the cache access type
                ctx.cf.complete(new ReadDataBlock(ctx.records, CacheAccessType.BLOCK_CACHE_HIT));
            } else {
                // The DataBlockIndex#size is not precise cause of the data block contains record header and data block header.
                // So we may need to retry read to fulfill the endOffset or maxBytes
                read0(ctx, nextStartOffset, endOffset, remainingSize);
            }
        }).whenComplete((nil, ex) -> blocks.forEach(Block::release));
    }

    void afterRead(ReadDataBlock readDataBlock) {
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
            } else {
                break;
            }
        }
        // try readahead to accelerate the next read
        readahead.tryReadahead();
    }

    private CompletableFuture<List<Block>> getBlocks(long startOffset, long endOffset, int maxBytes) {
        GetBlocksContext context = new GetBlocksContext();
        getBlocks0(context, startOffset, endOffset, maxBytes);
        return context.cf;
    }

    private void getBlocks0(GetBlocksContext ctx, long startOffset, long endOffset, int maxBytes) {
        Long floorKey = blocksMap.floorKey(startOffset);
        CompletableFuture<Map<Long, Block>> loadMoreBlocksCf;
        List<Block> newBlocks = new ArrayList<>();
        int remainingSize = maxBytes;
        if (floorKey == null || startOffset >= loadedBlockIndexEndOffset) {
            loadMoreBlocksCf = loadMoreBlocksWithoutData();
        } else {
            boolean firstBlock = true;
            boolean fulfill = false;
            for (Map.Entry<Long, Block> entry : blocksMap.tailMap(floorKey).entrySet()) {
                DataBlockIndex index = entry.getValue().index;
                if (!firstBlock || index.startOffset() == startOffset) {
                    remainingSize -= index.size();
                }
                if (firstBlock) {
                    firstBlock = false;
                }
                Block block = entry.getValue();
                // after read the data will be return to the cache, so we need reload the data every time
                block.loadData();
                newBlocks.add(block);
                if ((endOffset != -1L && index.endOffset() >= endOffset) || remainingSize <= 0) {
                    fulfill = true;
                    break;
                }
            }
            ctx.blocks.addAll(newBlocks);
            if (fulfill) {
                ctx.cf.complete(ctx.blocks);
                return;
            } else {
                loadMoreBlocksCf = loadMoreBlocksWithoutData();
            }
        }
        int finalRemainingSize = remainingSize;
        loadMoreBlocksCf.thenAccept(rst -> {
            if (rst.isEmpty()) {
                // it's already load to the end
                ctx.cf.complete(ctx.blocks);
            } else {
                long nextStartOffset = ctx.blocks.isEmpty() ? startOffset : ctx.blocks.get(ctx.blocks.size() - 1).index.endOffset();
                getBlocks0(ctx, nextStartOffset, endOffset, finalRemainingSize);
            }
        }).exceptionally(ex -> {
            ctx.cf.completeExceptionally(ex);
            return null;
        });
    }

    /**
     * Load more block indexes
     *
     * @return new block indexes
     */
    private CompletableFuture<Map<Long, Block>> loadMoreBlocksWithoutData() {
        if (inflightLoadIndexCf != null) {
            return inflightLoadIndexCf;
        }
        inflightLoadIndexCf = new CompletableFuture<>();
        long nextLoadingOffset = calWindowBlocksEndOffset();
        AtomicLong nextFindStartOffset = new AtomicLong(nextLoadingOffset);
        Map<Long, Block> newDataBlockIndex = new HashMap<>();
        // 1. get objects
        CompletableFuture<List<S3ObjectMetadata>> getObjectsCf = objectManager.getObjects(streamId, nextLoadingOffset, -1L, GET_OBJECT_STEP);
        // 2. get block indexes from objects
        CompletableFuture<Void> findBlockIndexesCf = getObjectsCf.thenComposeAsync(objects -> {
            CompletableFuture<Void> prevCf = CompletableFuture.completedFuture(null);
            for (S3ObjectMetadata objectMetadata : objects) {
                ObjectReader objectReader = objectReaderFactory.apply(objectMetadata);
                // TODO: warm up the lazy objectReader
                prevCf = prevCf.thenCompose(
                    nil ->
                        objectReader
                            .find(streamId, nextFindStartOffset.get(), -1L, Integer.MAX_VALUE)
                            .thenAcceptAsync(
                                findRst ->
                                    findRst.streamDataBlocks().forEach(streamDataBlock -> {
                                        DataBlockIndex index = streamDataBlock.dataBlockIndex();
                                        Block block = new Block(objectMetadata, index);
                                        blocksMap.put(index.startOffset(), block);
                                        newDataBlockIndex.put(objectMetadata.objectId(), block);
                                        nextFindStartOffset.set(streamDataBlock.getEndOffset());
                                    }),
                                eventLoop
                            ).whenComplete((nil2, ex) -> objectReader.release())
                );
            }
            return prevCf;
        }, eventLoop);
        findBlockIndexesCf.whenCompleteAsync((nil, ex) -> {
            if (ex != null) {
                inflightLoadIndexCf.completeExceptionally(ex);
                return;
            }
            loadedBlockIndexEndOffset = calWindowBlocksEndOffset();
            CompletableFuture<Map<Long, Block>> cf = inflightLoadIndexCf;
            inflightLoadIndexCf = null;
            cf.complete(newDataBlockIndex);
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
        Block blockInMap = blocksMap.get(block.index.startOffset());
        if (block == blockInMap) {
            // The unread block is evicted; It means the cache is full, we need to reset the readahead.
            readahead.reset();
            LOG_SUPPRESSOR.warn("The unread block is evicted, please increase the block cache size");
        }
    }

    static class GetBlocksContext {
        List<Block> blocks = new ArrayList<>();
        CompletableFuture<List<Block>> cf = new CompletableFuture<>();
    }

    static class ReadContext {
        List<StreamRecordBatch> records = new LinkedList<>();
        CacheAccessType accessType = CacheAccessType.BLOCK_CACHE_HIT;
        CompletableFuture<ReadDataBlock> cf = new CompletableFuture<>();
    }

    class Block {
        final S3ObjectMetadata metadata;
        final DataBlockIndex index;
        DataBlock data;
        CompletableFuture<Void> loadCf;
        Throwable exception;

        public Block(S3ObjectMetadata metadata, DataBlockIndex index) {
            this.metadata = metadata;
            this.index = index;
        }

        public void loadData() {
            ObjectReader objectReader = objectReaderFactory.apply(metadata);
            loadCf = dataBlockCache.getBlock(objectReader, index).thenAccept(db -> {
                if (data != db) {
                    // the data block is first loaded or evict & reload
                    data = db;
                    db.markUnread();
                    data.freeFuture().whenComplete((nil, ex) -> handleBlockFree(this));
                }
            }).exceptionally(ex -> {
                exception = ex;
                return null;
            }).whenComplete((nil, ex) -> objectReader.release());
        }

        public void release() {
            if (data != null) {
                data.release();
            }
        }
    }

    class Readahead {
        long nextReadaheadOffset;
        int nextReadaheadSize = DEFAULT_READAHEAD_SIZE;
        long readaheadMarkOffset;
        boolean requireReset;
        private CompletableFuture<Void> inflightReadaheadCf;

        public void tryReadahead() {
            if (inflightReadaheadCf != null) {
                return;
            }
            if (requireReset) {
                nextReadaheadOffset = 0L;
                nextReadaheadSize = DEFAULT_READAHEAD_SIZE;
                readaheadMarkOffset = 0L;
                requireReset = false;
            }
            if (nextReadOffset >= nextReadaheadOffset) {
                // if the user read is beyond the readahead, we need to increase the readahead size
                nextReadaheadOffset = nextReadOffset;
                nextReadaheadSize = Math.min(nextReadaheadSize * 2, MAX_READAHEAD_SIZE);
            } else if (nextReadOffset <= readaheadMarkOffset) {
                // if the user read doesn't reach the readahead mark, we don't need to readahead
                return;
            }
            readaheadMarkOffset = nextReadaheadOffset;
            inflightReadaheadCf = getBlocks(nextReadaheadOffset, -1L, nextReadaheadSize).thenAccept(blocks -> {
                nextReadaheadOffset = blocks.isEmpty() ? nextReadaheadOffset : blocks.get(blocks.size() - 1).index.endOffset();
                for (Block block : blocks) {
                    block.loadCf.whenComplete((nil, ex) -> block.release());
                }
            });
            // For get block indexes and load data block are sync success,
            // the whenComplete will invoke first before assign CompletableFuture to inflightReadaheadCf
            inflightReadaheadCf.whenComplete((nil, ex) -> inflightReadaheadCf = null);
        }

        public void reset() {
            requireReset = true;
        }
    }

}
