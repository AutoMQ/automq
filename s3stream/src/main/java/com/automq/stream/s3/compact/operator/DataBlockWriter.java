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

package com.automq.stream.s3.compact.operator;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.utils.CompactionUtils;
import com.automq.stream.s3.compact.utils.GroupByLimitPredicate;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.stats.CompactionStats;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.s3.operator.Writer;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.STREAM_OBJECT_COMPACTION_WRITE;
import static com.automq.stream.s3.ByteBufAlloc.STREAM_SET_OBJECT_COMPACTION_READ;
import static com.automq.stream.s3.operator.Writer.MIN_PART_SIZE;

//TODO: refactor to reduce duplicate code with ObjectWriter
public class DataBlockWriter {
    private final int partSizeThreshold;
    private final List<StreamDataBlock> waitingUploadBlocks;
    private final Map<StreamDataBlock, CompletableFuture<Void>> waitingUploadBlockCfs;
    private final List<StreamDataBlock> completedBlocks;
    private final Writer writer;
    private final long objectId;
    private IndexBlock indexBlock;
    private long nextDataBlockPosition;
    private long size;

    public DataBlockWriter(long objectId, ObjectStorage objectStorage, int partSizeThreshold) {
        this.objectId = objectId;
        String objectKey = ObjectUtils.genKey(0, objectId);
        this.partSizeThreshold = Math.max(MIN_PART_SIZE, partSizeThreshold);
        waitingUploadBlocks = new LinkedList<>();
        waitingUploadBlockCfs = new ConcurrentHashMap<>();
        completedBlocks = new LinkedList<>();
        writer = objectStorage.writer(
            new WriteOptions().allocType(STREAM_SET_OBJECT_COMPACTION_READ).throttleStrategy(ThrottleStrategy.COMPACTION),
            objectKey);
    }

    public long getObjectId() {
        return objectId;
    }

    public void write(StreamDataBlock dataBlock) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.whenComplete((nil, ex) -> CompactionStats.getInstance().compactionWriteSizeStats.add(MetricsLevel.INFO, dataBlock.getBlockSize()));
        waitingUploadBlockCfs.put(dataBlock, cf);
        waitingUploadBlocks.add(dataBlock);
        long waitingUploadSize = waitingUploadBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum();
        if (waitingUploadSize >= partSizeThreshold) {
            uploadWaitingList();
        }
    }

    public CompletableFuture<Void> forceUpload() {
        uploadWaitingList();
        writer.copyOnWrite();
        return CompletableFuture.allOf(waitingUploadBlockCfs.values().toArray(new CompletableFuture[0]));
    }

    private void uploadWaitingList() {
        CompositeByteBuf buf = groupWaitingBlocks();
        List<StreamDataBlock> blocks = new LinkedList<>(waitingUploadBlocks);
        writer.write(buf).whenComplete((v, ex) -> {
            for (StreamDataBlock block : blocks) {
                waitingUploadBlockCfs.computeIfPresent(block, (k, cf) -> {
                    if (ex != null) {
                        cf.completeExceptionally(ex);
                    } else {
                        cf.complete(null);
                    }
                    return null;
                });
            }
        });
        if (writer.hasBatchingPart()) {
            // prevent blocking on part that's waiting for batch when force upload waiting list
            for (StreamDataBlock block : blocks) {
                waitingUploadBlockCfs.computeIfPresent(block, (k, cf) -> {
                    cf.complete(null);
                    return null;
                });
            }
        }
        waitingUploadBlocks.clear();
    }

    public CompletableFuture<Void> close() {
        CompositeByteBuf buf = groupWaitingBlocks();
        List<StreamDataBlock> blocks = new LinkedList<>(waitingUploadBlocks);
        waitingUploadBlocks.clear();
        indexBlock = new IndexBlock();
        buf.addComponent(true, indexBlock.buffer());
        Footer footer = new Footer();
        buf.addComponent(true, footer.buffer());
        writer.write(buf.duplicate());
        size = indexBlock.position() + indexBlock.size() + footer.size();
        return writer.close().thenAccept(nil -> {
            for (StreamDataBlock block : blocks) {
                waitingUploadBlockCfs.computeIfPresent(block, (k, cf) -> {
                    cf.complete(null);
                    return null;
                });
            }
        });
    }

    private CompositeByteBuf groupWaitingBlocks() {
        CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
        for (StreamDataBlock block : waitingUploadBlocks) {
            buf.addComponent(true, block.getAndReleaseData());
            completedBlocks.add(block);
            nextDataBlockPosition += block.getBlockSize();
        }
        return buf;
    }

    public CompletableFuture<Void> release() {
        // release buffer that is batching for upload
        return writer.release();
    }

    public long objectId() {
        return objectId;
    }

    public long size() {
        return size;
    }

    public short bucketId() {
        return writer.bucketId();
    }

    class IndexBlock {
        private static final int DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD = 1024 * 1024; // 1MiB
        private final ByteBuf buf;
        private final long position;

        public IndexBlock() {
            position = nextDataBlockPosition;

            List<DataBlockIndex> dataBlockIndices = CompactionUtils.buildDataBlockIndicesFromGroup(
                CompactionUtils.groupStreamDataBlocks(completedBlocks, new GroupByLimitPredicate(DEFAULT_DATA_BLOCK_GROUP_SIZE_THRESHOLD)));
            buf = ByteBufAlloc.byteBuffer(dataBlockIndices.size() * DataBlockIndex.BLOCK_INDEX_SIZE, ByteBufAlloc.STREAM_SET_OBJECT_COMPACTION_WRITE);
            for (DataBlockIndex dataBlockIndex : dataBlockIndices) {
                dataBlockIndex.encode(buf);
            }
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public long position() {
            return position;
        }

        public int size() {
            return buf.readableBytes();
        }
    }

    class Footer {
        private static final int FOOTER_SIZE = 48;
        private static final long MAGIC = 0x88e241b785f4cff7L;
        private final ByteBuf buf;

        public Footer() {
            buf = ByteBufAlloc.byteBuffer(FOOTER_SIZE, STREAM_OBJECT_COMPACTION_WRITE);
            buf.writeLong(indexBlock.position());
            buf.writeInt(indexBlock.size());
            buf.writeZero(40 - 8 - 4);
            buf.writeLong(MAGIC);
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public int size() {
            return FOOTER_SIZE;
        }

    }
}
