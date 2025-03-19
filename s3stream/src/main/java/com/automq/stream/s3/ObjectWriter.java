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

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.WRITE_DATA_BLOCK_HEADER;
import static com.automq.stream.s3.ByteBufAlloc.WRITE_FOOTER;
import static com.automq.stream.s3.ByteBufAlloc.WRITE_INDEX_BLOCK;

/**
 * Write stream records to a single object.
 */
public interface ObjectWriter {

    byte DATA_BLOCK_MAGIC = 0x5A;
    // TODO: first n bit is the compressed flag
    byte DATA_BLOCK_DEFAULT_FLAG = 0x02;

    static ObjectWriter writer(long objectId, ObjectStorage objectStorage, int blockSizeThreshold,
        int partSizeThreshold, ObjectStorage.WriteOptions writeOptions) {
        return new DefaultObjectWriter(objectId, objectStorage, blockSizeThreshold, partSizeThreshold, writeOptions);
    }

    static ObjectWriter writer(long objectId, ObjectStorage objectStorage, int blockSizeThreshold,
        int partSizeThreshold) {
        return writer(objectId, objectStorage, blockSizeThreshold, partSizeThreshold, new ObjectStorage.WriteOptions());
    }

    static ObjectWriter noop(long objectId) {
        return new NoopObjectWriter(objectId);
    }

    /**
     * Write records to the object. When close the object, ObjectWriter will generate the index block and footer.
     */
    default void write(long streamId, List<StreamRecordBatch> records) {
        throw new UnsupportedOperationException();
    }

    /**
     * Link the component object as a part to the composite object.
     */
    default void addComponent(S3ObjectMetadata partObjectMetadata, List<DataBlockIndex> partObjectIndexes) {
        throw new UnsupportedOperationException();
    }

    CompletableFuture<Void> close();

    List<ObjectStreamRange> getStreamRanges();

    long size();

    short bucketId();

    class DefaultObjectWriter implements ObjectWriter {

        private final int blockSizeThreshold;
        private final int partSizeThreshold;
        private final List<DataBlock> waitingUploadBlocks;
        private final List<DataBlock> completedBlocks;
        private final Writer writer;
        private int waitingUploadBlocksSize;
        private IndexBlock indexBlock;
        private long size;

        private long lastStreamId = Constants.NOOP_STREAM_ID;
        private long lastEndOffset = Constants.NOOP_OFFSET;

        /**
         * Create a new object writer.
         *
         * @param objectId           object id
         * @param objectStorage      S3 operator
         * @param blockSizeThreshold the max size of a block
         * @param partSizeThreshold  the max size of a part. If it is smaller than {@link Writer#MIN_PART_SIZE}, it will be set to {@link Writer#MIN_PART_SIZE}.
         * @param writeOptions       the object storage write options
         */
        public DefaultObjectWriter(long objectId, ObjectStorage objectStorage, int blockSizeThreshold,
            int partSizeThreshold, ObjectStorage.WriteOptions writeOptions) {
            String objectKey = ObjectUtils.genKey(0, objectId);
            this.blockSizeThreshold = blockSizeThreshold;
            this.partSizeThreshold = Math.max(Writer.MIN_PART_SIZE, partSizeThreshold);
            waitingUploadBlocks = new LinkedList<>();
            completedBlocks = new LinkedList<>();
            writer = objectStorage.writer(writeOptions, objectKey);
        }

        public synchronized void write(long streamId, List<StreamRecordBatch> records) {
            check(streamId, records);
            List<List<StreamRecordBatch>> blocks = groupByBlock(records);
            for (List<StreamRecordBatch> blockRecords : blocks) {
                DataBlock block = new DataBlock(streamId, blockRecords);
                waitingUploadBlocks.add(block);
                waitingUploadBlocksSize += block.size();
            }
            if (waitingUploadBlocksSize >= partSizeThreshold) {
                tryUploadPart();
            }
        }

        private void check(long streamId, List<StreamRecordBatch> records) {
            if (records.isEmpty()) {
                return;
            }
            long recordsEndOffset = records.get(records.size() - 1).getLastOffset();
            if (lastStreamId == Constants.NOOP_STREAM_ID) {
                lastStreamId = streamId;
                lastEndOffset = recordsEndOffset;
                return;
            }
            if (lastStreamId > streamId) {
                throw new IllegalArgumentException(String.format("The incoming streamId=%s is less than last streamId=%s", streamId, lastStreamId));
            } else if (lastStreamId == streamId) {
                long recordsStartOffset = records.get(0).getBaseOffset();
                if (recordsStartOffset < lastEndOffset) {
                    throw new IllegalArgumentException(String.format("The incoming streamId=%s startOffset=%s is less than lastEndOffset=%s",
                        streamId, recordsStartOffset, lastEndOffset));
                } else {
                    lastEndOffset = recordsEndOffset;
                }
            } else {
                lastStreamId = streamId;
                lastEndOffset = recordsEndOffset;
            }
        }

        private List<List<StreamRecordBatch>> groupByBlock(List<StreamRecordBatch> records) {
            List<List<StreamRecordBatch>> blocks = new LinkedList<>();
            List<StreamRecordBatch> blockRecords = new ArrayList<>(records.size());
            for (StreamRecordBatch record : records) {
                size += record.size();
                blockRecords.add(record);
                if (size >= blockSizeThreshold) {
                    blocks.add(blockRecords);
                    blockRecords = new ArrayList<>(records.size());
                    size = 0;
                }
            }
            if (!blockRecords.isEmpty()) {
                blocks.add(blockRecords);
            }
            return blocks;
        }

        private void tryUploadPart() {
            for (; ; ) {
                List<DataBlock> uploadBlocks = new ArrayList<>(waitingUploadBlocks.size());
                boolean partFull = false;
                int size = 0;
                for (DataBlock block : waitingUploadBlocks) {
                    uploadBlocks.add(block);
                    size += block.size();
                    if (size >= partSizeThreshold) {
                        partFull = true;
                        break;
                    }
                }
                if (partFull) {
                    CompositeByteBuf partBuf = ByteBufAlloc.compositeByteBuffer();
                    for (DataBlock block : uploadBlocks) {
                        waitingUploadBlocksSize -= block.size();
                        partBuf.addComponent(true, block.buffer());
                    }
                    writer.write(partBuf);
                    completedBlocks.addAll(uploadBlocks);
                    waitingUploadBlocks.removeIf(uploadBlocks::contains);
                } else {
                    break;
                }
            }
        }

        public synchronized CompletableFuture<Void> close() {
            CompositeByteBuf buf = ByteBufAlloc.compositeByteBuffer();
            for (DataBlock block : waitingUploadBlocks) {
                buf.addComponent(true, block.buffer());
                completedBlocks.add(block);
            }
            waitingUploadBlocks.clear();
            indexBlock = new IndexBlock();
            buf.addComponent(true, indexBlock.buffer());
            Footer footer = new Footer(indexBlock.position(), indexBlock.size());
            buf.addComponent(true, footer.buffer());
            writer.write(buf.duplicate());
            size = indexBlock.position() + indexBlock.size() + footer.size();
            return writer.close();
        }

        public List<ObjectStreamRange> getStreamRanges() {
            List<ObjectStreamRange> streamRanges = new LinkedList<>();
            ObjectStreamRange lastStreamRange = null;
            for (DataBlock block : completedBlocks) {
                ObjectStreamRange streamRange = block.getStreamRange();
                if (lastStreamRange == null || lastStreamRange.getStreamId() != streamRange.getStreamId()) {
                    if (lastStreamRange != null) {
                        streamRanges.add(lastStreamRange);
                    }
                    lastStreamRange = new ObjectStreamRange();
                    lastStreamRange.setStreamId(streamRange.getStreamId());
                    lastStreamRange.setEpoch(streamRange.getEpoch());
                    lastStreamRange.setStartOffset(streamRange.getStartOffset());
                }
                lastStreamRange.setEndOffset(streamRange.getEndOffset());
            }
            if (lastStreamRange != null) {
                streamRanges.add(lastStreamRange);
            }
            return streamRanges;
        }

        public long size() {
            return size;
        }

        @Override
        public short bucketId() {
            return writer.bucketId();
        }

        class IndexBlock {
            private final ByteBuf buf;
            private final long position;

            public IndexBlock() {
                long nextPosition = 0;
                int indexBlockSize = DataBlockIndex.BLOCK_INDEX_SIZE * completedBlocks.size();
                buf = ByteBufAlloc.byteBuffer(indexBlockSize, WRITE_INDEX_BLOCK);
                for (DataBlock block : completedBlocks) {
                    ObjectStreamRange streamRange = block.getStreamRange();
                    new DataBlockIndex(streamRange.getStreamId(), streamRange.getStartOffset(), (int) (streamRange.getEndOffset() - streamRange.getStartOffset()),
                        block.recordCount(), nextPosition, block.size()).encode(buf);
                    nextPosition += block.size();
                }
                position = nextPosition;
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
    }

    class DataBlock {
        public static final int BLOCK_HEADER_SIZE = 1 /* magic */ + 1/* flag */ + 4 /* record count*/ + 4 /* data length */;
        private final CompositeByteBuf encodedBuf;
        private final ObjectStreamRange streamRange;
        private final int recordCount;
        private final int size;

        public DataBlock(long streamId, List<StreamRecordBatch> records) {
            this.recordCount = records.size();
            this.encodedBuf = ByteBufAlloc.compositeByteBuffer();
            ByteBuf header = ByteBufAlloc.byteBuffer(BLOCK_HEADER_SIZE, WRITE_DATA_BLOCK_HEADER);
            header.writeByte(DATA_BLOCK_MAGIC);
            header.writeByte(DATA_BLOCK_DEFAULT_FLAG);
            header.writeInt(recordCount);
            header.writeInt(0); // data length
            encodedBuf.addComponent(true, header);
            records.forEach(r -> encodedBuf.addComponent(true, r.encoded().retain()));
            this.size = encodedBuf.readableBytes();
            encodedBuf.setInt(BLOCK_HEADER_SIZE - 4, size - BLOCK_HEADER_SIZE);
            this.streamRange = new ObjectStreamRange(streamId, records.get(0).getEpoch(), records.get(0).getBaseOffset(), records.get(records.size() - 1).getLastOffset(), size);
        }

        public int size() {
            return size;
        }

        public int recordCount() {
            return recordCount;
        }

        public ObjectStreamRange getStreamRange() {
            return streamRange;
        }

        public ByteBuf buffer() {
            return encodedBuf.duplicate();
        }
    }

    class Footer {
        public static final int FOOTER_SIZE = 48;
        public static final long MAGIC = 0x88e241b785f4cff7L;
        private final ByteBuf buf;

        public Footer(long indexStartPosition, int indexBlockLength) {
            buf = ByteBufAlloc.byteBuffer(FOOTER_SIZE, WRITE_FOOTER);
            // start position of index block
            buf.writeLong(indexStartPosition);
            // size of index block
            buf.writeInt(indexBlockLength);
            // reserved for future
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

    class NoopObjectWriter implements ObjectWriter {
        private final long objectId;

        public NoopObjectWriter(long objectId) {
            this.objectId = objectId;
        }

        @Override
        public void write(long streamId, List<StreamRecordBatch> records) {
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public List<ObjectStreamRange> getStreamRanges() {
            return Collections.emptyList();
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public short bucketId() {
            return 0;
        }
    }

}
