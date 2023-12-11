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

package com.automq.stream.s3;

import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.CloseableIterator;
import com.automq.stream.api.exceptions.ErrorCode;
import com.automq.stream.api.exceptions.StreamClientException;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.ByteBufInputStream;
import com.automq.stream.utils.biniarysearch.IndexBlockOrderedBytes;
import io.netty.buffer.ByteBuf;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.automq.stream.s3.ObjectWriter.Footer.FOOTER_SIZE;
import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;

public class ObjectReader implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectReader.class);
    private final S3ObjectMetadata metadata;
    private final String objectKey;
    private final S3Operator s3Operator;
    private final CompletableFuture<BasicObjectInfo> basicObjectInfoCf;
    private final AtomicInteger refCount = new AtomicInteger(1);

    public ObjectReader(S3ObjectMetadata metadata, S3Operator s3Operator) {
        this.metadata = metadata;
        this.objectKey = metadata.key();
        this.s3Operator = s3Operator;
        this.basicObjectInfoCf = new CompletableFuture<>();
        asyncGetBasicObjectInfo();
    }

    public String objectKey() {
        return objectKey;
    }

    public CompletableFuture<BasicObjectInfo> basicObjectInfo() {
        return basicObjectInfoCf;
    }

    public CompletableFuture<FindIndexResult> find(long streamId, long startOffset, long endOffset) {
        return find(streamId, startOffset, endOffset, Integer.MAX_VALUE);
    }

    public CompletableFuture<FindIndexResult> find(long streamId, long startOffset, long endOffset, int maxBytes) {
        return basicObjectInfoCf.thenApply(basicObjectInfo -> basicObjectInfo.indexBlock().find(streamId, startOffset, endOffset, maxBytes));
    }

    public CompletableFuture<DataBlock> read(DataBlockIndex block) {
        CompletableFuture<ByteBuf> rangeReadCf = s3Operator.rangeRead(objectKey, block.startPosition(), block.endPosition(), ThrottleStrategy.THROTTLE_1);
        return rangeReadCf.thenApply(buf -> new DataBlock(buf, block.recordCount()));
    }

    void asyncGetBasicObjectInfo() {
        int guessIndexBlockSize = 1024 + (int) (metadata.objectSize() / (1024 * 1024 /* 1MB */) * 36 /* index unit size*/);
        asyncGetBasicObjectInfo0(Math.max(0, metadata.objectSize() - guessIndexBlockSize), true);
    }

    private void asyncGetBasicObjectInfo0(long startPosition, boolean firstAttempt) {
        CompletableFuture<ByteBuf> cf = s3Operator.rangeRead(objectKey, startPosition, metadata.objectSize());
        cf.thenAccept(buf -> {
            try {
                BasicObjectInfo basicObjectInfo = BasicObjectInfo.parse(buf, metadata);
                basicObjectInfoCf.complete(basicObjectInfo);
            } catch (IndexBlockParseException ex) {
                asyncGetBasicObjectInfo0(ex.indexBlockPosition, false);
            }
        }).exceptionally(ex -> {
            LOGGER.warn("s3 range read from {} [{}, {}) failed", objectKey, startPosition, metadata.objectSize(), ex);
            // TODO: delay retry.
            if (firstAttempt) {
                asyncGetBasicObjectInfo0(startPosition, false);
            } else {
                basicObjectInfoCf.completeExceptionally(ex);
            }
            return null;
        });
    }

    public ObjectReader retain() {
        refCount.incrementAndGet();
        return this;
    }

    public ObjectReader release() {
        if (refCount.decrementAndGet() == 0) {
            close0();
        }
        return this;
    }

    @Override
    public void close() {
        release();
    }

    public void close0() {
        basicObjectInfoCf.thenAccept(BasicObjectInfo::close);
    }

    /**
     * @param dataBlockSize  The total size of the data blocks, which equals to index start position.
     * @param indexBlock     raw index data.
     * @param blockCount     The number of data blocks in the object.
     * @param indexBlockSize The size of the index blocks.
     */
    public record BasicObjectInfo(long dataBlockSize, IndexBlock indexBlock, int blockCount, int indexBlockSize) {

        public static BasicObjectInfo parse(ByteBuf objectTailBuf, S3ObjectMetadata s3ObjectMetadata) throws IndexBlockParseException {
            long indexBlockPosition = objectTailBuf.getLong(objectTailBuf.readableBytes() - FOOTER_SIZE);
            int indexBlockSize = objectTailBuf.getInt(objectTailBuf.readableBytes() - 40);
            if (indexBlockPosition + objectTailBuf.readableBytes() < s3ObjectMetadata.objectSize()) {
                objectTailBuf.release();
                throw new IndexBlockParseException(indexBlockPosition);
            } else {
                int indexRelativePosition = objectTailBuf.readableBytes() - (int) (s3ObjectMetadata.objectSize() - indexBlockPosition);

                // trim the ByteBuf to avoid extra memory occupy.
                ByteBuf indexBlockBuf = objectTailBuf.slice(objectTailBuf.readerIndex() + indexRelativePosition, indexBlockSize);
                ByteBuf copy = DirectByteBufAlloc.byteBuffer(indexBlockBuf.readableBytes());
                indexBlockBuf.readBytes(copy, indexBlockBuf.readableBytes());
                objectTailBuf.release();
                indexBlockBuf = copy;

                int blockCount = indexBlockBuf.readInt();
                ByteBuf blocks = indexBlockBuf.retainedSlice(indexBlockBuf.readerIndex(), blockCount * 16);
                indexBlockBuf.skipBytes(blockCount * 16);
                ByteBuf streamRanges = indexBlockBuf.retainedSlice(indexBlockBuf.readerIndex(), indexBlockBuf.readableBytes());
                indexBlockBuf.release();
                return new BasicObjectInfo(indexBlockPosition, new IndexBlock(s3ObjectMetadata, blocks, streamRanges), blockCount, indexBlockSize);
            }
        }

        public int size() {
            return indexBlock.size();
        }

        void close() {
            indexBlock.close();
        }
    }

    public static class IndexBlock {
        private final S3ObjectMetadata s3ObjectMetadata;
        private final ByteBuf blocks;
        private final ByteBuf streamRanges;
        private final int size;

        public IndexBlock(S3ObjectMetadata s3ObjectMetadata, ByteBuf blocks, ByteBuf streamRanges) {
            this.s3ObjectMetadata = s3ObjectMetadata;
            this.blocks = blocks;
            this.streamRanges = streamRanges;
            this.size = blocks.readableBytes() + streamRanges.readableBytes();
        }

        public ByteBuf blocks() {
            return blocks.slice();
        }

        public ByteBuf streamRanges() {
            return streamRanges.slice();
        }

        public FindIndexResult find(long streamId, long startOffset, long endOffset) {
            return find(streamId, startOffset, endOffset, Integer.MAX_VALUE);
        }

        public FindIndexResult find(long streamId, long startOffset, long endOffset, int maxBytes) {
            long nextStartOffset = startOffset;
            int nextMaxBytes = maxBytes;
            boolean matched = false;
            boolean isFulfilled = false;
            List<StreamDataBlock> rst = new LinkedList<>();
            IndexBlockOrderedBytes indexBlockOrderedBytes = new IndexBlockOrderedBytes(streamRanges);
            int startIndex = indexBlockOrderedBytes.search(new IndexBlockOrderedBytes.TargetStreamOffset(streamId, startOffset));
            if (startIndex == -1) {
                // mismatched
                return new FindIndexResult(false, nextStartOffset, nextMaxBytes, rst);
            }
            for (int i = startIndex * 24; i < streamRanges.readableBytes(); i += 24) {
                long rangeStreamId = streamRanges.getLong(i);
                long rangeStartOffset = streamRanges.getLong(i + 8);
                long rangeEndOffset = rangeStartOffset + streamRanges.getInt(i + 16);
                int rangeBlockId = streamRanges.getInt(i + 20);
                if (rangeStreamId == streamId) {
                    if (nextStartOffset < rangeStartOffset) {
                        break;
                    }
                    if (rangeEndOffset <= nextStartOffset) {
                        continue;
                    }
                    matched = nextStartOffset == rangeStartOffset;
                    nextStartOffset = rangeEndOffset;
                    long blockPosition = blocks.getLong(rangeBlockId * 16);
                    int blockSize = blocks.getInt(rangeBlockId * 16 + 8);
                    int recordCount = blocks.getInt(rangeBlockId * 16 + 12);
                    rst.add(new StreamDataBlock(streamId, rangeStartOffset, rangeEndOffset, s3ObjectMetadata.objectId(),
                            new DataBlockIndex(rangeBlockId, blockPosition, blockSize, recordCount)));

                    // we consider first block as not matched because we do not know exactly how many bytes are within
                    // the range in first block, as a result we may read one more block than expected.
                    if (matched) {
                        nextMaxBytes -= Math.min(nextMaxBytes, blockSize);
                    }
                    if ((endOffset != NOOP_OFFSET && nextStartOffset >= endOffset) || nextMaxBytes == 0) {
                        isFulfilled = true;
                        break;
                    }
                } else if (matched) {
                    break;
                }
            }
            return new FindIndexResult(isFulfilled, nextStartOffset, nextMaxBytes, rst);
        }

        int size() {
            return size;
        }

        void close() {
            blocks.release();
            streamRanges.release();
        }
    }

    public record FindIndexResult(boolean isFulfilled, long nextStartOffset, int nextMaxBytes, List<StreamDataBlock> streamDataBlocks) {

    }

    public static class IndexBlockParseException extends Exception {
        long indexBlockPosition;

        public IndexBlockParseException(long indexBlockPosition) {
            this.indexBlockPosition = indexBlockPosition;
        }

    }

    public record DataBlockIndex(int blockId, long startPosition, int size, int recordCount) {
        public static final int BLOCK_INDEX_SIZE = 8 + 4 + 4;

        public long endPosition() {
            return startPosition + size;
        }

        @Override
        public String toString() {
            return "DataBlockIndex{" +
                    "blockId=" + blockId +
                    ", startPosition=" + startPosition +
                    ", size=" + size +
                    ", recordCount=" + recordCount +
                    '}';
        }
    }

    public static class DataBlock implements AutoCloseable {
        private final ByteBuf buf;
        private final int recordCount;

        public DataBlock(ByteBuf buf, int recordCount) {
            this.buf = buf;
            this.recordCount = recordCount;
        }

        public CloseableIterator<StreamRecordBatch> iterator() {
            ByteBuf buf = this.buf.duplicate();
            AtomicInteger remainingRecordCount = new AtomicInteger(recordCount);
            // skip magic and flag
            byte magicCode = buf.readByte();
            buf.readByte();

            if (magicCode != ObjectWriter.DATA_BLOCK_MAGIC) {
                LOGGER.error("magic code mismatch, expected {}, actual {}", ObjectWriter.DATA_BLOCK_MAGIC, magicCode);
                throw new RuntimeException("[FATAL] magic code mismatch, data is corrupted");
            }
            // TODO: check flag, use uncompressed stream or compressed stream.
//            DataInputStream in = new DataInputStream(ZstdFactory.wrapForInput(buf.nioBuffer(), (byte) 0, BufferSupplier.NO_CACHING));
            DataInputStream in = new DataInputStream(new ByteBufInputStream(buf.duplicate()));
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    // in.available() is not reliable. ZstdInputStreamNoFinalizer might return 1 when there is no more data.
                    return remainingRecordCount.get() != 0;
                }

                @Override
                public StreamRecordBatch next() {
                    if (remainingRecordCount.decrementAndGet() < 0) {
                        throw new NoSuchElementException();
                    }
                    return StreamRecordBatchCodec.decode(in);
                }

                @Override
                public void close() {
                    try {
                        in.close();
                    } catch (IOException e) {
                        throw new StreamClientException(ErrorCode.UNEXPECTED, "Failed to close object block stream ", e);
                    }
                }
            };
        }

        public int recordCount() {
            return recordCount;
        }

        @Override
        public void close() {
            buf.release();
        }
    }

}
