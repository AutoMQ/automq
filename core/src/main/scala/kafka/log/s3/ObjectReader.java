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

package kafka.log.s3;

import io.netty.buffer.ByteBuf;
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectReader.class);
    private final S3ObjectMetadata metadata;
    private final String objectKey;
    private final S3Operator s3Operator;
    private final CompletableFuture<BasicObjectInfo> basicObjectInfoCf;

    public ObjectReader(S3ObjectMetadata metadata, S3Operator s3Operator) {
        this.metadata = metadata;
        this.objectKey = metadata.key();
        this.s3Operator = s3Operator;
        this.basicObjectInfoCf = new CompletableFuture<>();
        asyncGetBasicObjectInfo();
    }

    public CompletableFuture<BasicObjectInfo> basicObjectInfo() {
        return basicObjectInfoCf;
    }

    public CompletableFuture<List<DataBlockIndex>> find(long streamId, long startOffset, long endOffset) {
        return basicObjectInfoCf.thenApply(basicObjectInfo -> basicObjectInfo.indexBlock().find(streamId, startOffset, endOffset));
    }

    public CompletableFuture<DataBlock> read(DataBlockIndex block) {
        CompletableFuture<ByteBuf> rangeReadCf = s3Operator.rangeRead(objectKey, block.startPosition(), block.endPosition());
        return rangeReadCf.thenApply(buf -> new DataBlock(buf, block.recordCount()));
    }

    private void asyncGetBasicObjectInfo() {
        asyncGetBasicObjectInfo0(Math.max(0, metadata.objectSize() - 1024 * 1024));
    }

    private void asyncGetBasicObjectInfo0(long startPosition) {
        CompletableFuture<ByteBuf> cf = s3Operator.rangeRead(objectKey, startPosition, metadata.objectSize());
        cf.thenAccept(buf -> {
            try {
                BasicObjectInfo basicObjectInfo = BasicObjectInfo.parse(buf, metadata.objectSize());
                basicObjectInfoCf.complete(basicObjectInfo);
            } catch (IndexBlockParseException ex) {
                asyncGetBasicObjectInfo0(ex.indexBlockPosition);
            }
        }).exceptionally(ex -> {
            LOGGER.warn("s3 range read from {} [{}, {}) failed", objectKey, startPosition, metadata.objectSize(), ex);
            // TODO: delay retry.
            asyncGetBasicObjectInfo0(startPosition);
            return null;
        });
    }

    static class BasicObjectInfo {
        /**
         * The total size of the data blocks, which equals to index start position.
         */
        private final long dataBlockSize;
        /**
         * raw index data.
         */
        private final IndexBlock indexBlock;
        /**
         * The number of data blocks in the object.
         */
        private final int blockCount;
        /**
         * The size of the index blocks.
         */
        private final int indexBlockSize;

        public BasicObjectInfo(long dataBlockSize, IndexBlock indexBlock, int blockCount, int indexBlockSize) {
            this.dataBlockSize = dataBlockSize;
            this.indexBlock = indexBlock;
            this.blockCount = blockCount;
            this.indexBlockSize = indexBlockSize;
        }

        public static BasicObjectInfo parse(ByteBuf objectTailBuf, long objectSize) throws IndexBlockParseException {
            long indexBlockPosition = objectTailBuf.getLong(objectTailBuf.readableBytes() - 48);
            int indexBlockSize = objectTailBuf.getInt(objectTailBuf.readableBytes() - 40);
            if (indexBlockPosition + objectTailBuf.readableBytes() < objectSize) {
                throw new IndexBlockParseException(indexBlockPosition);
            } else {
                int indexRelativePosition = objectTailBuf.readableBytes() - (int) (objectSize - indexBlockPosition);
                ByteBuf indexBlockBuf = objectTailBuf.slice(objectTailBuf.readerIndex() + indexRelativePosition, indexBlockSize);
                int blockCount = indexBlockBuf.readInt();
                ByteBuf blocks = indexBlockBuf.slice(indexBlockBuf.readerIndex(), blockCount * 16);
                indexBlockBuf.skipBytes(blockCount * 16);
                ByteBuf streamRanges = indexBlockBuf.slice(indexBlockBuf.readerIndex(), indexBlockBuf.readableBytes());
                return new BasicObjectInfo(indexBlockPosition, new IndexBlock(blocks, streamRanges), blockCount, indexBlockSize);
            }
        }

        public long dataBlockSize() {
            return dataBlockSize;
        }

        public IndexBlock indexBlock() {
            return indexBlock;
        }

        public int blockCount() {
            return blockCount;
        }

        public int indexBlockSize() {
            return indexBlockSize;
        }
    }

    static class IndexBlock {
        private final ByteBuf blocks;
        private final ByteBuf streamRanges;

        public IndexBlock(ByteBuf blocks, ByteBuf streamRanges) {
            this.blocks = blocks;
            this.streamRanges = streamRanges;
        }

        public ByteBuf blocks() {
            return blocks.slice();
        }

        public ByteBuf streamRanges() {
            return streamRanges.slice();
        }

        public List<DataBlockIndex> find(long streamId, long startOffset, long endOffset) {
            // TODO: binary search
            long nextStartOffset = startOffset;
            List<DataBlockIndex> rst = new LinkedList<>();
            for (int i = 0; i < streamRanges.readableBytes(); i += 24) {
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
                    nextStartOffset = rangeEndOffset;
                    long blockPosition = blocks.getLong(rangeBlockId * 16);
                    int blockSize = blocks.getInt(rangeBlockId * 16 + 8);
                    int recordCount = blocks.getInt(rangeBlockId * 16 + 12);
                    rst.add(new DataBlockIndex(rangeBlockId, blockPosition, blockSize, recordCount));
                    if (nextStartOffset >= endOffset) {
                        break;
                    }
                }
            }
            return rst;
        }

    }

    static class IndexBlockParseException extends Exception {
        long indexBlockPosition;

        public IndexBlockParseException(long indexBlockPosition) {
            this.indexBlockPosition = indexBlockPosition;
        }

    }

    static class BasicObjectInfoParseException extends Exception {
        long indexBlockPosition;

        public BasicObjectInfoParseException(long indexBlockPosition) {
            this.indexBlockPosition = indexBlockPosition;
        }

    }


    public static class DataBlockIndex {
        public static final int BLOCK_INDEX_SIZE = 8 + 4 + 4;
        private final int blockId;
        private final long startPosition;
        private final int size;
        private final int recordCount;

        public DataBlockIndex(int blockId, long startPosition, int size, int recordCount) {
            this.blockId = blockId;
            this.startPosition = startPosition;
            this.size = size;
            this.recordCount = recordCount;
        }

        public int blockId() {
            return blockId;
        }

        public long startPosition() {
            return startPosition;
        }

        public long endPosition() {
            return startPosition + size;
        }

        public int recordCount() {
            return recordCount;
        }
    }

    public static class DataBlock {
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
            buf.skipBytes(2);
            // TODO: check flag, use uncompressed stream or compressed stream.
//            DataInputStream in = new DataInputStream(ZstdFactory.wrapForInput(buf.nioBuffer(), (byte) 0, BufferSupplier.NO_CACHING));
            DataInputStream in = new DataInputStream(new ByteBufferInputStream(buf.nioBuffer()));
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
                        throw new KafkaException("Failed to close object block stream", e);
                    }
                }
            };
        }
    }

}
