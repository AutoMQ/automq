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

package kafka.log.s3.compact.operator;

import io.netty.buffer.ByteBuf;
import kafka.log.s3.ObjectReader;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

//TODO: refactor to reduce duplicate code with ObjectWriter
public class DataBlockReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectReader.class);
    private final S3ObjectMetadata metadata;
    private final String objectKey;
    private final S3Operator s3Operator;
    private final CompletableFuture<List<StreamDataBlock>> indexBlockCf = new CompletableFuture<>();

    public DataBlockReader(S3ObjectMetadata metadata, S3Operator s3Operator) {
        this.metadata = metadata;
        this.objectKey = metadata.key();
        this.s3Operator = s3Operator;
    }

    public CompletableFuture<List<StreamDataBlock>> getDataBlockIndex() {
        return indexBlockCf;
    }

    public void parseDataBlockIndex() {
        parseDataBlockIndex(Math.max(0, metadata.getObjectSize() - 1024 * 1024));
    }

    public void parseDataBlockIndex(long startPosition) {
        s3Operator.rangeRead(objectKey, startPosition, metadata.getObjectSize())
                .thenAccept(buf -> {
                    try {
                        indexBlockCf.complete(IndexBlock.parse(buf, metadata.getObjectSize(), metadata.getObjectId()));
                    } catch (IndexBlockParseException ex) {
                        parseDataBlockIndex(ex.indexBlockPosition);
                    }
                }).exceptionally(ex -> {
                    // unrecoverable error, possibly read on a deleted object
                    LOGGER.warn("s3 range read from {} [{}, {}) failed, ex", objectKey, startPosition, metadata.getObjectSize(), ex);
                    indexBlockCf.completeExceptionally(ex);
                    return null;
                });
    }

    public CompletableFuture<List<DataBlock>> readBlocks(List<DataBlockIndex> blockIndices) {
        CompletableFuture<ByteBuf> rangeReadCf = s3Operator.rangeRead(objectKey, blockIndices.get(0).startPosition(),
                blockIndices.get(blockIndices.size() - 1).endPosition());
        return rangeReadCf.thenApply(buf -> {
            List<DataBlock> dataBlocks = new ArrayList<>();
            parseDataBlocks(buf, blockIndices, dataBlocks);
            return dataBlocks;
        });
    }

    private void parseDataBlocks(ByteBuf buf, List<DataBlockIndex> blockIndices, List<DataBlock> dataBlocks) {
        for (DataBlockIndex blockIndexEntry : blockIndices) {
            int blockSize = blockIndexEntry.size;
            ByteBuf blockBuf = buf.slice(buf.readerIndex(), blockSize);
            buf.skipBytes(blockSize);
            dataBlocks.add(new DataBlock(blockBuf, blockIndexEntry.recordCount));
        }
    }

    static class IndexBlock {
        static List<StreamDataBlock> parse(ByteBuf objectTailBuf, long objectSize, long objectId) throws IndexBlockParseException {
            long indexBlockPosition = objectTailBuf.getLong(objectTailBuf.readableBytes() - 48);
            int indexBlockSize = objectTailBuf.getInt(objectTailBuf.readableBytes() - 40);
            if (indexBlockPosition + objectTailBuf.readableBytes() < objectSize) {
                throw new IndexBlockParseException(indexBlockPosition);
            } else {
                int indexRelativePosition = objectTailBuf.readableBytes() - (int) (objectSize - indexBlockPosition);
                ByteBuf indexBlockBuf = objectTailBuf.slice(objectTailBuf.readerIndex() + indexRelativePosition, indexBlockSize);
                int blockCount = indexBlockBuf.readInt();
                ByteBuf blocks = indexBlockBuf.slice(indexBlockBuf.readerIndex(), blockCount * 16);
                List<DataBlockIndex> dataBlockIndices = new ArrayList<>();
                for (int i = 0; i < blockCount; i++) {
                    long blockPosition = blocks.readLong();
                    int blockSize = blocks.readInt();
                    int recordCount = blocks.readInt();
                    dataBlockIndices.add(new DataBlockIndex(i, blockPosition, blockSize, recordCount));
                }
                indexBlockBuf.skipBytes(blockCount * 16);
                ByteBuf streamRanges = indexBlockBuf.slice(indexBlockBuf.readerIndex(), indexBlockBuf.readableBytes());
                List<StreamDataBlock> streamDataBlocks = new ArrayList<>();
                for (int i = 0; i < blockCount; i++) {
                    long streamId = streamRanges.readLong();
                    long startOffset = streamRanges.readLong();
                    int rangeSize = streamRanges.readInt();
                    int blockIndex = streamRanges.readInt();
                    streamDataBlocks.add(new StreamDataBlock(streamId, startOffset, startOffset + rangeSize, blockIndex,
                            objectId, dataBlockIndices.get(i).startPosition, dataBlockIndices.get(i).size, dataBlockIndices.get(i).recordCount));
                }
                return streamDataBlocks;
            }
        }

    }

    static class IndexBlockParseException extends Exception {
        long indexBlockPosition;

        public IndexBlockParseException(long indexBlockPosition) {
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

        public ByteBuf buffer() {
            return buf;
        }
    }

}
