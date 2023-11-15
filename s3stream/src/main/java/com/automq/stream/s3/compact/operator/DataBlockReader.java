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

package com.automq.stream.s3.compact.operator;

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.compact.objects.StreamDataBlock;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.S3Operator;
import io.github.bucket4j.Bucket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

//TODO: refactor to reduce duplicate code with ObjectWriter
public class DataBlockReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockReader.class);
    private final S3ObjectMetadata metadata;
    private final String objectKey;
    private final S3Operator s3Operator;
    private final CompletableFuture<List<StreamDataBlock>> indexBlockCf = new CompletableFuture<>();
    private final Bucket throttleBucket;
    private final ScheduledExecutorService bucketCallbackExecutor;

    public DataBlockReader(S3ObjectMetadata metadata, S3Operator s3Operator) {
        this(metadata, s3Operator, null, null);
    }

    public DataBlockReader(S3ObjectMetadata metadata, S3Operator s3Operator, Bucket throttleBucket, ScheduledExecutorService bucketCallbackExecutor) {
        this.metadata = metadata;
        this.objectKey = metadata.key();
        this.s3Operator = s3Operator;
        this.throttleBucket = throttleBucket;
        if (this.throttleBucket != null) {
            this.bucketCallbackExecutor = Objects.requireNonNull(bucketCallbackExecutor);
        } else {
            this.bucketCallbackExecutor = null;
        }
    }

    public CompletableFuture<List<StreamDataBlock>> getDataBlockIndex() {
        return indexBlockCf;
    }

    public void parseDataBlockIndex() {
        parseDataBlockIndex(Math.max(0, metadata.objectSize() - 1024 * 1024));
    }

    public void parseDataBlockIndex(long startPosition) {
        s3Operator.rangeRead(objectKey, startPosition, metadata.objectSize(), ThrottleStrategy.THROTTLE_2)
                .thenAccept(buf -> {
                    try {
                        indexBlockCf.complete(IndexBlock.parse(buf, metadata.objectSize(), metadata.objectId()));
                    } catch (IndexBlockParseException ex) {
                        parseDataBlockIndex(ex.indexBlockPosition);
                    }
                }).exceptionally(ex -> {
                    // unrecoverable error, possibly read on a deleted object
                    LOGGER.warn("s3 range read from {} [{}, {}) failed", objectKey, startPosition, metadata.objectSize(), ex);
                    indexBlockCf.completeExceptionally(ex);
                    return null;
                });

    }

    public void readBlocks(List<StreamDataBlock> streamDataBlocks) {
        readBlocks(streamDataBlocks, -1);
    }

    public void readBlocks(List<StreamDataBlock> streamDataBlocks, long maxReadBatchSize) {
        if (streamDataBlocks.isEmpty()) {
            return;
        }
        int start = 0;
        int end = 0;
        long offset = -1;
        // split streamDataBlocks to blocks with continuous offset
        while (end < streamDataBlocks.size()) {
            if (offset != -1 && streamDataBlocks.get(end).getBlockStartPosition() != offset) {
                readContinuousBlocks(streamDataBlocks.subList(start, end), maxReadBatchSize);
                start = end;
            }
            offset = streamDataBlocks.get(end).getBlockEndPosition();
            end++;
        }
        if (end > start) {
            readContinuousBlocks(streamDataBlocks.subList(start, end), maxReadBatchSize);
        }
    }

    public void readContinuousBlocks(List<StreamDataBlock> streamDataBlocks, long maxReadBatchSize) {
        long objectId = metadata.objectId();
        if (maxReadBatchSize <= 0) {
            readContinuousBlocks0(streamDataBlocks);
            return;
        }

        long currentReadSize = 0;
        int start = 0;
        int end = 0;
        while (end < streamDataBlocks.size()) {
            currentReadSize += streamDataBlocks.get(end).getBlockSize();
            if (currentReadSize >= maxReadBatchSize) {
                final int finalStart = start;
                if (start == end) {
                    // split single data block to multiple read
                    long remainBytes = streamDataBlocks.get(end).getBlockSize();
                    long startPosition = streamDataBlocks.get(end).getBlockStartPosition();
                    long endPosition;
                    List<CompletableFuture<Void>> cfList = new ArrayList<>();
                    Map<Integer, ByteBuf> bufferMap = new ConcurrentHashMap<>();
                    int cnt = 0;
                    while (remainBytes > 0) {
                        long readSize = Math.min(remainBytes, maxReadBatchSize);
                        endPosition = startPosition + readSize;
                        final int finalCnt = cnt;
                        cfList.add(rangeRead(startPosition, endPosition).thenAccept(buf -> bufferMap.put(finalCnt, buf)));
                        remainBytes -= readSize;
                        startPosition += readSize;
                        cnt++;
                    }
                    final int iterations = cnt;
                    final int finalEnd = end + 1; // include current block
                    CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0]))
                            .thenAccept(v -> {
                                CompositeByteBuf compositeByteBuf = DirectByteBufAlloc.compositeByteBuffer();
                                for (int j = 0; j < iterations; j++) {
                                    compositeByteBuf.addComponent(true, bufferMap.get(j));
                                }
                                parseDataBlocks(compositeByteBuf, streamDataBlocks.subList(finalStart, finalEnd));
                            })
                            .exceptionally(ex -> {
                                LOGGER.error("read data from object {} failed", objectId, ex);
                                failDataBlocks(streamDataBlocks, ex);
                                return null;
                            });
                    end++;
                } else {
                    // read before current block
                    readContinuousBlocks0(streamDataBlocks.subList(start, end));
                }
                start = end;
                currentReadSize = 0;
            } else {
                end++;
            }
        }
        if (start < end) {
            readContinuousBlocks0(streamDataBlocks.subList(start, end));
        }
    }

    private void readContinuousBlocks0(List<StreamDataBlock> streamDataBlocks) {
        rangeRead(streamDataBlocks.get(0).getBlockStartPosition(),
                streamDataBlocks.get(streamDataBlocks.size() - 1).getBlockEndPosition())
                .thenAccept(buf -> parseDataBlocks(buf, streamDataBlocks))
                .exceptionally(ex -> {
                    LOGGER.error("read data from object {} failed", metadata.objectId(), ex);
                    failDataBlocks(streamDataBlocks, ex);
                    return null;
                });
    }

    private CompletableFuture<ByteBuf> rangeRead(long start, long end) {
        if (throttleBucket == null) {
            return s3Operator.rangeRead(objectKey, start, end, ThrottleStrategy.THROTTLE_2);
        } else {
            return throttleBucket.asScheduler().consume(end - start + 1, bucketCallbackExecutor)
                    .thenCompose(v -> s3Operator.rangeRead(objectKey, start, end, ThrottleStrategy.THROTTLE_2));
        }
    }

    private void parseDataBlocks(ByteBuf buf, List<StreamDataBlock> streamDataBlocks) {
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            int blockSize = streamDataBlock.getBlockSize();
            ByteBuf blockBuf = buf.retainedSlice(buf.readerIndex(), blockSize);
            buf.skipBytes(blockSize);
            streamDataBlock.getDataCf().complete(blockBuf);
        }
        buf.release();
    }

    private void failDataBlocks(List<StreamDataBlock> streamDataBlocks, Throwable ex) {
        for (StreamDataBlock streamDataBlock : streamDataBlocks) {
            streamDataBlock.getDataCf().completeExceptionally(ex);
        }
    }

    static class IndexBlock {
        static List<StreamDataBlock> parse(ByteBuf objectTailBuf, long objectSize, long objectId) throws IndexBlockParseException {
            try {
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
                        dataBlockIndices.add(new DataBlockIndex(blockPosition, blockSize, recordCount));
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
            } finally {
                objectTailBuf.release();
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
        private final long startPosition;
        private final int size;
        private final int recordCount;

        public DataBlockIndex(long startPosition, int size, int recordCount) {
            this.startPosition = startPosition;
            this.size = size;
            this.recordCount = recordCount;
        }
    }
}
