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

package com.automq.stream.s3.compact.operator;

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.stats.CompactionStats;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import io.github.bucket4j.Bucket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.STREAM_SET_OBJECT_COMPACTION_READ;

//TODO: refactor to reduce duplicate code with ObjectWriter
public class DataBlockReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockReader.class);
    private static final ByteBufSeqAlloc DIRECT_ALLOC = new ByteBufSeqAlloc(STREAM_SET_OBJECT_COMPACTION_READ, 1);
    private final S3ObjectMetadata metadata;
    private final String objectKey;
    private final ObjectStorage objectStorage;
    private final CompletableFuture<List<StreamDataBlock>> indexBlockCf = new CompletableFuture<>();
    private final Bucket throttleBucket;
    private final ScheduledExecutorService bucketCallbackExecutor;

    public DataBlockReader(S3ObjectMetadata metadata, ObjectStorage objectStorage) {
        this(metadata, objectStorage, null, null);
    }

    public DataBlockReader(S3ObjectMetadata metadata, ObjectStorage objectStorage, Bucket throttleBucket,
        ScheduledExecutorService bucketCallbackExecutor) {
        this.metadata = metadata;
        this.objectKey = metadata.key();
        this.objectStorage = objectStorage;
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
        // TODO: throttle level
        @SuppressWarnings("resource") ObjectReader objectReader = ObjectReader.reader(metadata, objectStorage);
        objectReader.basicObjectInfo().thenAccept(info -> {
            List<StreamDataBlock> blocks = new ArrayList<>(info.indexBlock().count());
            Iterator<DataBlockIndex> it = info.indexBlock().iterator();
            while (it.hasNext()) {
                blocks.add(new StreamDataBlock(metadata.objectId(), it.next()));
            }
            indexBlockCf.complete(blocks);
        }).exceptionally(ex -> {
            // unrecoverable error, possibly read on a deleted object
            LOGGER.warn("object {} index parse fail", objectKey, ex);
            indexBlockCf.completeExceptionally(ex);
            return null;
        }).whenComplete((nil, ex) -> objectReader.release());
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
                            CompositeByteBuf compositeByteBuf = ByteBufAlloc.compositeByteBuffer();
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
        return rangeRead0(start, end).whenComplete((ret, ex) -> {
            if (ex == null) {
                CompactionStats.getInstance().compactionReadSizeStats.add(MetricsLevel.INFO, ret.readableBytes());
            }
        });
    }

    private CompletableFuture<ByteBuf> rangeRead0(long start, long end) {
        if (throttleBucket == null) {
            return objectStorage.rangeRead(new ReadOptions().throttleStrategy(ThrottleStrategy.COMPACTION).bucket(metadata.bucket()), objectKey, start, end).thenApply(buf -> {
                // convert heap buffer to direct buffer
                ByteBuf directBuf = DIRECT_ALLOC.alloc(buf.readableBytes());
                directBuf.writeBytes(buf);
                buf.release();
                return directBuf;
            });
        } else {
            return throttleBucket.asScheduler().consume(end - start + 1, bucketCallbackExecutor)
                .thenCompose(v ->
                    objectStorage.rangeRead(new ReadOptions().throttleStrategy(ThrottleStrategy.COMPACTION).bucket(metadata.bucket()), objectKey, start, end).thenApply(buf -> {
                        // convert heap buffer to direct buffer
                        ByteBuf directBuf = DIRECT_ALLOC.alloc(buf.readableBytes());
                        directBuf.writeBytes(buf);
                        buf.release();
                        return directBuf;
                    }));
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
}
