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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.CloseableIterator;
import com.automq.stream.utils.biniarysearch.IndexBlockOrderedBytes;
import io.netty.buffer.ByteBuf;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return rangeReadCf.thenApply(DataBlock::new);
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
     * @param dataBlockSize The total size of the data blocks, which equals to index start position.
     * @param indexBlock    raw index data.
     */
    public record BasicObjectInfo(long dataBlockSize, IndexBlock indexBlock) {

        public static BasicObjectInfo parse(ByteBuf objectTailBuf,
            S3ObjectMetadata s3ObjectMetadata) throws IndexBlockParseException {
            objectTailBuf = objectTailBuf.slice();
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
                return new BasicObjectInfo(indexBlockPosition, new IndexBlock(s3ObjectMetadata, indexBlockBuf));
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
        public static final int INDEX_BLOCK_UNIT_SIZE = 8/* streamId */ + 8 /* startOffset */ + 4 /* endOffset delta */
            + 4 /* record count */ + 8 /* block position */ + 4 /* block size */;
        private final S3ObjectMetadata s3ObjectMetadata;
        private final ByteBuf buf;
        private final int size;
        private final int count;

        public IndexBlock(S3ObjectMetadata s3ObjectMetadata, ByteBuf buf) {
            this.s3ObjectMetadata = s3ObjectMetadata;
            this.buf = buf;
            this.size = buf.readableBytes();
            this.count = buf.readableBytes() / INDEX_BLOCK_UNIT_SIZE;
        }

        public Iterator<DataBlockIndex> iterator() {
            AtomicInteger getIndex = new AtomicInteger(0);
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return getIndex.get() < count;
                }

                @Override
                public DataBlockIndex next() {
                    return get(getIndex.getAndIncrement());
                }
            };
        }

        public DataBlockIndex get(int index) {
            if (index < 0 || index >= count) {
                throw new IllegalArgumentException("index" + index + " is out of range [0, " + count + ")");
            }
            int base = index * INDEX_BLOCK_UNIT_SIZE;
            long streamId = buf.getLong(base);
            long startOffset = buf.getLong(base + 8);
            int endOffsetDelta = buf.getInt(base + 16);
            int recordCount = buf.getInt(base + 20);
            long blockPosition = buf.getLong(base + 24);
            int blockSize = buf.getInt(base + 32);
            return new DataBlockIndex(streamId, startOffset, endOffsetDelta, recordCount, blockPosition, blockSize);
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
            IndexBlockOrderedBytes indexBlockOrderedBytes = new IndexBlockOrderedBytes(this);
            int startIndex = indexBlockOrderedBytes.search(new IndexBlockOrderedBytes.TargetStreamOffset(streamId, startOffset));
            if (startIndex == -1) {
                // mismatched
                return new FindIndexResult(false, nextStartOffset, nextMaxBytes, rst);
            }
            for (int i = startIndex; i < count(); i++) {
                DataBlockIndex index = get(i);
                if (index.streamId() == streamId) {
                    if (nextStartOffset < index.startOffset()) {
                        break;
                    }
                    if (index.endOffset() <= nextStartOffset) {
                        continue;
                    }
                    matched = nextStartOffset == index.startOffset();
                    nextStartOffset = index.endOffset();
                    rst.add(new StreamDataBlock(s3ObjectMetadata.objectId(), index));

                    // we consider first block as not matched because we do not know exactly how many bytes are within
                    // the range in first block, as a result we may read one more block than expected.
                    if (matched) {
                        int recordPayloadSize = index.size()
                            - index.recordCount() * StreamRecordBatchCodec.HEADER_SIZE // sum of encoded record header size
                            - ObjectWriter.DataBlock.BLOCK_HEADER_SIZE; // block header size
                        nextMaxBytes -= Math.min(nextMaxBytes, recordPayloadSize);
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

        public int size() {
            return size;
        }

        public int count() {
            return count;
        }

        void close() {
            buf.release();
        }
    }

    public record FindIndexResult(boolean isFulfilled, long nextStartOffset, int nextMaxBytes,
                                  List<StreamDataBlock> streamDataBlocks) {

    }

    public static class IndexBlockParseException extends Exception {
        long indexBlockPosition;

        public IndexBlockParseException(long indexBlockPosition) {
            this.indexBlockPosition = indexBlockPosition;
        }

    }

    public static class DataBlock implements AutoCloseable {
        private final ByteBuf buf;
        private final int recordCount;

        public DataBlock(ByteBuf buf) {
            this.buf = buf.duplicate();
            this.recordCount = check(buf);
        }

        private static int check(ByteBuf buf) {
            buf = buf.duplicate();
            int recordCount = 0;
            while (buf.readableBytes() > 0) {
                byte magicCode = buf.readByte();
                if (magicCode != ObjectWriter.DATA_BLOCK_MAGIC) {
                    LOGGER.error("magic code mismatch, expected {}, actual {}", ObjectWriter.DATA_BLOCK_MAGIC, magicCode);
                    throw new RuntimeException("[FATAL] magic code mismatch, data is corrupted");
                }
                buf.readByte(); // flag
                recordCount += buf.readInt();
                int dataLength = buf.readInt();
                buf.skipBytes(dataLength);
            }
            return recordCount;
        }

        public CloseableIterator<StreamRecordBatch> iterator() {
            ByteBuf buf = this.buf.duplicate();
            AtomicInteger currentBlockRecordCount = new AtomicInteger(0);
            AtomicInteger remainingRecordCount = new AtomicInteger(recordCount);
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
                    if (currentBlockRecordCount.get() == 0) {
                        buf.skipBytes(1 /* magic */ + 1 /* flag */);
                        currentBlockRecordCount.set(buf.readInt());
                        buf.skipBytes(4);
                    }
                    currentBlockRecordCount.decrementAndGet();
                    return StreamRecordBatchCodec.duplicateDecode(buf);
                }

                @Override
                public void close() {
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
