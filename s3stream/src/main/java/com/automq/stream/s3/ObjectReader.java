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

import com.automq.stream.s3.cache.AsyncMeasurable;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.utils.CloseableIterator;
import com.automq.stream.utils.biniarysearch.IndexBlockOrderedBytes;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.ByteBufAlloc.BLOCK_CACHE;
import static com.automq.stream.s3.ByteBufAlloc.READ_INDEX_BLOCK;
import static com.automq.stream.s3.ObjectWriter.Footer.FOOTER_SIZE;
import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OFFSET;

public interface ObjectReader extends AsyncMeasurable {

    static ObjectReader reader(S3ObjectMetadata metadata, ObjectStorage objectStorage) {
        switch (ObjectAttributes.from(metadata.attributes()).type()) {
            case Normal:
                return new DefaultObjectReader(metadata, objectStorage);
            case Composite:
                return CompositeObject.reader(metadata, objectStorage);
            default:
                throw new UnsupportedOperationException();
        }
    }

    S3ObjectMetadata metadata();

    String objectKey();

    CompletableFuture<BasicObjectInfo> basicObjectInfo();

    default CompletableFuture<FindIndexResult> find(long streamId, long startOffset, long endOffset) {
        return find(streamId, startOffset, endOffset, Integer.MAX_VALUE);
    }

    default CompletableFuture<FindIndexResult> find(long streamId, long startOffset, long endOffset, int maxBytes) {
        return basicObjectInfo().thenApply(basicObjectInfo -> basicObjectInfo.indexBlock().find(streamId, startOffset, endOffset, maxBytes));
    }

    default CompletableFuture<DataBlockGroup> read(DataBlockIndex block) {
        return read(ReadOptions.DEFAULT, block);
    }

    CompletableFuture<DataBlockGroup> read(ReadOptions readOptions, DataBlockIndex block);

    ObjectReader retain();

    ObjectReader release();

    void close();

    CompletableFuture<Integer> size();

    interface RangeReader {
        default CompletableFuture<ByteBuf> rangeRead(S3ObjectMetadata metadata, long start, long end) {
            return rangeRead(ReadOptions.DEFAULT, metadata, start, end);
        }

        CompletableFuture<ByteBuf> rangeRead(ReadOptions readOptions, S3ObjectMetadata metadata, long start, long end);
    }

    class DefaultObjectReader implements ObjectReader {

        private static final Logger LOGGER = LoggerFactory.getLogger(ObjectReader.class);
        private final S3ObjectMetadata metadata;
        private final String objectKey;
        private final ObjectStorage objectStorage;
        private CompletableFuture<BasicObjectInfo> basicObjectInfoCf;
        private CompletableFuture<Integer> sizeCf;
        private final AtomicInteger refCount = new AtomicInteger(1);
        private final AtomicBoolean isShutdown = new AtomicBoolean(false);

        public DefaultObjectReader(S3ObjectMetadata metadata, ObjectStorage objectStorage) {
            this.metadata = metadata;
            this.objectKey = metadata.key();
            this.objectStorage = objectStorage;
        }

        public S3ObjectMetadata metadata() {
            return metadata;
        }

        public String objectKey() {
            return objectKey;
        }

        public synchronized CompletableFuture<BasicObjectInfo> basicObjectInfo() {
            if (isShutdown.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("ObjectReader is already shutdown"));
            }
            if (basicObjectInfoCf == null) {
                this.basicObjectInfoCf = new CompletableFuture<>();
                asyncGetBasicObjectInfo();
            }
            return basicObjectInfoCf;
        }

        public CompletableFuture<FindIndexResult> find(long streamId, long startOffset, long endOffset) {
            return find(streamId, startOffset, endOffset, Integer.MAX_VALUE);
        }

        public CompletableFuture<FindIndexResult> find(long streamId, long startOffset, long endOffset, int maxBytes) {
            return basicObjectInfo().thenApply(basicObjectInfo -> basicObjectInfo.indexBlock().find(streamId, startOffset, endOffset, maxBytes));
        }

        public CompletableFuture<DataBlockGroup> read(ReadOptions readOptions, DataBlockIndex block) {
            CompletableFuture<ByteBuf> rangeReadCf = objectStorage.rangeRead(
                new ObjectStorage.ReadOptions().throttleStrategy(readOptions.throttleStrategy).bucket(metadata.bucket()),
                metadata.key(),
                block.startPosition(),
                block.endPosition()
            );
            return rangeReadCf.thenApply(buf -> {
                ByteBuf pooled = ByteBufAlloc.byteBuffer(buf.readableBytes(), BLOCK_CACHE);
                pooled.writeBytes(buf);
                buf.release();
                return new DataBlockGroup(pooled);
            });
        }

        void asyncGetBasicObjectInfo() {
            int guessIndexBlockSize = 1024 + (int) (metadata.objectSize() / (1024 * 1024 /* 1MB */) * 36 /* index unit size*/);
            asyncGetBasicObjectInfo0(Math.max(0, metadata.objectSize() - guessIndexBlockSize), true);
        }

        private void asyncGetBasicObjectInfo0(long startPosition, boolean firstAttempt) {
            CompletableFuture<ByteBuf> cf = objectStorage.rangeRead(new ObjectStorage.ReadOptions().bucket(metadata.bucket()),
                metadata.key(), startPosition, metadata.objectSize());
            cf.thenAccept(buf -> {
                try {
                    BasicObjectInfo basicObjectInfo = BasicObjectInfo.parse(buf, metadata);
                    basicObjectInfo().complete(basicObjectInfo);
                } catch (ObjectParseException ex) {
                    if (!firstAttempt) {
                        basicObjectInfo().completeExceptionally(ex);
                        return;
                    }
                    // retry with corrected position.
                    asyncGetBasicObjectInfo0(ex.indexBlockPosition, false);
                }
            }).exceptionally(ex -> {
                LOGGER.error("get {} object info failed", metadata, ex);
                basicObjectInfo().completeExceptionally(ex);
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

        @Override
        public synchronized CompletableFuture<Integer> size() {
            if (sizeCf == null) {
                sizeCf = basicObjectInfo().thenApply(BasicObjectInfo::size);
            }
            return sizeCf;
        }

        public synchronized void close0() {
            if (!isShutdown.compareAndSet(false, true)) {
                return;
            }
            if (basicObjectInfoCf != null) {
                basicObjectInfoCf.thenAccept(BasicObjectInfo::close);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            DefaultObjectReader reader = (DefaultObjectReader) o;
            return Objects.equals(metadata.objectId(), reader.metadata.objectId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(metadata.objectId());
        }

    }

    /**
     *
     */
    class BasicObjectInfo {
        private final long dataBlockSize;
        private final IndexBlock indexBlock;

        /**
         * @param dataBlockSize The total size of the data blocks, which equals to index start position.
         * @param indexBlock    raw index data.
         */
        public BasicObjectInfo(long dataBlockSize, IndexBlock indexBlock) {
            this.dataBlockSize = dataBlockSize;
            this.indexBlock = indexBlock;
        }

        public static BasicObjectInfo parse(ByteBuf objectTailBuf,
            S3ObjectMetadata s3ObjectMetadata) throws ObjectParseException {
            objectTailBuf = objectTailBuf.slice();
            long footerMagic = objectTailBuf.getLong(objectTailBuf.readableBytes() - 8);
            if (footerMagic != ObjectWriter.Footer.MAGIC) {
                throw new ObjectParseException("Invalid footer magic: " + footerMagic);
            }

            long indexBlockPosition = objectTailBuf.getLong(objectTailBuf.readableBytes() - FOOTER_SIZE);
            int indexBlockSize = objectTailBuf.getInt(objectTailBuf.readableBytes() - 40);
            if (indexBlockPosition + objectTailBuf.readableBytes() < s3ObjectMetadata.objectSize()) {
                objectTailBuf.release();
                throw new ObjectParseException(indexBlockPosition);
            } else {
                int indexRelativePosition = objectTailBuf.readableBytes() - (int) (s3ObjectMetadata.objectSize() - indexBlockPosition);

                // trim the ByteBuf to avoid extra memory occupy.
                ByteBuf indexBlockBuf = objectTailBuf.slice(objectTailBuf.readerIndex() + indexRelativePosition, indexBlockSize);
                ByteBuf copy = ByteBufAlloc.byteBuffer(indexBlockBuf.readableBytes(), READ_INDEX_BLOCK);
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

        public long dataBlockSize() {
            return dataBlockSize;
        }

        public IndexBlock indexBlock() {
            return indexBlock;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (BasicObjectInfo) obj;
            return this.dataBlockSize == that.dataBlockSize &&
                Objects.equals(this.indexBlock, that.indexBlock);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataBlockSize, indexBlock);
        }

        @Override
        public String toString() {
            return "BasicObjectInfo[" +
                "dataBlockSize=" + dataBlockSize + ", " +
                "indexBlock=" + indexBlock + ']';
        }

    }

    class IndexBlock {
        public static final int INDEX_BLOCK_UNIT_SIZE = 8/* streamId */ + 8 /* startOffset */ + 4 /* endOffset delta */
            + 4 /* record count */ + 8 /* block position */ + 4 /* block size */;
        private final S3ObjectMetadata s3ObjectMetadata;
        private final ByteBuf buf;
        private final int size;
        private final int count;

        public IndexBlock(ByteBuf buf) {
            this(null, buf);
        }

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

        public List<DataBlockIndex> indexes() {
            List<DataBlockIndex> indexes = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                indexes.add(get(i));
            }
            return indexes;
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
            return new DataBlockIndex(index, streamId, startOffset, endOffsetDelta, recordCount, blockPosition, blockSize);
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
            if (startIndex < 0) {
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
                    rst.add(new StreamDataBlock(s3ObjectMetadata == null ? Constants.NOOP_EPOCH : s3ObjectMetadata.objectId(), index));

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

        public Optional<StreamOffsetRange> findStreamOffsetRange(long streamId) {
            IndexBlockOrderedBytes indexBlockOrderedBytes = new IndexBlockOrderedBytes(this);
            int searchRst = indexBlockOrderedBytes.search(new IndexBlockOrderedBytes.TargetStreamOffset(streamId, Long.MIN_VALUE));
            int insertPoint = -searchRst - 1;
            long startOffset = Constants.NOOP_OFFSET;
            long endOffset = Constants.NOOP_OFFSET;
            for (int i = insertPoint; i < count; i++) {
                DataBlockIndex dataBlockIndex = get(i);
                if (dataBlockIndex.streamId() != streamId) {
                    break;
                }
                if (startOffset == Constants.NOOP_OFFSET) {
                    startOffset = dataBlockIndex.startOffset();
                }
                endOffset = dataBlockIndex.endOffset();
            }
            if (startOffset == Constants.NOOP_OFFSET) {
                return Optional.empty();
            } else {
                return Optional.of(new StreamOffsetRange(streamId, startOffset, endOffset));
            }
        }

        public List<StreamOffsetRange> streamOffsetRanges() {
            List<StreamOffsetRange> ranges = new ArrayList<>(count);
            Iterator<DataBlockIndex> it = iterator();
            long streamId = Constants.NOOP_STREAM_ID;
            long startOffset = Constants.NOOP_OFFSET;
            long endOffset = Constants.NOOP_OFFSET;
            while (it.hasNext()) {
                DataBlockIndex index = it.next();
                if (index.streamId() != streamId && streamId != Constants.NOOP_STREAM_ID) {
                    ranges.add(new StreamOffsetRange(streamId, startOffset, endOffset));
                    startOffset = Constants.NOOP_OFFSET;
                }
                streamId = index.streamId();
                if (startOffset == Constants.NOOP_OFFSET) {
                    startOffset = index.startOffset();
                }
                endOffset = index.endOffset();
            }
            if (streamId != Constants.NOOP_STREAM_ID) {
                ranges.add(new StreamOffsetRange(streamId, startOffset, endOffset));
            }
            return ranges;
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

    final class FindIndexResult {
        private final boolean isFulfilled;
        private final long nextStartOffset;
        private final int nextMaxBytes;
        private final List<StreamDataBlock> streamDataBlocks;

        public FindIndexResult(boolean isFulfilled, long nextStartOffset, int nextMaxBytes,
            List<StreamDataBlock> streamDataBlocks) {
            this.isFulfilled = isFulfilled;
            this.nextStartOffset = nextStartOffset;
            this.nextMaxBytes = nextMaxBytes;
            this.streamDataBlocks = streamDataBlocks;
        }

        public boolean isFulfilled() {
            return isFulfilled;
        }

        public long nextStartOffset() {
            return nextStartOffset;
        }

        public int nextMaxBytes() {
            return nextMaxBytes;
        }

        public List<StreamDataBlock> streamDataBlocks() {
            return streamDataBlocks;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (FindIndexResult) obj;
            return this.isFulfilled == that.isFulfilled &&
                this.nextStartOffset == that.nextStartOffset &&
                this.nextMaxBytes == that.nextMaxBytes &&
                Objects.equals(this.streamDataBlocks, that.streamDataBlocks);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isFulfilled, nextStartOffset, nextMaxBytes, streamDataBlocks);
        }

        @Override
        public String toString() {
            return "FindIndexResult[" +
                "isFulfilled=" + isFulfilled + ", " +
                "nextStartOffset=" + nextStartOffset + ", " +
                "nextMaxBytes=" + nextMaxBytes + ", " +
                "streamDataBlocks=" + streamDataBlocks + ']';
        }

    }

    class ObjectParseException extends Exception {
        long indexBlockPosition;

        public ObjectParseException(long indexBlockPosition) {
            this.indexBlockPosition = indexBlockPosition;
        }

        public ObjectParseException(String message) {
            super(message);
        }

    }

    class DataBlockGroup implements AutoCloseable {
        private final ByteBuf buf;
        private final int recordCount;

        public DataBlockGroup(ByteBuf buf) {
            this.buf = buf;
            this.recordCount = check(buf);
        }

        private static int check(ByteBuf buf) {
            buf = buf.duplicate();
            int recordCount = 0;
            while (buf.readableBytes() > 0) {
                byte magicCode = buf.readByte();
                if (magicCode != ObjectWriter.DATA_BLOCK_MAGIC) {
                    throw new RuntimeException("[FATAL] magic code mismatch, data is corrupted, expected " + ObjectWriter.DATA_BLOCK_MAGIC + ", actual " + magicCode);
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

        public List<StreamRecordBatch> records() {
            List<StreamRecordBatch> records = new ArrayList<>(recordCount);
            try (CloseableIterator<StreamRecordBatch> it = iterator()) {
                it.forEachRemaining(records::add);
            }
            return records;
        }

        public int recordCount() {
            return recordCount;
        }

        public ByteBuf buf() {
            return buf;
        }

        @Override
        public void close() {
            buf.release();
        }

        public void retain() {
            buf.retain();
        }

        public void release() {
            buf.release();
        }
    }

    class ReadOptions {
        public static final ReadOptions DEFAULT = new ReadOptions();
        private ThrottleStrategy throttleStrategy = ThrottleStrategy.BYPASS;

        public ReadOptions throttleStrategy(ThrottleStrategy throttleStrategy) {
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public ThrottleStrategy throttleStrategy() {
            return throttleStrategy;
        }
    }

}
