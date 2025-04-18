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

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.utils.biniarysearch.AbstractOrderedCollection;
import com.automq.stream.utils.biniarysearch.ComparableItem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.BLOCK_CACHE;
import static com.automq.stream.s3.CompositeObject.FOOTER_MAGIC;
import static com.automq.stream.s3.CompositeObject.OBJECTS_BLOCK_MAGIC;
import static com.automq.stream.s3.CompositeObject.OBJECT_BLOCK_HEADER_SIZE;
import static com.automq.stream.s3.CompositeObject.OBJECT_UNIT_SIZE;
import static com.automq.stream.s3.ObjectWriter.Footer.FOOTER_SIZE;
import static com.automq.stream.s3.operator.ObjectStorage.RANGE_READ_TO_END;

public class CompositeObjectReader implements ObjectReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeObjectReader.class);
    private final S3ObjectMetadata objectMetadata;
    private final RangeReader rangeReader;
    private CompletableFuture<BasicObjectInfo> basicObjectInfoCf;
    private CompletableFuture<Integer> sizeCf;
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public CompositeObjectReader(S3ObjectMetadata objectMetadata, RangeReader rangeReader) {
        this.objectMetadata = objectMetadata;
        this.rangeReader = rangeReader;
    }

    @Override
    public S3ObjectMetadata metadata() {
        return objectMetadata;
    }

    @Override
    public String objectKey() {
        return ObjectUtils.genKey(0, objectMetadata.objectId());
    }

    @Override
    public synchronized CompletableFuture<BasicObjectInfo> basicObjectInfo() {
        if (isShutdown.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("ObjectReader is already shutdown"));
        }
        if (basicObjectInfoCf == null) {
            this.basicObjectInfoCf = new CompletableFuture<>();
            this.basicObjectInfoCf.exceptionally(ex -> {
                LOGGER.error("get {} composite object info failed", objectMetadata, ex);
                return null;
            });
            asyncGetBasicObjectInfo(this.basicObjectInfoCf);
        }
        return basicObjectInfoCf;
    }

    @Override
    public CompletableFuture<DataBlockGroup> read(ReadOptions readOptions, DataBlockIndex block) {
        return basicObjectInfo().thenCompose(info -> read0(readOptions, info, block));
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
        // NOTE: DO NOT OVERRIDE THIS
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        // NOTE: DO NOT OVERRIDE THIS
        return super.hashCode();
    }

    private void asyncGetBasicObjectInfo(CompletableFuture<BasicObjectInfo> basicObjectInfoCf) {
        CompletableFuture<ByteBuf> cf = rangeReader.rangeRead(objectMetadata, 0, RANGE_READ_TO_END);
        cf.thenAccept(buf -> {
            try {
                buf = buf.slice();
                int readableBytes = buf.readableBytes();
                long footerMagic = buf.getLong(readableBytes - 8);
                if (footerMagic != FOOTER_MAGIC) {
                    throw new ObjectParseException("Invalid footer magic: " + footerMagic);
                }
                long indexBlockPosition = buf.getLong(readableBytes - FOOTER_SIZE);
                int indexBlockSize = buf.getInt(readableBytes - 40);
                ByteBuf objectsBlockBuf = buf.retainedSlice(0, (int) indexBlockPosition);
                ByteBuf indexesBlockBuf = buf.retainedSlice((int) indexBlockPosition, indexBlockSize);
                buf.release();
                IndexBlock indexBlock = new IndexBlock(indexesBlockBuf);
                ObjectsBlock objectsBlock = new ObjectsBlock(objectsBlockBuf, indexBlock.count());
                basicObjectInfoCf.complete(new BasicObjectInfoExt(objectsBlock, new IndexBlock(indexesBlockBuf)));
            } catch (Throwable e) {
                buf.release();
                basicObjectInfoCf.completeExceptionally(e);
            }
        }).exceptionally(ex -> {
            basicObjectInfoCf.completeExceptionally(ex);
            return null;
        });
    }

    private CompletableFuture<DataBlockGroup> read0(ReadOptions readOptions, BasicObjectInfo info, DataBlockIndex block) {
        S3ObjectMetadata linkObjectMetadata = ((BasicObjectInfoExt) info).objectsBlock().getLinkObjectMetadata(block.id());
        return rangeReader.rangeRead(readOptions, linkObjectMetadata, block.startPosition(), block.endPosition()).thenApply(buf -> {
            ByteBuf pooled = ByteBufAlloc.byteBuffer(buf.readableBytes(), BLOCK_CACHE);
            pooled.writeBytes(buf);
            buf.release();
            return new DataBlockGroup(pooled);
        });
    }

    public class BasicObjectInfoExt extends BasicObjectInfo {
        private final ObjectsBlock objectsBlock;

        public BasicObjectInfoExt(ObjectsBlock objectsBlock, IndexBlock indexBlock) throws ObjectParseException {
            super(-1L, indexBlock);
            this.objectsBlock = objectsBlock;
        }

        @Override
        void close() {
            super.close();
            objectsBlock.close();
        }

        public ObjectsBlock objectsBlock() {
            return objectsBlock;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            if (!super.equals(o))
                return false;
            BasicObjectInfoExt ext = (BasicObjectInfoExt) o;
            return objectId() == ext.objectId();
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectId());
        }

        long objectId() {
            return objectMetadata.objectId();
        }
    }

    static class ObjectsSearcher extends AbstractOrderedCollection<SearchTarget> {
        private final ByteBuf objectsBlockBuf;
        private final int size;

        public ObjectsSearcher(ByteBuf objectsBlockBuf) {
            this.objectsBlockBuf = objectsBlockBuf;
            this.size = objectsBlockBuf.getInt(1);
        }

        @Override
        protected int size() {
            return size;
        }

        @Override
        protected ComparableItem<SearchTarget> get(int index) {
            int base = OBJECT_BLOCK_HEADER_SIZE + index * OBJECT_UNIT_SIZE;
            int blockStartIndex = objectsBlockBuf.getInt(base + 8);
            int endOffset = Integer.MAX_VALUE;
            if (index < size - 1) {
                endOffset = objectsBlockBuf.getInt(base + OBJECT_UNIT_SIZE + 8);
            }

            return new ObjectCompareItem(blockStartIndex, endOffset);
        }
    }

    static class SearchTarget {
        final int blockStartIndex;

        SearchTarget(int blockIndex) {
            this.blockStartIndex = blockIndex;
        }
    }

    static class ObjectCompareItem extends SearchTarget implements ComparableItem<SearchTarget> {
        final long blockEndIndex;

        ObjectCompareItem(int blockStartIndex, int blockEndIndex) {
            super(blockStartIndex);
            this.blockEndIndex = blockEndIndex;
        }

        @Override
        public boolean isLessThan(SearchTarget value) {
            return blockEndIndex <= value.blockStartIndex;
        }

        @Override
        public boolean isGreaterThan(SearchTarget value) {
            return blockStartIndex > value.blockStartIndex;
        }
    }

    public static class ObjectsBlock {
        private final ByteBuf buf;
        private final int count;
        private final int dataBlockIndexCount;
        private final ObjectsSearcher objectsSearcher;

        public ObjectsBlock(ByteBuf buf, int dataBlockIndexCount) throws ObjectParseException {
            if (buf.getByte(0) != OBJECTS_BLOCK_MAGIC) {
                throw new ObjectParseException("Invalid objects block magic: " + buf.getByte(0));
            }
            this.buf = buf.slice();
            this.count = this.buf.getInt(1);
            this.dataBlockIndexCount = dataBlockIndexCount;
            this.objectsSearcher = new ObjectsSearcher(buf);
        }

        public Iterator<ObjectIndex> iterator() {
            AtomicInteger getIndex = new AtomicInteger(0);
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return getIndex.get() < count;
                }

                @Override
                public ObjectIndex next() {
                    return get(getIndex.getAndIncrement());
                }
            };
        }

        public List<ObjectIndex> indexes() {
            List<ObjectIndex> indexes = new LinkedList<>();
            for (int i = 0; i < count; i++) {
                indexes.add(get(i));
            }
            return indexes;
        }

        public ObjectIndex get(int index) {
            if (index < 0 || index >= count) {
                throw new IllegalArgumentException("index" + index + " is out of range [0, " + count + ")");
            }
            int base = index * OBJECT_UNIT_SIZE + OBJECT_BLOCK_HEADER_SIZE;
            long objectId = buf.getLong(base);
            int blockStartIndex = buf.getInt(base + 8);
            short bucket = buf.getShort(base + 12);
            int blockEndIndex = dataBlockIndexCount;
            if (index < count - 1) {
                blockEndIndex = buf.getInt(base + OBJECT_UNIT_SIZE + 8);
            }
            return new ObjectIndex(objectId, blockStartIndex, blockEndIndex, bucket);
        }

        public S3ObjectMetadata getLinkObjectMetadata(int blockIndex) {
            // TODO: optimize for next continuous search
            int index = objectsSearcher.search(new SearchTarget(blockIndex));
            int base = OBJECT_BLOCK_HEADER_SIZE + index * OBJECT_UNIT_SIZE;
            long objectId = buf.getLong(base);
            short bucketId = buf.getShort(base + 12);
            return new S3ObjectMetadata(objectId, ObjectAttributes.builder().bucket(bucketId).build().attributes());
        }

        public void close() {
            buf.release();
        }
    }

    public static class ObjectIndex {
        private final long objectId;
        private final int blockStartIndex;
        private final int blockEndIndex;
        private final short bucketId;

        public ObjectIndex(long objectId, int blockStartIndex, int blockEndIndex, short bucketId) {
            this.objectId = objectId;
            this.blockStartIndex = blockStartIndex;
            this.blockEndIndex = blockEndIndex;
            this.bucketId = bucketId;
        }

        public long objectId() {
            return objectId;
        }

        public int blockStartIndex() {
            return blockStartIndex;
        }

        public int blockEndIndex() {
            return blockEndIndex;
        }

        public short bucketId() {
            return bucketId;
        }

        @Override
        public String toString() {
            return "ObjectIndex{" +
                "objectId=" + objectId +
                ", blockStartIndex=" + blockStartIndex +
                ", blockEndIndex=" + blockEndIndex +
                ", bucketId=" + bucketId +
                '}';
        }
    }

}
