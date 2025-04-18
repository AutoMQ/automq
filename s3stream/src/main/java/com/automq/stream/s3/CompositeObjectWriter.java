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

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.Writer;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import static com.automq.stream.s3.ByteBufAlloc.WRITE_FOOTER;
import static com.automq.stream.s3.ByteBufAlloc.WRITE_INDEX_BLOCK;
import static com.automq.stream.s3.CompositeObject.FOOTER_MAGIC;
import static com.automq.stream.s3.CompositeObject.FOOTER_SIZE;
import static com.automq.stream.s3.CompositeObject.OBJECTS_BLOCK_MAGIC;
import static com.automq.stream.s3.CompositeObject.OBJECT_BLOCK_HEADER_SIZE;
import static com.automq.stream.s3.CompositeObject.OBJECT_UNIT_SIZE;

public class CompositeObjectWriter implements ObjectWriter {
    private final NavigableMap<Integer, LinkedObject> components = new TreeMap<>();
    private long startOffset = Constants.NOOP_OFFSET;
    private long expectStreamId = Constants.NOOP_STREAM_ID;
    private long expectOffset = Constants.NOOP_OFFSET;
    private long size = 0;
    private int nextBlockStartIndex = 0;

    private final Writer writer;

    public CompositeObjectWriter(Writer writer) {
        this.writer = writer;
    }

    public void addComponent(S3ObjectMetadata partObjectMetadata, List<DataBlockIndex> partObjectIndexes) {
        continuousCheck(partObjectIndexes);
        components.put(nextBlockStartIndex, new LinkedObject(partObjectMetadata, partObjectIndexes));
        nextBlockStartIndex += partObjectIndexes.size();
        size += partObjectIndexes.stream().mapToLong(DataBlockIndex::size).sum();
    }

    @Override
    public CompletableFuture<Void> close() {
        ObjectsBlock objectsBlock = new ObjectsBlock(components);
        long indexesBlockStartPosition = objectsBlock.size();
        IndexesBlock indexesBlock = new IndexesBlock(components);
        Footer footer = new Footer(indexesBlockStartPosition, indexesBlock.size());
        CompositeByteBuf objBuf = ByteBufAlloc.compositeByteBuffer();
        objBuf.addComponent(true, objectsBlock.buffer());
        objBuf.addComponent(true, indexesBlock.buffer());
        objBuf.addComponent(true, footer.buffer());
        writer.write(objBuf);
        return writer.close();
    }

    @Override
    public List<ObjectStreamRange> getStreamRanges() {
        if (startOffset == Constants.NOOP_OFFSET) {
            return Collections.emptyList();
        }
        return List.of(new ObjectStreamRange(expectStreamId, Constants.NOOP_EPOCH, startOffset, expectOffset, -1));
    }

    /**
     * Return the retained size of the object.
     * For example, the composite object contains obj1 with size=10MiB and obj2 with size=20MiB, then the retained size is 30MiB
     */
    @Override
    public long size() {
        return size;
    }

    @Override
    public short bucketId() {
        return writer.bucketId();
    }

    void continuousCheck(List<DataBlockIndex> newIndexes) {
        for (DataBlockIndex index : newIndexes) {
            if (expectStreamId == Constants.NOOP_STREAM_ID) {
                expectStreamId = index.streamId();
                startOffset = index.startOffset();
            } else {
                if (expectStreamId != index.streamId() || expectOffset != index.startOffset()) {
                    throw new IllegalArgumentException(String.format("Invalid index %s, expect streamId=%s, offset=%s",
                        index, expectStreamId, expectOffset));
                }
            }
            expectOffset = index.endOffset();
        }
    }

    static class ObjectsBlock {
        private final ByteBuf buf;

        public ObjectsBlock(NavigableMap<Integer, LinkedObject> components) {
            buf = ByteBufAlloc.byteBuffer(OBJECT_BLOCK_HEADER_SIZE + OBJECT_UNIT_SIZE * components.size(), WRITE_INDEX_BLOCK);
            buf.writeByte(OBJECTS_BLOCK_MAGIC);
            buf.writeInt(components.size());
            components.forEach((blockStartIndex, linkedObject) -> {
                buf.writeLong(linkedObject.metadata.objectId());
                buf.writeInt(blockStartIndex);
                buf.writeShort(ObjectAttributes.from(linkedObject.metadata.attributes()).bucket());
            });
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public int size() {
            return buf.readableBytes();
        }
    }

    static class IndexesBlock {
        private final ByteBuf buf;

        public IndexesBlock(NavigableMap<Integer, LinkedObject> components) {
            int indexesCount = 0;
            for (LinkedObject linkedObject : components.values()) {
                indexesCount += linkedObject.indexes.size();
            }
            int indexBlockSize = DataBlockIndex.BLOCK_INDEX_SIZE * indexesCount;
            buf = ByteBufAlloc.byteBuffer(indexBlockSize, WRITE_INDEX_BLOCK);
            components.forEach((blockStartIndex, linkedObject) -> linkedObject.indexes.forEach(index -> index.encode(buf)));
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public int size() {
            return buf.readableBytes();
        }
    }

    static class Footer {
        private final ByteBuf buf;

        public Footer(long indexesBlockStartPosition, int indexBlockLength) {
            buf = ByteBufAlloc.byteBuffer(FOOTER_SIZE, WRITE_FOOTER);
            // start position of index block
            buf.writeLong(indexesBlockStartPosition);
            // size of index block
            buf.writeInt(indexBlockLength);
            // reserved for future
            buf.writeZero(40 - 8 - 4);
            buf.writeLong(FOOTER_MAGIC);
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public int size() {
            return FOOTER_SIZE;
        }

    }

    static class LinkedObject {
        final S3ObjectMetadata metadata;
        final List<DataBlockIndex> indexes;

        public LinkedObject(S3ObjectMetadata metadata, List<DataBlockIndex> indexes) {
            this.metadata = metadata;
            this.indexes = indexes;
        }
    }
}
