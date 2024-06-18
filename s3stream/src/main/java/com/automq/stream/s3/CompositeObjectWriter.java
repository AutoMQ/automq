/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

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
    private int size = -1;
    private int nextBlockStartIndex = 0;

    private final Writer writer;

    public CompositeObjectWriter(Writer writer) {
        this.writer = writer;
    }

    public void addComponent(S3ObjectMetadata partObjectMetadata, List<DataBlockIndex> partObjectIndexes) {
        continuousCheck(partObjectIndexes);
        components.put(nextBlockStartIndex, new LinkedObject(partObjectMetadata, partObjectIndexes));
        nextBlockStartIndex += partObjectIndexes.size();
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
        size = objBuf.readableBytes();
        writer.write(objBuf);
        return writer.close();
    }

    @Override
    public List<ObjectStreamRange> getStreamRanges() {
        return List.of(new ObjectStreamRange(expectStreamId, Constants.NOOP_EPOCH, startOffset, expectOffset, size));
    }

    @Override
    public long size() {
        return size;
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
