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

import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamObjectCopier {
    private final List<StreamObjectIndexData> completedObjects;
    private final S3Operator s3Operator;
    private final Writer writer;
    private long nextObjectDataStartPosition;
    private int blockCount;

    private long size;

    public StreamObjectCopier(long objectId, S3Operator s3Operator) {
        this.s3Operator = s3Operator;
        // TODO: use a better clusterName
        this.writer = s3Operator.writer(ObjectUtils.genKey(0, objectId), "[StreamObjectCopier objId=" + objectId + "]");
        this.completedObjects = new LinkedList<>();
        this.nextObjectDataStartPosition = 0;
        this.blockCount = 0;
        this.size = 0;
    }

    public void copy(S3ObjectMetadata metadata) {
        splitAndCopy(metadata, 1);
    }

    public void splitAndCopy(S3ObjectMetadata metadata, int splitCount) {
        if (metadata.getType() != S3ObjectType.STREAM) {
            throw new IllegalArgumentException("Only stream object can be handled.");
        }
        if (metadata.objectSize() <= 0) {
            throw new IllegalArgumentException("Object size must be positive.");
        }
        if (splitCount <= 0) {
            throw new IllegalArgumentException("Split count must be positive.");
        }
        try (ObjectReader reader = new ObjectReader(metadata, s3Operator)) {
            ObjectReader.BasicObjectInfo basicObjectInfo = reader.basicObjectInfo().join();

            long restBytes = basicObjectInfo.dataBlockSize();
            // Only copy data blocks for now.
            for (long i = 0; i < splitCount - 1 && restBytes >= Writer.MAX_PART_SIZE; i++) {
                writer.copyWrite(metadata.key(), i * Writer.MAX_PART_SIZE, (i + 1) * Writer.MAX_PART_SIZE);
                restBytes -= Writer.MAX_PART_SIZE;
            }
            if (restBytes > Writer.MAX_PART_SIZE) {
                throw new IllegalArgumentException("splitCount is too small, resting bytes: " + restBytes + " is larger than MAX_PART_SIZE: " + Writer.MAX_PART_SIZE + ".");
            }
            if (restBytes > 0) {
                writer.copyWrite(metadata.key(), (splitCount - 1) * Writer.MAX_PART_SIZE, basicObjectInfo.dataBlockSize());
            }

            completedObjects.add(new StreamObjectIndexData(basicObjectInfo.indexBlock(), nextObjectDataStartPosition, blockCount));
            blockCount += basicObjectInfo.blockCount();
            nextObjectDataStartPosition += basicObjectInfo.dataBlockSize();
            size += basicObjectInfo.dataBlockSize();
        }
    }

    public CompletableFuture<Void> close() {
        CompositeByteBuf buf = ByteBufAlloc.ALLOC.compositeBuffer();
        IndexBlock indexBlock = new IndexBlock();
        buf.addComponent(true, indexBlock.buffer());
        ObjectWriter.Footer footer = new ObjectWriter.Footer(indexBlock.position(), indexBlock.size());
        buf.addComponent(true, footer.buffer());
        writer.write(buf.duplicate());
        size += indexBlock.size() + footer.size();
        return writer.close();
    }

    public long size() {
        return size;
    }

    private class IndexBlock {
        private final CompositeByteBuf buf;
        private final long position;

        public IndexBlock() {
            position = nextObjectDataStartPosition;
            buf = ByteBufAlloc.ALLOC.compositeBuffer();
            // block count
            buf.addComponent(true, ByteBufAlloc.ALLOC.buffer(4).writeInt(blockCount));
            // block index
            for (StreamObjectIndexData indexData : completedObjects) {
                buf.addComponent(true, indexData.blockBuf());
            }
            // object stream range
            for (StreamObjectIndexData indexData : completedObjects) {
                buf.addComponent(true, indexData.rangesBuf());
            }
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public long position() {
            return position;
        }

        public int size() {
            return buf.readableBytes();
        }
    }

    static class StreamObjectIndexData {
        private final ByteBuf blockBuf;
        private final ByteBuf rangesBuf;

        public StreamObjectIndexData(ObjectReader.IndexBlock indexBlock, long blockStartPosition, int blockStartId) {
            this.blockBuf = indexBlock.blocks().copy();
            this.rangesBuf = indexBlock.streamRanges().copy();

            int blockPositionIndex = 0;
            while (blockPositionIndex < blockBuf.readableBytes()) {
                // The value is now the relative block position.
                long blockPosition = blockBuf.getLong(blockPositionIndex);
                // update block position with start position.
                blockBuf.setLong(blockPositionIndex, blockPosition + blockStartPosition);
                blockPositionIndex += 8 + 4 + 4;
            }

            int startBlockIdIndex = 8 + 8 + 4;
            while (startBlockIdIndex < rangesBuf.readableBytes()) {
                // The value is now the relative block id.
                int blockId = rangesBuf.getInt(startBlockIdIndex);
                // update block id with start block id.
                rangesBuf.setInt(startBlockIdIndex, blockId + blockStartId);
                startBlockIdIndex += 8 + 8 + 4 + 4;
            }
        }

        public ByteBuf blockBuf() {
            return blockBuf.duplicate();
        }

        public ByteBuf rangesBuf() {
            return rangesBuf.duplicate();
        }
    }
}
