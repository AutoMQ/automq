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
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.operator.Writer;
import org.apache.kafka.metadata.stream.ObjectUtils;
import org.apache.kafka.metadata.stream.S3ObjectMetadata;
import org.apache.kafka.metadata.stream.S3ObjectType;

public class StreamObjectCopyer {
    private final List<StreamObjectIndexData> completedObjects;
    private final S3Operator s3Operator;
    private final Writer writer;
    private final long objectId;
    private long nextObjectDataStartPosition;
    private int blockCount;

    private long size;

    public StreamObjectCopyer(long objectId, S3Operator s3Operator) {
        // TODO: use a better clusterName
        this(objectId, s3Operator, s3Operator.writer(ObjectUtils.genKey(0, "todocluster", objectId)));
    }

    public StreamObjectCopyer(long objectId, S3Operator s3Operator, Writer writer) {
        this.objectId = objectId;
        this.s3Operator = s3Operator;
        this.writer = writer;
        this.completedObjects = new LinkedList<>();
        this.nextObjectDataStartPosition = 0;
        this.blockCount = 0;
        this.size = 0;
    }

    public void write(S3ObjectMetadata metadata) {
        if (metadata.getType() != S3ObjectType.STREAM) {
            throw new IllegalArgumentException("Only stream object can be handled.");
        }
        ObjectReader reader = new ObjectReader(metadata, s3Operator);
        ObjectReader.BasicObjectInfo basicObjectInfo = reader.basicObjectInfo().join();
        // Only copy data blocks for now.
        writer.copyWrite(metadata.key(), 0, basicObjectInfo.dataBlockSize());
        completedObjects.add(new StreamObjectIndexData(basicObjectInfo.indexBlock(), basicObjectInfo.blockCount(), nextObjectDataStartPosition, blockCount, basicObjectInfo.indexBlockSize()));
        blockCount += basicObjectInfo.blockCount();
        nextObjectDataStartPosition += basicObjectInfo.dataBlockSize();
        size += basicObjectInfo.dataBlockSize();
    }

    public CompletableFuture<Void> close() {
        CompositeByteBuf buf = Unpooled.compositeBuffer();
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
            buf = Unpooled.compositeBuffer();
            // block count
            buf.addComponent(true, Unpooled.buffer(4).writeInt(blockCount));
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
        /**
         * how many data blocks in this object.
         */
        private final int dataBlockCount;
        /**
         * The total length of the block index.
         */
        private final int blockIndexTotalLength;

        public StreamObjectIndexData(ObjectReader.IndexBlock indexBlock, int dataBlockCount, long blockStartPosition, int blockStartId, int blockIndexTotalLength) {
            this.dataBlockCount = dataBlockCount;
            this.blockIndexTotalLength = blockIndexTotalLength;
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
