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
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import kafka.log.s3.compact.objects.StreamDataBlock;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.operator.Writer;
import org.apache.kafka.metadata.stream.ObjectUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

//TODO: refactor to reduce duplicate code with ObjectWriter
public class DataBlockWriter {
    private final int partSizeThreshold;
    private final List<StreamDataBlock> waitingUploadBlocks;
    private final Map<StreamDataBlock, CompletableFuture<Void>> waitingUploadBlockCfs;
    private final List<StreamDataBlock> completedBlocks;
    private IndexBlock indexBlock;
    private final Writer writer;
    private final long objectId;
    private long nextDataBlockPosition;
    private long size;

    public DataBlockWriter(long objectId, S3Operator s3Operator, int partSizeThreshold) {
        this.objectId = objectId;
        String objectKey = ObjectUtils.genKey(0, "todocluster", objectId);
        this.partSizeThreshold = partSizeThreshold;
        waitingUploadBlocks = new LinkedList<>();
        waitingUploadBlockCfs = new ConcurrentHashMap<>();
        completedBlocks = new LinkedList<>();
        writer = s3Operator.writer(objectKey);
    }

    public long getObjectId() {
        return objectId;
    }

    public CompletableFuture<Void> write(StreamDataBlock dataBlock) {
        CompletableFuture<Void> writeCf = new CompletableFuture<>();
        waitingUploadBlockCfs.put(dataBlock, writeCf);
        waitingUploadBlocks.add(dataBlock);
        long waitingUploadSize = waitingUploadBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum();
        if (waitingUploadSize >= partSizeThreshold) {
            uploadWaitingList();
        }
        return writeCf;
    }

    public void copyWrite(StreamDataBlock dataBlock) {
        // size of data block is always smaller than MAX_PART_SIZE, no need to split into multiple parts
        String originObjectKey = ObjectUtils.genKey(0, "todocluster", dataBlock.getObjectId());
        writer.copyWrite(originObjectKey,
                dataBlock.getBlockPosition(), dataBlock.getBlockPosition() + dataBlock.getBlockSize());
        completedBlocks.add(dataBlock);
        nextDataBlockPosition += dataBlock.getBlockSize();
    }

    public void uploadWaitingList() {
        CompositeByteBuf partBuf = Unpooled.compositeBuffer();
        for (StreamDataBlock block : waitingUploadBlocks) {
            partBuf.addComponent(true, block.getDataCf().join());
            completedBlocks.add(block);
            nextDataBlockPosition += block.getBlockSize();
        }
        List<StreamDataBlock> blocks = new LinkedList<>(waitingUploadBlocks);
        writer.write(partBuf).thenAccept(v -> {
            for (StreamDataBlock block : blocks) {
                waitingUploadBlockCfs.get(block).complete(null);
                waitingUploadBlockCfs.remove(block);
            }
        });
        waitingUploadBlocks.clear();
    }

    public CompletableFuture<Void> close() {
        CompositeByteBuf buf = Unpooled.compositeBuffer();
        for (StreamDataBlock block : waitingUploadBlocks) {
            buf.addComponent(true, block.getDataCf().join());
            completedBlocks.add(block);
            nextDataBlockPosition += block.getBlockSize();
        }
        waitingUploadBlocks.clear();
        indexBlock = new IndexBlock();
        buf.addComponent(true, indexBlock.buffer());
        Footer footer = new Footer();
        buf.addComponent(true, footer.buffer());
        writer.write(buf.duplicate());
        size = indexBlock.position() + indexBlock.size() + footer.size();
        return writer.close();
    }

    public long objectId() {
        return objectId;
    }

    public long size() {
        return size;
    }

    class IndexBlock {
        private final ByteBuf buf;
        private final long position;

        public IndexBlock() {
            position = nextDataBlockPosition;
            buf = Unpooled.buffer(calculateIndexBlockSize());
            buf.writeInt(completedBlocks.size()); // block count
            long nextPosition = 0;
            // block index
            for (StreamDataBlock block : completedBlocks) {
                buf.writeLong(nextPosition);
                buf.writeInt(block.getBlockSize());
                buf.writeInt(block.getRecordCount());
                nextPosition += block.getBlockSize();
            }

            // object stream range
            for (int blockIndex = 0; blockIndex < completedBlocks.size(); blockIndex++) {
                StreamDataBlock block = completedBlocks.get(blockIndex);
                buf.writeLong(block.getStreamId());
                buf.writeLong(block.getStartOffset());
                buf.writeInt((int) (block.getEndOffset() - block.getStartOffset()));
                buf.writeInt(blockIndex);
            }
        }

        private int calculateIndexBlockSize() {
            return 4 + completedBlocks.size() * 40;
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

    class Footer {
        private static final int FOOTER_SIZE = 48;
        private static final long MAGIC = 0x88e241b785f4cff7L;
        private final ByteBuf buf;

        public Footer() {
            buf = Unpooled.buffer(FOOTER_SIZE);
            buf.writeLong(indexBlock.position());
            buf.writeInt(indexBlock.size());
            buf.writeZero(40 - 8 - 4);
            buf.writeLong(MAGIC);
        }

        public ByteBuf buffer() {
            return buf.duplicate();
        }

        public int size() {
            return FOOTER_SIZE;
        }

    }
}
