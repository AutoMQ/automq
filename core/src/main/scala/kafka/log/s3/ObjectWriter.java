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
import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.operator.Writer;
import org.apache.kafka.common.compress.ZstdFactory;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.metadata.stream.ObjectUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// TODO: memory optimization
public class ObjectWriter {
    private final int blockSizeThreshold;
    private final int partSizeThreshold;
    private final List<DataBlock> waitingUploadBlocks;
    private final List<DataBlock> completedBlocks;
    private IndexBlock indexBlock;
    private final Writer writer;
    private final long objectId;
    private long nextDataBlockPosition;

    private long size;

    private DataBlock dataBlock;

    public ObjectWriter(long objectId, S3Operator s3Operator, int blockSizeThreshold, int partSizeThreshold) {
        this.objectId = objectId;
        // TODO: use a better clusterName
        String objectKey = ObjectUtils.genKey(0, "todocluster", objectId);
        this.blockSizeThreshold = blockSizeThreshold;
        this.partSizeThreshold = partSizeThreshold;
        waitingUploadBlocks = new LinkedList<>();
        completedBlocks = new LinkedList<>();
        writer = s3Operator.writer(objectKey);
    }

    public void write(StreamRecordBatch record) {
        if (dataBlock == null) {
            dataBlock = new DataBlock(nextDataBlockPosition);
        }
        if (dataBlock.write(record)) {
            waitingUploadBlocks.add(dataBlock);
            nextDataBlockPosition += dataBlock.size();
            dataBlock = null;
            tryUploadPart();
        }
    }

    public void closeCurrentBlock() {
        if (dataBlock != null) {
            dataBlock.close();
            waitingUploadBlocks.add(dataBlock);
            nextDataBlockPosition += dataBlock.size();
            dataBlock = null;
            tryUploadPart();
        }
    }

    private void tryUploadPart() {
        long waitingUploadSize = waitingUploadBlocks.stream().mapToLong(DataBlock::size).sum();
        if (waitingUploadSize >= partSizeThreshold) {
            CompositeByteBuf partBuf = Unpooled.compositeBuffer();
            for (DataBlock block : waitingUploadBlocks) {
                partBuf.addComponent(true, block.buffer());
                completedBlocks.add(block);
            }
            writer.write(partBuf);
            waitingUploadBlocks.clear();
        }
    }

    public CompletableFuture<Void> close() {
        CompositeByteBuf buf = Unpooled.compositeBuffer();
        if (dataBlock != null) {
            dataBlock.close();
            nextDataBlockPosition += dataBlock.size();
            waitingUploadBlocks.add(dataBlock);
            dataBlock = null;
        }
        for (DataBlock block : waitingUploadBlocks) {
            buf.addComponent(true, block.buffer());
            completedBlocks.add(block);
        }
        waitingUploadBlocks.clear();
        indexBlock = new IndexBlock();
        buf.addComponent(true, indexBlock.buffer());
        Footer footer = new Footer(indexBlock.position(), indexBlock.size());
        buf.addComponent(true, footer.buffer());
        writer.write(buf.duplicate());
        size = indexBlock.position() + indexBlock.size() + footer.size();
        return writer.close();
    }

    public List<ObjectStreamRange> getStreamRanges() {
        List<ObjectStreamRange> streamRanges = new LinkedList<>();
        ObjectStreamRange lastStreamRange = null;
        for (DataBlock block : completedBlocks) {
            for (ObjectStreamRange streamRange : block.getStreamRanges()) {
                if (lastStreamRange == null || lastStreamRange.getStreamId() != streamRange.getStreamId()) {
                    lastStreamRange = new ObjectStreamRange();
                    lastStreamRange.setStreamId(streamRange.getStreamId());
                    lastStreamRange.setEpoch(streamRange.getEpoch());
                    lastStreamRange.setStartOffset(streamRange.getStartOffset());
                    streamRanges.add(lastStreamRange);
                }
                lastStreamRange.setEndOffset(streamRange.getEndOffset());

            }
        }
        return streamRanges;
    }

    public long objectId() {
        return objectId;
    }

    public long size() {
        return size;
    }

    class DataBlock {
        private final long position;
        private ByteBufferOutputStream compressedBlock;
        private OutputStream out;
        private ByteBuf compressedBlockBuf;
        private int blockSize;
        private final List<ObjectStreamRange> streamRanges;
        private ObjectStreamRange streamRange;
        private int recordCount = 0;

        public DataBlock(long position) {
            this.position = position;
            compressedBlock = new ByteBufferOutputStream(blockSizeThreshold * 3 / 2);
            out = ZstdFactory.wrapForOutput(compressedBlock);
            streamRanges = new LinkedList<>();
        }

        public boolean write(StreamRecordBatch record) {
            try {
                recordCount++;
                return write0(record);
            } catch (IOException ex) {
                // won't happen
                throw new RuntimeException(ex);
            }
        }

        public boolean write0(StreamRecordBatch record) throws IOException {
            if (streamRange == null || streamRange.getStreamId() != record.getStreamId()) {
                streamRange = new ObjectStreamRange();
                streamRange.setStreamId(record.getStreamId());
                streamRange.setEpoch(record.getEpoch());
                streamRange.setStartOffset(record.getBaseOffset());
                streamRanges.add(streamRange);
            }
            streamRange.setEndOffset(record.getLastOffset());

            ByteBuf recordBuf = record.encoded();
            out.write(recordBuf.array(), recordBuf.arrayOffset(), recordBuf.readableBytes());
            blockSize += recordBuf.readableBytes();
            if (blockSize >= blockSizeThreshold) {
                close();
                return true;
            }
            return false;
        }

        public void close() {
            try {
                close0();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void close0() throws IOException {
            out.close();
            compressedBlock.close();
            compressedBlockBuf = Unpooled.wrappedBuffer(compressedBlock.buffer().duplicate().flip());
            out = null;
            compressedBlock = null;
        }

        public long position() {
            return position;
        }

        public int size() {
            return compressedBlockBuf.readableBytes();
        }

        public int recordCount() {
            return recordCount;
        }

        public List<ObjectStreamRange> getStreamRanges() {
            return streamRanges;
        }

        public ByteBuf buffer() {
            return compressedBlockBuf.duplicate();
        }
    }

    class IndexBlock {
        private final ByteBuf buf;
        private final long position;

        public IndexBlock() {
            position = nextDataBlockPosition;
            buf = Unpooled.buffer(1024 * 1024);
            buf.writeInt(completedBlocks.size()); // block count
            // block index
            for (DataBlock block : completedBlocks) {
                // start position in the object
                buf.writeLong(block.position());
                // byte size of the block
                buf.writeInt(block.size());
                // how many ranges in the block
                buf.writeInt(block.recordCount());
            }
            // object stream range
            for (int blockIndex = 0; blockIndex < completedBlocks.size(); blockIndex++) {
                DataBlock block = completedBlocks.get(blockIndex);
                for (ObjectStreamRange range : block.getStreamRanges()) {
                    // stream id of this range
                    buf.writeLong(range.getStreamId());
                    // start offset of the related stream
                    buf.writeLong(range.getStartOffset());
                    // record count of the related stream in this range
                    buf.writeInt((int) (range.getEndOffset() - range.getStartOffset()));
                    // the index of block where this range is in
                    buf.writeInt(blockIndex);
                }
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

    static class Footer {
        private static final int FOOTER_SIZE = 48;
        private static final long MAGIC = 0x88e241b785f4cff7L;
        private final ByteBuf buf;

        public Footer(long indexStartPosition, int indexBlockLength) {
            buf = Unpooled.buffer(FOOTER_SIZE);
            // start position of index block
            buf.writeLong(indexStartPosition);
            // size of index block
            buf.writeInt(indexBlockLength);
            // reserved for future
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
