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

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.metadata.stream.ObjectUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

// TODO: memory optimization

/**
 * Write stream records to a single object.
 */
public interface ObjectWriter {

    byte DATA_BLOCK_MAGIC = 0x01;
    // TODO: first n bit is the compressed flag
    byte DATA_BLOCK_DEFAULT_FLAG = 0x02;

    void write(long streamId, List<StreamRecordBatch> records);

    CompletableFuture<Void> close();

    List<ObjectStreamRange> getStreamRanges();

    long objectId();

    long size();

    static ObjectWriter writer(long objectId, S3Operator s3Operator, int blockSizeThreshold, int partSizeThreshold) {
        return new DefaultObjectWriter(objectId, s3Operator, blockSizeThreshold, partSizeThreshold);
    }

    static ObjectWriter noop(long objectId) {
        return new NoopObjectWriter(objectId);
    }

    class DefaultObjectWriter implements ObjectWriter {

        private final int blockSizeThreshold;
        private final int partSizeThreshold;
        private final List<DataBlock> waitingUploadBlocks;
        private final List<DataBlock> completedBlocks;
        private IndexBlock indexBlock;
        private final Writer writer;
        private final long objectId;

        private long size;

        /**
         * Create a new object writer.
         *
         * @param objectId           object id
         * @param s3Operator         S3 operator
         * @param blockSizeThreshold the max size of a block
         * @param partSizeThreshold  the max size of a part. If it is smaller than {@link Writer#MIN_PART_SIZE}, it will be set to {@link Writer#MIN_PART_SIZE}.
         */
        public DefaultObjectWriter(long objectId, S3Operator s3Operator, int blockSizeThreshold, int partSizeThreshold) {
            this.objectId = objectId;
            // TODO: use a better clusterName
            String objectKey = ObjectUtils.genKey(0, objectId);
            this.blockSizeThreshold = blockSizeThreshold;
            this.partSizeThreshold = Math.max(Writer.MIN_PART_SIZE, partSizeThreshold);
            waitingUploadBlocks = new LinkedList<>();
            completedBlocks = new LinkedList<>();
            writer = s3Operator.writer(objectKey);
        }

        public void write(long streamId, List<StreamRecordBatch> records) {
            List<List<StreamRecordBatch>> blocks = groupByBlock(records);
            List<CompletableFuture<Void>> closeCf = new ArrayList<>(blocks.size());
            blocks.forEach(blockRecords -> {
                DataBlock block = new DataBlock(streamId, blockRecords);
                waitingUploadBlocks.add(block);
                closeCf.add(block.close());
            });
            CompletableFuture.allOf(closeCf.toArray(new CompletableFuture[0])).whenComplete((nil, ex) -> tryUploadPart());

        }

        private List<List<StreamRecordBatch>> groupByBlock(List<StreamRecordBatch> records) {
            List<List<StreamRecordBatch>> blocks = new LinkedList<>();
            List<StreamRecordBatch> blockRecords = new ArrayList<>(records.size());
            for (StreamRecordBatch record : records) {
                size += record.size();
                blockRecords.add(record);
                if (size >= blockSizeThreshold) {
                    blocks.add(blockRecords);
                    blockRecords = new ArrayList<>(records.size());
                    size = 0;
                }
            }
            if (!blockRecords.isEmpty()) {
                blocks.add(blockRecords);
            }
            return blocks;
        }

        private synchronized void tryUploadPart() {
            for (; ; ) {
                List<DataBlock> uploadBlocks = new ArrayList<>(32);
                boolean partFull = false;
                int size = 0;
                for (DataBlock block : waitingUploadBlocks) {
                    if (!block.close().isDone()) {
                        break;
                    }
                    uploadBlocks.add(block);
                    size += block.size();
                    if (size >= partSizeThreshold) {
                        partFull = true;
                        break;
                    }
                }
                if (partFull) {
                    CompositeByteBuf partBuf = ByteBufAlloc.ALLOC.compositeBuffer();
                    for (DataBlock block : uploadBlocks) {
                        partBuf.addComponent(true, block.buffer());
                    }
                    writer.write(partBuf);
                    completedBlocks.addAll(uploadBlocks);
                    waitingUploadBlocks.removeIf(uploadBlocks::contains);
                } else {
                    break;
                }
            }
        }

        public CompletableFuture<Void> close() {
            CompletableFuture<Void> waitBlocksCloseCf = CompletableFuture.allOf(waitingUploadBlocks.stream().map(DataBlock::close).toArray(CompletableFuture[]::new));
            return waitBlocksCloseCf.thenCompose(nil -> {
                CompositeByteBuf buf = ByteBufAlloc.ALLOC.compositeBuffer();
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
            });
        }

        public List<ObjectStreamRange> getStreamRanges() {
            List<ObjectStreamRange> streamRanges = new LinkedList<>();
            ObjectStreamRange lastStreamRange = null;
            for (DataBlock block : completedBlocks) {
                ObjectStreamRange streamRange = block.getStreamRange();
                if (lastStreamRange == null || lastStreamRange.getStreamId() != streamRange.getStreamId()) {
                    if (lastStreamRange != null) {
                        streamRanges.add(lastStreamRange);
                    }
                    lastStreamRange = new ObjectStreamRange();
                    lastStreamRange.setStreamId(streamRange.getStreamId());
                    lastStreamRange.setEpoch(streamRange.getEpoch());
                    lastStreamRange.setStartOffset(streamRange.getStartOffset());
                }
                lastStreamRange.setEndOffset(streamRange.getEndOffset());
            }
            if (lastStreamRange != null) {
                streamRanges.add(lastStreamRange);
            }
            return streamRanges;
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
                long nextPosition = 0;
                buf = Unpooled.buffer(1024 * 1024);
                buf.writeInt(completedBlocks.size()); // block count
                // block index
                for (DataBlock block : completedBlocks) {
                    // start position in the object
                    buf.writeLong(nextPosition);
                    // byte size of the block
                    buf.writeInt(block.size());
                    // how many ranges in the block
                    buf.writeInt(block.recordCount());
                    nextPosition += block.size();
                }
                position = nextPosition;
                // object stream range
                for (int blockIndex = 0; blockIndex < completedBlocks.size(); blockIndex++) {
                    DataBlock block = completedBlocks.get(blockIndex);
                    ObjectStreamRange range = block.getStreamRange();
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
    }

    class DataBlock {
        private final ByteBuf encodedBuf;
        private final ObjectStreamRange streamRange;
        private final int recordCount;
        private final int size;

        public DataBlock(long streamId, List<StreamRecordBatch> records) {
            this.streamRange = new ObjectStreamRange(streamId, records.get(0).getEpoch(), records.get(0).getBaseOffset(), records.get(records.size() - 1).getLastOffset());
            this.recordCount = records.size();
            int dataSize = records.stream().mapToInt(r -> r.encoded().readableBytes()).sum();
            this.encodedBuf = ByteBufAlloc.ALLOC.directBuffer(2 + 2 + dataSize);
            encodedBuf.writeByte(DATA_BLOCK_MAGIC);
            encodedBuf.writeByte(DATA_BLOCK_DEFAULT_FLAG);
            records.forEach(r -> encodedBuf.writeBytes(r.encoded()));
            this.size = encodedBuf.readableBytes();
        }

        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        public int size() {
            return size;
        }

        public int recordCount() {
            return recordCount;
        }

        public ObjectStreamRange getStreamRange() {
            return streamRange;
        }

        public ByteBuf buffer() {
            return encodedBuf.duplicate();
        }
    }

    class Footer {
        public static final int FOOTER_SIZE = 48;
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

    class NoopObjectWriter implements ObjectWriter {
        private final long objectId;

        public NoopObjectWriter(long objectId) {
            this.objectId = objectId;
        }

        @Override
        public void write(long streamId, List<StreamRecordBatch> records) {
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public List<ObjectStreamRange> getStreamRanges() {
            return Collections.emptyList();
        }

        @Override
        public long objectId() {
            return objectId;
        }

        @Override
        public long size() {
            return 0;
        }
    }

}
