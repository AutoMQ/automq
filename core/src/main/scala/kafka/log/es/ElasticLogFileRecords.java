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
 *
 */

package kafka.log.es;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.LogInputStream;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordBatchIterator;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.UnalignedFileRecords;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Time;
import sdk.elastic.stream.api.FetchResult;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.OptionalLong;
import java.util.Queue;

public class ElasticLogFileRecords extends FileRecords {
    private final ElasticStreamSegment streamSegment;
    private long lastModifiedTimeMs = System.currentTimeMillis();

    public ElasticLogFileRecords(ElasticStreamSegment streamSegment) {
        super(0, Integer.MAX_VALUE, false);
        this.streamSegment = streamSegment;
        size.set((int) streamSegment.nextOffset());
    }

    @Override
    public File file() {
        // TODO:
        return new File("mock");
    }

    public FileChannel channel() {
        // TODO: remove direct reference
        return null;
    }

    @Override
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        // don't expect to be called
        throw new UnsupportedOperationException();
    }

    @Override
    public FileRecords slice(int position, int size) throws IOException {
        // don't expect to be called
        throw new UnsupportedOperationException();
    }

    public Records read(int position, int maxSizeHint) throws IOException {
        try {
            FetchResult rst = streamSegment.fetch(position, maxSizeHint).get();
            CompositeByteBuf composited = Unpooled.compositeBuffer();
            rst.recordBatchList().forEach(r -> {
                composited.addComponent(true, Unpooled.wrappedBuffer(r.rawPayload()));
            });
            return MemoryRecords.readableRecords(composited.nioBuffer());
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    @Override
    public UnalignedFileRecords sliceUnaligned(int position, int size) {
        // don't expect to be called
        throw new UnsupportedOperationException();
    }

    @Override
    public int append(MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());
        int appendSize = records.sizeInBytes();
        streamSegment.append(RawPayloadRecordBatch.of(records.buffer()));
        size.getAndAdd(appendSize);
        lastModifiedTimeMs = System.currentTimeMillis();
        return appendSize;
    }

    @Override
    public void flush() throws IOException {
        //TODO: await all async append complete
    }

    @Override
    public void close() throws IOException {
        // TODO: recycle resource
    }

    @Override
    public void closeHandlers() throws IOException {
        // noop implementation
    }

    @Override
    public boolean deleteIfExists() throws IOException {
        // TODO: delete segment by outer segment
        return true;
    }

    @Override
    public void trim() throws IOException {
        // noop implementation.
    }

    @Override
    public void updateParentDir(File parentDir) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTo(File f) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int truncateTo(int targetSize) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long writeTo(TransferableChannel destChannel, long offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long getLastModifiedTimeMs() {
        return lastModifiedTimeMs;
    }

    protected RecordBatchIterator<FileLogInputStream.FileChannelRecordBatch> batchIterator(int start) {
        LogInputStream<FileLogInputStream.FileChannelRecordBatch> inputStream = new StreamSegmentInputStream(this, start, sizeInBytes());
        return new RecordBatchIterator<>(inputStream);
    }

    static class StreamSegmentInputStream implements LogInputStream<FileLogInputStream.FileChannelRecordBatch> {
        private final ElasticLogFileRecords elasticLogFileRecords;
        private final int end;
        private final Queue<FileChannelRecordBatchWrapper> remaining = new LinkedList<>();
        private int position;

        public StreamSegmentInputStream(ElasticLogFileRecords elasticLogFileRecords, int start, int end) {
            this.elasticLogFileRecords = elasticLogFileRecords;
            this.end = end;
            this.position = start;
        }


        @Override
        public FileLogInputStream.FileChannelRecordBatch nextBatch() throws IOException {
            for (; ; ) {
                FileChannelRecordBatchWrapper recordBatch = remaining.poll();
                if (recordBatch != null) {
                    return recordBatch;
                }
                // TODO: end 有点问题
                if (position >= end - HEADER_SIZE_UP_TO_MAGIC)
                    return null;
                try {
                    FetchResult rst = elasticLogFileRecords.streamSegment.fetch(position, 1).get();
                    rst.recordBatchList().forEach(streamRecord -> {
                        for (RecordBatch r : MemoryRecords.readableRecords(streamRecord.rawPayload()).batches()) {
                            remaining.offer(new FileChannelRecordBatchWrapper(r, position));
                            position += r.sizeInBytes();
                        }
                    });
                    if (remaining.isEmpty()) {
                        return null;
                    }
                } catch (Throwable e) {
                    throw new IOException(e);
                }
            }
        }
    }

    static class FileChannelRecordBatchWrapper extends FileLogInputStream.FileChannelRecordBatch {
        private final RecordBatch inner;
        private final int position;

        public FileChannelRecordBatchWrapper(RecordBatch recordBatch, int position) {
            this.inner = recordBatch;
            this.position = position;
        }

        @Override
        public boolean isValid() {
            return inner.isValid();
        }

        @Override
        public void ensureValid() {
            inner.ensureValid();
        }

        @Override
        public long checksum() {
            return inner.checksum();
        }

        @Override
        public long maxTimestamp() {
            return inner.maxTimestamp();
        }

        @Override
        public TimestampType timestampType() {
            return inner.timestampType();
        }

        @Override
        public long baseOffset() {
            return inner.baseOffset();
        }

        @Override
        public long lastOffset() {
            return inner.lastOffset();
        }

        @Override
        public long nextOffset() {
            return inner.nextOffset();
        }

        @Override
        public byte magic() {
            return inner.magic();
        }

        @Override
        public long producerId() {
            return inner.producerId();
        }

        @Override
        public short producerEpoch() {
            return inner.producerEpoch();
        }

        @Override
        public boolean hasProducerId() {
            return inner.hasProducerId();
        }

        @Override
        public int baseSequence() {
            return inner.baseSequence();
        }

        @Override
        public int lastSequence() {
            return inner.lastSequence();
        }

        @Override
        public CompressionType compressionType() {
            return inner.compressionType();
        }

        @Override
        public int sizeInBytes() {
            return inner.sizeInBytes();
        }

        @Override
        public Integer countOrNull() {
            return inner.countOrNull();
        }

        @Override
        public boolean isCompressed() {
            return inner.isCompressed();
        }

        @Override
        public void writeTo(ByteBuffer buffer) {
            inner.writeTo(buffer);
        }

        @Override
        protected RecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int headerSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int position() {
            return position;
        }

        @Override
        public boolean isTransactional() {
            return inner.isTransactional();
        }

        @Override
        public OptionalLong deleteHorizonMs() {
            return inner.deleteHorizonMs();
        }

        @Override
        public int partitionLeaderEpoch() {
            return inner.partitionLeaderEpoch();
        }

        @Override
        public CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier) {
            return inner.streamingIterator(decompressionBufferSupplier);
        }

        @Override
        public boolean isControlBatch() {
            return inner.isControlBatch();
        }

        @Override
        public Iterator<Record> iterator() {
            return inner.iterator();
        }
    }

}
