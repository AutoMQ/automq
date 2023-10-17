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

package kafka.log.streamaspect;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatchWithContext;
import org.apache.kafka.common.errors.es.SlowFetchHintException;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.LogInputStream;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.PooledResource;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordBatchIterator;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.RecordsUtil;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ElasticLogFileRecords {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticLogFileRecords.class);
    protected final AtomicInteger size;
    protected final Iterable<RecordBatch> batches;
    private final ElasticStreamSlice streamSlice;
    // This is The base offset of the corresponding segment.
    private final long baseOffset;
    private final AtomicLong nextOffset;
    private final AtomicLong committedOffset;
    // Inflight append result.
    private volatile CompletableFuture<?> lastAppend;
    private volatile ElasticResourceStatus status;


    public ElasticLogFileRecords(ElasticStreamSlice streamSlice, long baseOffset, int size) {
        this.baseOffset = baseOffset;
        this.streamSlice = streamSlice;
        long nextOffset = streamSlice.nextOffset();
        // Note that size is generally used to
        // 1) show the physical size of a segment. In these cases, size is refered to decide whether to roll a new
        // segment, or calculate the cleaned size in a cleaning task, etc. If size is not correctly recorded for any
        // reason, the worst thing will be just a bigger segment than configured.
        // 2) show whether this segment is empty, i.e., size == 0.
        // Therefore, it is fine to use the nextOffset as a backoff value.
        this.size = new AtomicInteger(size == 0 ? (int) nextOffset : size);
        this.nextOffset = new AtomicLong(baseOffset + nextOffset);
        this.committedOffset = new AtomicLong(baseOffset + nextOffset);
        this.lastAppend = CompletableFuture.completedFuture(null);

        batches = batchesFrom(baseOffset);
        status = ElasticResourceStatus.OK;
    }

    public int sizeInBytes() {
        return size.get();
    }

    public long nextOffset() {
        return nextOffset.get();
    }

    public long appendedOffset() {
        return nextOffset.get() - baseOffset;
    }

    public Records read(long startOffset, long maxOffset, int maxSize) throws SlowFetchHintException, IOException {
        if (ReadManualReleaseHint.isMarked()) {
            return readAll0(startOffset, maxOffset, maxSize);
        } else {
            return new BatchIteratorRecordsAdaptor(this, startOffset, maxOffset, maxSize);
        }
    }

    private Records readAll0(long startOffset, long maxOffset, int maxSize) throws SlowFetchHintException, IOException {
        int readSize = 0;
        // calculate the relative offset in the segment, which may start from 0.
        long nextFetchOffset = startOffset - baseOffset;
        long endOffset = Utils.min(this.committedOffset.get(), maxOffset) - baseOffset;
        if (nextFetchOffset >= endOffset) {
            return null;
        }
        List<FetchResult> fetchResults = new LinkedList<>();
        while (readSize <= maxSize && nextFetchOffset < endOffset) {
            FetchResult rst = streamSlice.fetch(nextFetchOffset, endOffset, Math.min(maxSize - readSize, 1024 * 1024));
            fetchResults.add(rst);
            for (RecordBatchWithContext recordBatchWithContext : rst.recordBatchList()) {
                if (recordBatchWithContext.lastOffset() > nextFetchOffset) {
                    nextFetchOffset = recordBatchWithContext.lastOffset();
                } else {
                    LOGGER.error("Invalid record batch, last offset {} is less than next offset {}",
                            recordBatchWithContext.lastOffset(), nextFetchOffset);
                    throw new IllegalStateException();
                }
                readSize += recordBatchWithContext.rawPayload().remaining();
            }
        }
        return PooledMemoryRecords.of(fetchResults);
    }

    /**
     * Append records to segment.
     * Note that lastOffset is the expected value of nextOffset after append. lastOffset = (the real last offset of the
     * records) + 1
     *
     * @param records    records to append
     * @param lastOffset expected next offset after append
     * @return the size of the appended records
     * @throws IOException
     */
    public int append(MemoryRecords records, long lastOffset) throws IOException {
        if (!status.writable()) {
            throw new IOException("Cannot append to a fenced log segment due to status " + status);
        }
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());
        int appendSize = records.sizeInBytes();
        // Note that the calculation of count requires strong consistency between nextOffset and the baseOffset of records.
        int count = (int) (lastOffset - nextOffset.get());
        com.automq.stream.DefaultRecordBatch batch = new com.automq.stream.DefaultRecordBatch(count, 0, Collections.emptyMap(), records.buffer());
        CompletableFuture<?> cf = streamSlice.append(batch);
        nextOffset.set(lastOffset);
        size.getAndAdd(appendSize);
        cf.whenComplete((rst, e) -> {
            if (e == null) {
                updateCommittedOffset(lastOffset);
            } else if (e instanceof IOException) {
                status = ElasticResourceStatus.FENCED;
                LOGGER.error("ElasticLogFileRecords[stream={}, baseOffset={}] fencing with ex: {}", streamSlice.stream().streamId(), baseOffset, e.getMessage());
            }
        });
        lastAppend = cf;
        return appendSize;
    }

    private void updateCommittedOffset(long newCommittedOffset) {
        while (true) {
            long oldCommittedOffset = this.committedOffset.get();
            if (oldCommittedOffset >= newCommittedOffset) {
                break;
            } else if (this.committedOffset.compareAndSet(oldCommittedOffset, newCommittedOffset)) {
                break;
            }
        }
    }

    public void flush() throws IOException {
        try {
            asyncFlush().get();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    public CompletableFuture<Void> asyncFlush() {
        return this.lastAppend.thenApply(rst -> null);
    }

    public void seal() {
        streamSlice.seal();
    }

    public void close() {
        status = ElasticResourceStatus.CLOSED;
    }

    public void closeHandlers() {
        status = ElasticResourceStatus.CLOSED;
    }

    public FileRecords.TimestampAndOffset searchForTimestamp(long targetTimestamp, long startingOffset) {
        for (RecordBatch batch : batchesFrom(startingOffset)) {
            if (batch.maxTimestamp() >= targetTimestamp) {
                // We found a message
                for (Record record : batch) {
                    long timestamp = record.timestamp();
                    if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                        return new FileRecords.TimestampAndOffset(timestamp, record.offset(),
                                maybeLeaderEpoch(batch.partitionLeaderEpoch()));
                }
            }
        }
        return null;
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
                Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * Return the largest timestamp of the messages after a given offset
     *
     * @param startOffset The starting offset.
     * @return The largest timestamp of the messages after the given position.
     */
    public FileRecords.TimestampAndOffset largestTimestampAfter(long startOffset) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long offsetOfMaxTimestamp = -1L;
        int leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH;

        for (RecordBatch batch : batchesFrom(startOffset)) {
            long timestamp = batch.maxTimestamp();
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                offsetOfMaxTimestamp = batch.lastOffset();
                leaderEpochOfMaxTimestamp = batch.partitionLeaderEpoch();
            }
        }
        return new FileRecords.TimestampAndOffset(maxTimestamp, offsetOfMaxTimestamp,
                maybeLeaderEpoch(leaderEpochOfMaxTimestamp));
    }

    public ElasticStreamSlice streamSegment() {
        return streamSlice;
    }

    public Iterable<RecordBatch> batchesFrom(final long startOffset) {
        return () -> batchIterator(startOffset, Long.MAX_VALUE, Integer.MAX_VALUE);
    }

    protected RecordBatchIterator<RecordBatch> batchIterator(long startOffset, long maxOffset, int fetchSize) {
        LogInputStream<RecordBatch> inputStream = new StreamSegmentInputStream(this, startOffset, maxOffset, fetchSize);
        return new RecordBatchIterator<>(inputStream);
    }

    public static class PooledMemoryRecords extends AbstractRecords implements PooledResource {
        private final List<FetchResult> fetchResults;
        private final MemoryRecords memoryRecords;

        private PooledMemoryRecords(List<FetchResult> fetchResults) {
            this.fetchResults = fetchResults;
            CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
            for (FetchResult fetchResult : fetchResults) {
                for (RecordBatchWithContext recordBatchWithContext : fetchResult.recordBatchList()) {
                    compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(recordBatchWithContext.rawPayload()));
                }
            }
            this.memoryRecords = MemoryRecords.readableRecords(compositeByteBuf.nioBuffer());
        }

        public static PooledMemoryRecords of(List<FetchResult> fetchResults) {
            return new PooledMemoryRecords(fetchResults);
        }

        @Override
        public int sizeInBytes() {
            return memoryRecords.sizeInBytes();
        }

        @Override
        public Iterable<? extends RecordBatch> batches() {
            return memoryRecords.batches();
        }

        @Override
        public AbstractIterator<? extends RecordBatch> batchIterator() {
            return memoryRecords.batchIterator();
        }

        @Override
        public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
            return memoryRecords.downConvert(toMagic, firstOffset, time);
        }

        @Override
        public long writeTo(TransferableChannel channel, long position, int length) throws IOException {
            return memoryRecords.writeTo(channel, position, length);
        }

        @Override
        public void release() {
            fetchResults.forEach(FetchResult::free);
            fetchResults.clear();
        }
    }

    static class StreamSegmentInputStream implements LogInputStream<RecordBatch> {
        private static final int FETCH_BATCH_SIZE = 64 * 1024;
        private final ElasticLogFileRecords elasticLogFileRecords;
        private final Queue<RecordBatch> remaining = new LinkedList<>();
        private final int maxSize;
        private final long endOffset;
        private long nextFetchOffset;
        private int readSize;


        public StreamSegmentInputStream(ElasticLogFileRecords elasticLogFileRecords, long startOffset, long maxOffset, int maxSize) {
            this.elasticLogFileRecords = elasticLogFileRecords;
            this.maxSize = maxSize;
            this.nextFetchOffset = startOffset - elasticLogFileRecords.baseOffset;
            this.endOffset = Utils.min(elasticLogFileRecords.committedOffset.get(), maxOffset) - elasticLogFileRecords.baseOffset;
        }


        @Override
        public RecordBatch nextBatch() throws IOException {
            for (; ; ) {
                RecordBatch recordBatch = remaining.poll();
                if (recordBatch != null) {
                    return recordBatch;
                }
                if (readSize > maxSize || nextFetchOffset >= endOffset) {
                    return null;
                }
                try {
                    FetchResult rst = elasticLogFileRecords.streamSlice.fetch(nextFetchOffset, endOffset, Math.min(maxSize - readSize, FETCH_BATCH_SIZE));
                    rst.recordBatchList().forEach(streamRecord -> {
                        try {
                            ByteBuffer buf = streamRecord.rawPayload();
                            if (buf.isDirect()) {
                                ByteBuffer heapBuf = ByteBuffer.allocate(buf.remaining());
                                heapBuf.put(buf);
                                heapBuf.flip();
                                buf = heapBuf;
                            }
                            readSize += buf.remaining();
                            for (RecordBatch r : MemoryRecords.readableRecords(buf).batches()) {
                                remaining.offer(r);
                                nextFetchOffset = r.lastOffset() - elasticLogFileRecords.baseOffset + 1;
                            }
                        } catch (Throwable e) {
                            ElasticStreamSlice slice = elasticLogFileRecords.streamSlice;
                            byte[] bytes = new byte[streamRecord.rawPayload().remaining()];
                            streamRecord.rawPayload().get(bytes);
                            LOGGER.error("next batch parse error, stream={} baseOffset={} payload={}", slice.stream().streamId(), slice.sliceRange().start() + streamRecord.baseOffset(), bytes);
                            throw new RuntimeException(e);
                        }
                    });
                    rst.free();
                    if (remaining.isEmpty()) {
                        return null;
                    }
                } catch (Throwable e) {
                    throw new IOException(e);
                }
            }
        }
    }

    public static class BatchIteratorRecordsAdaptor extends AbstractRecords {
        private final ElasticLogFileRecords elasticLogFileRecords;
        // This is the offset in Kafka layer.
        private final long startOffset;
        private final long maxOffset;
        private final int fetchSize;
        private int sizeInBytes = -1;
        private MemoryRecords memoryRecords;
        // iterator last record batch exclusive last offset.
        private long lastOffset = -1;

        public BatchIteratorRecordsAdaptor(ElasticLogFileRecords elasticLogFileRecords, long startOffset, long maxOffset, int fetchSize) {
            this.elasticLogFileRecords = elasticLogFileRecords;
            this.startOffset = startOffset;
            this.maxOffset = maxOffset;
            this.fetchSize = fetchSize;
        }


        @Override
        public int sizeInBytes() {
            try {
                ensureAllLoaded();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return sizeInBytes;
        }

        @Override
        public Iterable<? extends RecordBatch> batches() {
            if (memoryRecords == null) {
                Iterator<RecordBatch> iterator = elasticLogFileRecords.batchIterator(startOffset, maxOffset, fetchSize);
                return (Iterable<RecordBatch>) () -> iterator;
            } else {
                return memoryRecords.batches();
            }
        }

        @Override
        public AbstractIterator<? extends RecordBatch> batchIterator() {
            return elasticLogFileRecords.batchIterator(startOffset, maxOffset, fetchSize);
        }

        @Override
        public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
            return RecordsUtil.downConvert(batches(), toMagic, firstOffset, time);
        }

        @Override
        public long writeTo(TransferableChannel channel, long position, int length) throws IOException {
            // only use in RecordsSend which send Records to network. usually the size won't be large.
            ensureAllLoaded();
            return memoryRecords.writeTo(channel, position, length);
        }

        public long lastOffset() throws IOException {
            ensureAllLoaded();
            return lastOffset;
        }

        private void ensureAllLoaded() throws IOException {
            if (sizeInBytes != -1) {
                return;
            }
            Records records = elasticLogFileRecords.readAll0(startOffset, maxOffset, fetchSize);
            sizeInBytes = 0;
            CompositeByteBuf allRecordsBuf = Unpooled.compositeBuffer();
            RecordBatch lastBatch = null;
            for (RecordBatch batch : records.batches()) {
                sizeInBytes += batch.sizeInBytes();
                ByteBuffer buffer = ((DefaultRecordBatch) batch).buffer().duplicate();
                allRecordsBuf.addComponent(true, Unpooled.wrappedBuffer(buffer));
                lastBatch = batch;
            }
            if (lastBatch != null) {
                lastOffset = lastBatch.lastOffset() + 1;
            } else {
                lastOffset = startOffset;
            }
            memoryRecords = MemoryRecords.readableRecords(allRecordsBuf.nioBuffer());
        }
    }

}
