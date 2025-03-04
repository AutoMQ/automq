/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect;

import kafka.log.stream.s3.telemetry.ContextUtils;
import kafka.log.stream.s3.telemetry.TelemetryConstants;

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
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

import com.automq.stream.api.FetchResult;
import com.automq.stream.api.ReadOptions;
import com.automq.stream.api.RecordBatchWithContext;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.trace.TraceUtils;
import com.automq.stream.utils.FutureUtil;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.Attributes;

import static com.automq.stream.s3.ByteBufAlloc.POOLED_MEMORY_RECORDS;
import static com.automq.stream.utils.FutureUtil.suppress;

public class ElasticLogFileRecords implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticLogFileRecords.class);

    protected final AtomicInteger size;
    // only used for recover
    protected final Iterable<RecordBatch> batches;
    private final ElasticStreamSlice streamSlice;
    // This is The base offset of the corresponding segment.
    private final long baseOffset;
    // Inflight append result.
    private volatile CompletableFuture<?> lastAppend;
    private volatile ElasticResourceStatus status;


    public ElasticLogFileRecords(ElasticStreamSlice streamSlice, long baseOffset, int size) {
        this.baseOffset = baseOffset;
        this.streamSlice = streamSlice;
        // Note that size is generally used to
        // 1) show the physical size of a segment. In these cases, size is referred to decide whether to roll a new
        // segment, or calculate the cleaned size in a cleaning task, etc. If size is not correctly recorded for any
        // reason, the worst thing will be just a bigger segment than configured.
        // 2) show whether this segment is empty, i.e., size == 0.
        // Therefore, it is fine to use the nextOffset as a backoff value.
        this.size = new AtomicInteger(size == 0 ? (int) Math.min(streamSlice.nextOffset(), Integer.MAX_VALUE / 2) : size);
        this.lastAppend = CompletableFuture.completedFuture(null);

        batches = batchesFrom(baseOffset);
        status = ElasticResourceStatus.OK;
    }

    public int sizeInBytes() {
        return size.get();
    }

    public long nextOffset() {
        return baseOffset + streamSlice.nextOffset();
    }

    public long appendedOffset() {
        return nextOffset() - baseOffset;
    }

    public CompletableFuture<Records> read(long startOffset, long maxOffset, int maxSize) {
        if (startOffset >= maxOffset) {
            return CompletableFuture.completedFuture(MemoryRecords.EMPTY);
        }
        if (ReadHint.isReadAll()) {
            ReadOptions readOptions = ReadOptions.builder().fastRead(ReadHint.isFastRead()).pooledBuf(true).build();
            FetchContext fetchContext = ContextUtils.creaetFetchContext();
            fetchContext.setReadOptions(readOptions);
            Attributes attributes = Attributes.builder()
                    .put(TelemetryConstants.START_OFFSET_NAME, startOffset)
                    .put(TelemetryConstants.END_OFFSET_NAME, maxOffset)
                    .put(TelemetryConstants.MAX_BYTES_NAME, maxSize)
                    .build();
            try {
                return TraceUtils.runWithSpanAsync(fetchContext, attributes, "ElasticLogFileRecords::read",
                        () -> readAll0(fetchContext, startOffset, maxOffset, maxSize));
            } catch (Throwable ex) {
                return CompletableFuture.failedFuture(ex);
            }
        } else {
            long endOffset = Utils.min(confirmOffset(), maxOffset);
            return CompletableFuture.completedFuture(new BatchIteratorRecordsAdaptor(this, startOffset, endOffset, maxSize));
        }
    }

    private CompletableFuture<Records> readAll0(FetchContext context, long startOffset, long maxOffset, int maxSize) {
        // calculate the relative offset in the segment, which may start from 0.
        long nextFetchOffset = startOffset - baseOffset;
        long endOffset = Utils.min(confirmOffset(), maxOffset) - baseOffset;
        if (nextFetchOffset >= endOffset) {
            return CompletableFuture.completedFuture(MemoryRecords.EMPTY);
        }
        List<FetchResult> results = new LinkedList<>();
        return fetch0(context, nextFetchOffset, endOffset, maxSize, results)
                .whenComplete((nil, e) -> {
                    if (e != null) {
                        results.forEach(FetchResult::free);
                        results.clear();
                    }
                })
                .thenApply(nil -> PooledMemoryRecords.of(baseOffset, results, context.readOptions().pooledBuf()));
    }

    /**
     * Fetch records from the {@link ElasticStreamSlice}
     *
     * @param context fetch context
     * @param startOffset start offset
     * @param endOffset end offset
     * @param maxSize max size of the fetched records
     * @param results result list to be filled
     * @return a future that completes when reaching the end offset or the max size
     */
    private CompletableFuture<Void> fetch0(FetchContext context, long startOffset, long endOffset, int maxSize, List<FetchResult> results) {
        if (startOffset >= endOffset || maxSize <= 0) {
            return CompletableFuture.completedFuture(null);
        }
        int adjustedMaxSize = Math.min(maxSize, 1024 * 1024);
        return streamSlice.fetch(context, startOffset, endOffset, adjustedMaxSize)
                .thenCompose(rst -> {
                    results.add(rst);
                    long nextFetchOffset = startOffset;
                    int readSize = 0;
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
                    if (readSize == 0) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return fetch0(context, nextFetchOffset, endOffset, maxSize - readSize, results);
                });
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
        int count = (int) (lastOffset - nextOffset());
        com.automq.stream.DefaultRecordBatch batch = new com.automq.stream.DefaultRecordBatch(count, 0, Collections.emptyMap(), records.buffer());

        AppendContext context = ContextUtils.createAppendContext();
        CompletableFuture<?> cf;
        try {
            cf = TraceUtils.runWithSpanAsync(context, Attributes.empty(), "ElasticLogFileRecords::append",
                    () -> streamSlice.append(context, batch));
        } catch (Throwable ex) {
            throw new IOException("Failed to append to stream " + streamSlice.stream().streamId(), ex);
        }

        size.getAndAdd(appendSize);
        cf.whenComplete((rst, e) -> {
            if (e instanceof IOException) {
                status = ElasticResourceStatus.FENCED;
                LOGGER.error("ElasticLogFileRecords[stream={}, baseOffset={}] fencing with ex: {}", streamSlice.stream().streamId(), baseOffset, e.getMessage());
            }
        });
        lastAppend = cf;
        return appendSize;
    }

    private long confirmOffset() {
        return baseOffset + streamSlice.confirmOffset();
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
        suppress(this::flush, LOGGER);
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
        return batchesFrom(FetchContext.DEFAULT, startOffset);
    }

    public Iterable<RecordBatch> batchesFrom(FetchContext fetchContext, final long startOffset) {
        return () -> batchIterator(fetchContext, startOffset, Long.MAX_VALUE, Integer.MAX_VALUE);
    }

    protected RecordBatchIterator<RecordBatch> batchIterator(long startOffset, long maxOffset, int fetchSize) {
        return batchIterator(FetchContext.DEFAULT, startOffset, maxOffset, fetchSize);
    }

    protected RecordBatchIterator<RecordBatch> batchIterator(FetchContext fetchContext, long startOffset, long maxOffset, int fetchSize) {
        LogInputStream<RecordBatch> inputStream = new StreamSegmentInputStream(fetchContext, this, startOffset, maxOffset, fetchSize);
        return new RecordBatchIterator<>(inputStream);
    }

    public static class PooledMemoryRecords extends AbstractRecords implements PooledResource {
        private final long logBaseOffset;
        private final ByteBuf pack;
        private final MemoryRecords memoryRecords;
        private final long lastOffset;
        private final boolean pooled;
        private boolean freed;

        private PooledMemoryRecords(long logBaseOffset, List<FetchResult> fetchResults, boolean pooled) {
            this.logBaseOffset = logBaseOffset;
            this.pooled = pooled;
            long lastOffset = 0;
            int size = 0;
            for (FetchResult fetchResult : fetchResults) {
                for (RecordBatchWithContext recordBatchWithContext : fetchResult.recordBatchList()) {
                    size += recordBatchWithContext.rawPayload().remaining();
                    lastOffset = recordBatchWithContext.lastOffset();
                }
            }
            // TODO: create a new ByteBufMemoryRecords data struct to avoid copy
            if (pooled) {
                this.pack = ByteBufAlloc.byteBuffer(size, POOLED_MEMORY_RECORDS);
            } else {
                this.pack = Unpooled.buffer(size);
            }
            for (FetchResult fetchResult : fetchResults) {
                for (RecordBatchWithContext recordBatchWithContext : fetchResult.recordBatchList()) {
                    pack.writeBytes(recordBatchWithContext.rawPayload());
                }
            }
            fetchResults.forEach(FetchResult::free);
            fetchResults.clear();
            this.memoryRecords = MemoryRecords.readableRecords(pack.nioBuffer());
            this.lastOffset = logBaseOffset + lastOffset;
        }

        public static PooledMemoryRecords of(long logBaseOffset, List<FetchResult> fetchResults, boolean pooled) {
            return new PooledMemoryRecords(logBaseOffset, fetchResults, pooled);
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
        public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
            return memoryRecords.writeTo(channel, position, length);
        }

        @Override
        public void release() {
            if (!freed) {
                pack.release();
                freed = true;
            } else {
                LOGGER.warn("PooledMemoryRecords[{}] has been freed", this, new RuntimeException());
            }
        }

        public long lastOffset() {
            return lastOffset;
        }
    }

    static class StreamSegmentInputStream implements LogInputStream<RecordBatch> {
        private static final int FETCH_BATCH_SIZE = 64 * 1024;
        private final ElasticLogFileRecords elasticLogFileRecords;
        private final Queue<RecordBatch> remaining = new LinkedList<>();
        private final int maxSize;
        private final long endOffset;
        private final FetchContext fetchContext;
        private long nextFetchOffset;
        private int readSize;

        public StreamSegmentInputStream(FetchContext fetchContext, ElasticLogFileRecords elasticLogFileRecords, long startOffset, long maxOffset, int maxSize) {
            this.fetchContext = fetchContext;
            this.elasticLogFileRecords = elasticLogFileRecords;
            this.maxSize = maxSize;
            this.nextFetchOffset = startOffset - elasticLogFileRecords.baseOffset;
            this.endOffset = Utils.min(elasticLogFileRecords.confirmOffset(), maxOffset) - elasticLogFileRecords.baseOffset;
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
                    FetchResult rst = elasticLogFileRecords.streamSlice.fetch(fetchContext, nextFetchOffset, endOffset, Math.min(maxSize - readSize, FETCH_BATCH_SIZE)).get();
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
        public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
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
            Records records;
            try {
                records = elasticLogFileRecords.readAll0(FetchContext.DEFAULT, startOffset, maxOffset, fetchSize).get();
            } catch (Throwable t) {
                throw new IOException(FutureUtil.cause(t));
            }
            sizeInBytes = records.sizeInBytes();
            if (records instanceof PooledMemoryRecords) {
                memoryRecords = MemoryRecords.readableRecords(((PooledMemoryRecords) records).pack.nioBuffer());
                lastOffset = ((PooledMemoryRecords) records).lastOffset();
            } else if (records instanceof MemoryRecords) {
                memoryRecords = (MemoryRecords) records;
                lastOffset = startOffset;
                for (RecordBatch batch: records.batches()) {
                    lastOffset = batch.lastOffset() + 1;
                }
            } else {
                throw new IllegalArgumentException("unknown records type " + records.getClass());
            }
        }
    }

}
