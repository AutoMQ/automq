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

package kafka.log.streamaspect;

import kafka.cluster.PartitionSnapshot;
import kafka.log.streamaspect.cache.FileCache;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.CompletedTxn;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegmentOffsetOverflowException;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.RollParams;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TimestampOffset;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;

import com.automq.stream.api.ReadOptions;
import com.automq.stream.s3.context.FetchContext;

import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class ElasticLogSegment extends LogSegment implements Comparable<ElasticLogSegment> {
    public static FileCache timeCache = FileCache.NOOP;
    public static FileCache txnCache = FileCache.NOOP;

    private final ElasticStreamSegmentMeta meta;
    private final long baseOffset;
    private final ElasticLogFileRecords log;
    private final ElasticTimeIndex timeIndex;
    private final ElasticTransactionIndex txnIndex;

    private final ElasticLogSegmentEventListener logListener;

    private final String logIdent;

    private boolean forceRoll;

    public ElasticLogSegment(
        File dir,
        ElasticStreamSegmentMeta meta,
        ElasticStreamSliceManager sm,
        LogConfig logConfig,
        Time time,
        ElasticLogSegmentEventListener segmentEventListener,
        String logIdent
    ) throws IOException {
        super(null, null, null, null, meta.baseOffset(), logConfig.indexInterval, logConfig.segmentJitterMs, time);
        this.meta = meta;
        baseOffset = meta.baseOffset();
        String suffix = meta.streamSuffix();

        log = new ElasticLogFileRecords(sm.loadOrCreateSlice("log" + suffix, meta.log()), baseOffset, meta.logSize());

        TimestampOffset lastTimeIndexEntry = meta.timeIndexLastEntry().toTimestampOffset();
        timeIndex = new ElasticTimeIndex(
            LogFileUtils.timeIndexFile(dir, baseOffset, suffix),
            baseOffset,
            logConfig.maxIndexSize,
            new DefaultStreamSliceSupplier(sm, "tim" + suffix, meta.time()),
            lastTimeIndexEntry,
            timeCache
        );

        txnIndex = new ElasticTransactionIndex(
            baseOffset,
            LogFileUtils.transactionIndexFile(dir, baseOffset, suffix),
            new DefaultStreamSliceSupplier(sm, "txn" + suffix, meta.txn()),
            txnCache
        );
        this.logListener = segmentEventListener;
        this.logIdent = logIdent;

        if (meta.firstBatchTimestamp() != 0) {
            // optimize loadFirstBatchTimestamp
            rollingBasedTimestamp = OptionalLong.of(meta.firstBatchTimestamp());
        }
    }

    @Override
    public OffsetIndex offsetIndex() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public File offsetIndexFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimeIndex timeIndex() throws IOException {
        return timeIndex;
    }

    @Override
    public File timeIndexFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long baseOffset() {
        return meta.baseOffset();
    }

    @Override
    public FileRecords log() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionIndex txnIndex() {
        return txnIndex;
    }

    @Override
    public boolean shouldRoll(RollParams rollParams) {
        if (forceRoll) {
            return true;
        }
        boolean reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs;
        int size = size();
        return size > rollParams.maxSegmentBytes - rollParams.messagesSize ||
            (size > 0 && reachedRollMs) || timeIndex.isFull() || !canConvertToRelativeOffset(rollParams.maxOffsetInMessages);
    }

    @Override
    public void resizeIndexes(int size) {
        // noop implementation.
    }

    @Override
    public void sanityCheck(boolean timeIndexFileNewlyCreated) {
        // do nothing since it will not be called.
    }

    @Override
    public TimestampOffset readMaxTimestampAndOffsetSoFar() throws IOException {
        // it's ok to call super method
        return super.readMaxTimestampAndOffsetSoFar();
    }

    @Override
    public long maxTimestampSoFar() throws IOException {
        // it's ok to call super method
        return super.maxTimestampSoFar();
    }

    @Override
    public int size() {
        return log.sizeInBytes();
    }

    /**
     * Checks that the argument offset can be represented as an integer offset relative to the baseOffset.
     * This method is similar in purpose to {@see org.apache.kafka.storage.internals.log.LogSegment#canConvertToRelativeOffset}.
     * <p>
     * The implementation is inspired by {@see org.apache.kafka.storage.internals.log.AbstractIndex#canAppendOffset},
     * but uses {@code < Integer.MAX_VALUE} instead of {@code <= Integer.MAX_VALUE} to address an offset overflow issue.
     *
     * @param offset The offset to check.
     * @return true if the offset can be converted, false otherwise.
     * @see <a href="https://github.com/AutoMQ/automq/issues/2718">Issue #2718</a>
     */
    private boolean canConvertToRelativeOffset(long offset) {
        long relativeOffset = offset - baseOffset;
        // Note: The check is `relativeOffset < Integer.MAX_VALUE` instead of `<=` to avoid overflow.
        // See https://github.com/AutoMQ/automq/issues/2718 for details.
        return relativeOffset >= 0 && relativeOffset < Integer.MAX_VALUE;
    }
    private void ensureOffsetInRange(long offset) throws IOException {
        if (!canConvertToRelativeOffset(offset))
            throw new LogSegmentOffsetOverflowException(this, offset);
    }

    @Override
    public void append(
        long largestOffset,
        long largestTimestampMs,
        long offsetOfMaxTimestamp,
        MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > 0) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Inserting {} bytes at end offset {} at position {} with largest timestamp {} at offset {}",
                    records.sizeInBytes(), largestOffset, log.sizeInBytes(), largestTimestampMs, offsetOfMaxTimestamp);
            }
            int physicalPosition = log.sizeInBytes();
            if (physicalPosition == 0) {
                rollingBasedTimestamp = OptionalLong.of(largestTimestampMs);
                meta.firstBatchTimestamp(largestTimestampMs);
            }

            ensureOffsetInRange(largestOffset);

            // append the messages
            long appendedBytes = log.append(records, largestOffset + 1);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Appended {} to {} at end offset {}", appendedBytes, log, largestOffset);
            }
            // Update the in memory max timestamp and corresponding offset.
            if (largestTimestampMs > maxTimestampSoFar()) {
                maxTimestampAndOffsetSoFar = new TimestampOffset(largestTimestampMs, offsetOfMaxTimestamp);
            }
            // append an entry to the index (if needed)
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                timeIndex.maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar());
                bytesSinceLastIndexEntry = 0;
            }
            bytesSinceLastIndexEntry += records.sizeInBytes();
        }
        meta.lastModifiedTimestamp(System.currentTimeMillis());
    }

    @Override
    public int appendFromFile(FileRecords records, int start) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTxnIndex(CompletedTxn completedTxn, long lastStableOffset) {
        if (completedTxn.isAborted) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Writing aborted transaction {} to transaction index, last stable offset is {}", completedTxn, lastStableOffset);
            }
            txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset));
        }
    }

    @Override
    public FileRecords.LogOffsetPosition translateOffset(long offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FetchDataInfo read(long startOffset, int maxSize, Optional<Long> maxPositionOpt,
        boolean minOneMessage) throws IOException {
        try {
            return readAsync(startOffset, maxSize, maxPositionOpt, Long.MAX_VALUE, minOneMessage).get();
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }

    @Override
    public CompletableFuture<FetchDataInfo> readAsync(long startOffset, int maxSize, Optional<Long> maxPositionOpt, long maxOffset,
        boolean minOneMessage) {
        if (maxSize < 0)
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid max size " + maxSize + " for log read from segment " + log));
        // Note that relativePositionInSegment here is a fake value. There are no 'position' in elastic streams.

        // if the start offset is less than base offset, use base offset. This usually happens when the prev segment is generated
        // by compaction and its last offset is less than the base offset of the current segment.
        startOffset = Utils.max(startOffset, baseOffset);

        // if the start offset is already off the end of the log, return null
        if (startOffset >= log.nextOffset()) {
            return CompletableFuture.completedFuture(null);
        }
        LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, (int) (startOffset - this.baseOffset));

        int adjustedMaxSize = minOneMessage ? Math.max(maxSize, 1) : maxSize;

        // return a log segment but with zero size in the case below
        if (adjustedMaxSize == 0) {
            return CompletableFuture.completedFuture(new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY));
        }

        return log.read(startOffset, maxOffset, adjustedMaxSize)
            .thenApply(records -> new FetchDataInfo(offsetMetadata, records));
    }

    @Override
    public OptionalLong fetchUpperBoundOffset(OffsetPosition startOffsetPosition, int fetchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int recover(ProducerStateManager producerStateManager,
        Optional<LeaderEpochFileCache> leaderEpochCache) throws IOException {
        logListener.onEvent(baseOffset, ElasticLogSegmentEvent.SEGMENT_UPDATE);
        return recover0(producerStateManager, leaderEpochCache);
    }

    @Override
    public boolean hasOverflow() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TxnIndexSearchResult collectAbortedTxns(long fetchOffset, long upperBoundOffset) {
        return txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset);
    }

    @Override
    public String toString() {
        return "ElasticLogSegment{meta=" + meta + '}';
    }

    @Override
    public int truncateTo(long offset) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readNextOffset() {
        return log.nextOffset();
    }

    @Override
    public void flush() throws IOException {
        try {
            // lambdas cannot declare a more specific exception type, so we use an anonymous inner class
            LOG_FLUSH_TIMER.time((Callable<Void>) () -> {
                log.flush();
                timeIndex.flush();
                txnIndex.flush();
                return null;
            });
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
            else if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new IllegalStateException("Unexpected exception thrown: " + e, e);
        }
    }

    @Override
    public void changeFileSuffixes(String oldSuffix, String newSuffix) {
        // noop implementation.
    }

    @Override
    public boolean hasSuffix(String suffix) {
        // No need to support suffix in ElasticLogSegment. Return false.
        return false;
    }

    @Override
    public void onBecomeInactiveSegment() throws IOException {
        timeIndex.maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar(), true);
        log.seal();
        meta.log(log.streamSegment().sliceRange());
        timeIndex.trimToValidSize();
        timeIndex.seal();
        meta.time(timeIndex.stream.sliceRange());
        meta.timeIndexLastEntry(timeIndex.lastEntry());
        txnIndex.seal();
        meta.txn(txnIndex.stream.sliceRange());
    }

    @Override
    protected void loadFirstBatchTimestamp() {
        if (rollingBasedTimestamp.isEmpty()) {
            if (log.sizeInBytes() > 0) {
                try {
                    Records records = log.read(baseOffset, baseOffset + 1, 1).get();
                    Iterator<? extends RecordBatch> iter = records.batches().iterator();
                    if (iter.hasNext())
                        rollingBasedTimestamp = OptionalLong.of(iter.next().maxTimestamp());
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public long timeWaitedForRoll(long now, long messageTimestamp) {
        // it's ok to call super method
        return super.timeWaitedForRoll(now, messageTimestamp);
    }

    @Override
    public Optional<FileRecords.TimestampAndOffset> findOffsetByTimestamp(long timestampMs,
        long startingOffset) throws IOException {
        // TODO: async the find to avoid blocking the thread
        // Get the index entry with a timestamp less than or equal to the target timestamp
        TimestampOffset timestampOffset = timeIndex.lookup(timestampMs);
        // Search the timestamp
        return Optional.ofNullable(log.searchForTimestamp(timestampMs, timestampOffset.offset));
    }

    @Override
    public void close() throws IOException {
        meta(); // fill the metadata
        if (maxTimestampAndOffsetSoFar != TimestampOffset.UNKNOWN)
            Utils.swallow(LOGGER, Level.WARN, "maybeAppend", () -> timeIndex.maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar(), true));
        Utils.closeQuietly(timeIndex, "timeIndex", LOGGER);
        Utils.closeQuietly(log, "log", LOGGER);
        Utils.closeQuietly(txnIndex, "txnIndex", LOGGER);
    }

    @Override
    protected void closeHandlers() {
        Utils.swallow(LOGGER, Level.WARN, "timeIndex", () -> timeIndex.closeHandler());
        Utils.swallow(LOGGER, Level.WARN, "log", () -> log.closeHandlers());
        Utils.closeQuietly(txnIndex, "txnIndex", LOGGER);
    }

    @Override
    public void deleteIfExists() throws IOException {
        logListener.onEvent(baseOffset, ElasticLogSegmentEvent.SEGMENT_DELETE);
    }

    @Override
    public boolean deleted() {
        // No need to support this method. If this instance can still be accessed, it is not deleted.
        return false;
    }

    @Override
    public long lastModified() {
        return meta.lastModifiedTimestamp();
    }

    public ElasticStreamSegmentMeta meta() {
        meta.log(log.streamSegment().sliceRange());
        meta.logSize(log.sizeInBytes());
        meta.time(timeIndex.stream.sliceRange());
        meta.txn(txnIndex.stream.sliceRange());
        meta.timeIndexLastEntry(timeIndex.lastEntry());
        meta.streamSuffix(meta.streamSuffix());
        return meta;
    }

    @Override
    public OptionalLong largestRecordTimestamp() throws IOException {
        // it's ok to call super method
        return super.largestRecordTimestamp();
    }

    @Override
    public long largestTimestamp() throws IOException {
        // it's ok to call super method
        return super.largestTimestamp();
    }

    @Override
    public void setLastModified(long ms) {
        meta.lastModifiedTimestamp(ms);
    }

    public long appendedOffset() {
        return log.appendedOffset();
    }

    public CompletableFuture<Void> asyncLogFlush() {
        return log.asyncFlush();
    }

    public void forceRoll() {
        this.forceRoll = true;
    }

    private void updateProducerState(ProducerStateManager producerStateManager, RecordBatch batch,
        OptionalLong txnIndexCheckpoint) {
        if (batch.hasProducerId()) {
            long producerId = batch.producerId();
            ProducerAppendInfo appendInfo = producerStateManager.prepareUpdate(producerId, AppendOrigin.REPLICATION);
            Optional<CompletedTxn> maybeCompletedTxn = appendInfo.append(batch, Optional.empty());
            producerStateManager.update(appendInfo);
            maybeCompletedTxn.ifPresent(completedTxn -> {
                long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                if (txnIndexCheckpoint.isEmpty() || txnIndexCheckpoint.getAsLong() < completedTxn.lastOffset) {
                    updateTxnIndex(completedTxn, lastStableOffset);
                }
                producerStateManager.completeTxn(completedTxn);
            });
        }
        producerStateManager.updateMapEndOffset(batch.lastOffset() + 1);
    }

    private int recover0(ProducerStateManager producerStateManager,
        Optional<LeaderEpochFileCache> leaderEpochCache) throws IOException {
        int validBytes = 0;
        int lastIndexEntry = 0;
        maxTimestampAndOffsetSoFar = TimestampOffset.UNKNOWN;
        // exclusive recover from the checkpoint cause the offset the offset of record in batch
        long timeIndexCheckpoint = timeIndex.loadLastEntry().offset;
        // exclusive recover from the checkpoint
        OptionalLong txnIndexCheckpoint = txnIndex.loadLastOffset();
        FetchContext fetchContext = new FetchContext();
        ReadOptions readOptions = ReadOptions.builder().prioritizedRead(true).build();
        fetchContext.setReadOptions(readOptions);
        try {
            long recoverPoint = Math.max(producerStateManager.mapEndOffset(), baseOffset);
            LOGGER.info("{} [UNCLEAN_SHUTDOWN] recover range [{}, {})", logIdent, recoverPoint, log.nextOffset());
            for (RecordBatch batch : log.batchesFrom(fetchContext, recoverPoint)) {
                batch.ensureValid();
                // The max timestamp is exposed at the batch level, so no need to iterate the records
                if (batch.maxTimestamp() > maxTimestampSoFar()) {
                    maxTimestampAndOffsetSoFar = new TimestampOffset(batch.maxTimestamp(), batch.lastOffset());
                }

                // Build offset index
                if (validBytes - lastIndexEntry > indexIntervalBytes && batch.baseOffset() > timeIndexCheckpoint) {
                    timeIndex.maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar());
                    lastIndexEntry = validBytes;
                }
                validBytes += batch.sizeInBytes();

                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    leaderEpochCache.ifPresent(cache -> {
                        if (batch.partitionLeaderEpoch() >= 0) {
                            if (cache.latestEpoch().isEmpty() || batch.partitionLeaderEpoch() > cache.latestEpoch().getAsInt()) {
                                cache.assign(batch.partitionLeaderEpoch(), batch.baseOffset());
                            }
                        }
                    });
                    updateProducerState(producerStateManager, batch, txnIndexCheckpoint);
                }
            }
        } catch (CorruptRecordException | InvalidRecordException e) {
            LOGGER.warn("{} Found invalid messages in log segment at byte offset {}", logIdent, validBytes, e);
        }
        // won't have record corrupted cause truncate
        // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
        timeIndex.maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar(), true);
        return 0;
    }

    @Override
    public int compareTo(ElasticLogSegment o) {
        return Long.compare(baseOffset, o.baseOffset);
    }

    public Iterable<? extends RecordBatch> batches() {
        return log.batchesFrom(baseOffset);
    }

    public Iterable<? extends org.apache.kafka.common.record.Record> records() {
        return (Iterable<Record>) () -> {
            Iterator<? extends RecordBatch> recordBatchIt = batches().iterator();
            AtomicReference<Iterator<Record>> recordItRef = new AtomicReference<>(recordBatchIt.hasNext() ? recordBatchIt.next().iterator() : null);
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    for (; ; ) {
                        Iterator<Record> recordIt = recordItRef.get();
                        if (recordIt == null) {
                            if (!recordBatchIt.hasNext()) {
                                return false;
                            }
                            recordItRef.set(recordBatchIt.next().iterator());
                            continue;
                        }
                        if (!recordIt.hasNext()) {
                            recordItRef.set(null);
                            continue;
                        }
                        return true;
                    }
                }

                @Override
                public Record next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    Iterator<Record> recordIt = recordItRef.get();
                    return recordIt.next();
                }
            };
        };
    }

    void snapshot(PartitionSnapshot snapshot) {
        log.size(snapshot.logEndOffset().relativePositionInSegment);
        timeIndex.snapshot(snapshot.lastTimestampOffset());
    }
}
