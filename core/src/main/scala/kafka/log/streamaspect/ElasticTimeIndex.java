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

import kafka.log.streamaspect.cache.FileCache;

import org.apache.kafka.common.errors.InvalidOffsetException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.storage.internals.log.IndexSearchType;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TimestampOffset;

import com.automq.stream.api.FetchResult;
import com.automq.stream.api.RecordBatchWithContext;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ElasticTimeIndex extends TimeIndex {
    private final File file;
    private final FileCache cache;
    private final long cacheId;
    final ElasticStreamSlice stream;

    private volatile CompletableFuture<?> lastAppend = CompletableFuture.completedFuture(null);
    private boolean closed = false;

    public ElasticTimeIndex(
        File file,
        long baseOffset,
        int maxIndexSize,
        StreamSliceSupplier sliceSupplier,
        TimestampOffset initLastEntry,
        FileCache cache) throws IOException {
        super(file, baseOffset, maxIndexSize, true, true);
        this.file = file;
        this.cache = cache;
        this.cacheId = cache.newCacheId();
        this.stream = sliceSupplier.get();
        setEntries((int) (stream.nextOffset() / ENTRY_SIZE));
        if (entries() == 0) {
            lastEntry(new TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset()));
        } else {
            lastEntry(initLastEntry);
        }
    }

    @Override
    public void sanityCheck() {
        // noop implementation.
    }

    @Override
    public void truncateTo(long offset) {
        throw new UnsupportedOperationException("truncateTo() is not supported in ElasticTimeIndex");
    }

    @Override
    public boolean isFull() {
        // it's ok to call super method
        return super.isFull();
    }

    @Override
    public TimestampOffset lastEntry() {
        // it's ok to call super method
        return super.lastEntry();
    }

    @Override
    public TimestampOffset entry(int n) {
        // it's ok to call super method
        return super.entry(n);
    }

    @Override
    public TimestampOffset lookup(long targetTimestamp) {
        return maybeLock(lock, () -> {
            int slot = largestLowerBoundSlotFor(null, targetTimestamp, IndexSearchType.KEY);
            if (slot == -1)
                return new TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset());
            else
                return parseEntry(null, slot);
        });
    }

    @Override
    public void maybeAppend(long timestamp, long offset, boolean skipFullCheck) {
        lock.lock();
        try {
            if (!skipFullCheck && isFull()) {
                throw new IllegalArgumentException("Attempt to append to a full time index (size = " + entries() + ").");
            }

            // We do not throw exception when the offset equals to the offset of last entry. That means we are trying
            // to insert the same time index entry as the last entry.
            // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion
            // because that could happen in the following two scenarios:
            // 1. A log segment is closed.
            // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled.
            TimestampOffset lastEntry = lastEntry();
            if (entries() != 0 && offset < lastEntry.offset)
                throw new InvalidOffsetException("Attempt to append an offset (" + offset + ") to slot " + entries()
                    + " no larger than the last offset appended (" + lastEntry.offset + ") to " + file().getAbsolutePath());
            if (entries() != 0 && timestamp < lastEntry.timestamp)
                throw new IllegalStateException("Attempt to append a timestamp (" + timestamp + ") to slot " + entries()
                    + " no larger than the last timestamp appended (" + lastEntry.timestamp + ") to " + file().getAbsolutePath());
            if (closed)
                throw new IllegalStateException("Attempt to append to a closed time index " + file.getAbsolutePath());
            // We only append to the time index when the timestamp is greater than the last inserted timestamp.
            // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time
            // index will be empty.
            if (timestamp > lastEntry.timestamp) {
                if (log.isTraceEnabled()) {
                    log.trace("Adding index entry {} => {} to {}.", timestamp, offset, file().getAbsolutePath());
                }

                ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
                buffer.putLong(timestamp);
                int relatedOffset = relativeOffset(offset);
                buffer.putInt(relatedOffset);
                buffer.flip();
                long position = stream.nextOffset();
                lastAppend = stream.append(RawPayloadRecordBatch.of(buffer));
                cache.put(cacheId, position, Unpooled.wrappedBuffer(buffer));
                incrementEntries();
                lastEntry(new TimestampOffset(timestamp, offset));
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void reset() throws IOException {
        // AutoMQ support incremental recovery, so reset is not needed.
        throw new UnsupportedOperationException("reset() is not supported in ElasticTimeIndex");
    }

    /**
     * Note that stream index actually does not need to resize. Here we only change the maxEntries in memory to be
     * consistent with raw Apache Kafka.
     */
    @Override
    public boolean resize(int newSize) {
        lock.lock();
        try {
            int roundedNewMaxEntries = roundDownToExactMultiple(newSize, ENTRY_SIZE) / ENTRY_SIZE;

            if (maxEntries() == roundedNewMaxEntries) {
                if (log.isDebugEnabled()) {
                    log.debug("Index " + file.getAbsolutePath() + " was not resized because it already has maxEntries " + roundedNewMaxEntries);
                }
                return false;
            } else {
                setMaxEntries(roundedNewMaxEntries);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void truncate() {
        throw new UnsupportedOperationException("truncate() is not supported in ElasticTimeIndex");
    }

    /**
     * In the case of unclean shutdown, the last entry needs to be recovered from the time index.
     */
    public TimestampOffset loadLastEntry() {
        TimestampOffset lastEntry = lastEntryFromIndexFile();
        lastEntry(lastEntry);
        return lastEntry;
    }

    @Override
    public long length() {
        return (long) maxEntries() * ENTRY_SIZE;
    }

    @Override
    public void updateParentDir(File parentDir) {
        // noop implementation. AutoMQ partition doesn't have a real file.
    }

    @Override
    public void renameTo(File f) throws IOException {
        // noop implementation. AutoMQ partition doesn't have a real file.
    }

    @Override
    public void flush() {
        try {
            lastAppend.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteIfExists() throws IOException {
        close();
        return true;
    }

    @Override
    public void trimToValidSize() throws IOException {
        // it's ok to call super method
        super.trimToValidSize();
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public void closeHandler() {
        // noop implementation.
    }

    @Override
    public void forceUnmap() throws IOException {
        // noop implementation.
    }

    public void seal() {
        stream.seal();
    }

    @Override
    protected TimestampOffset parseEntry(ByteBuffer buffer, int n) {
        if (buffer != null) {
            throw new IllegalStateException("expect empty mmap");
        }
        return parseEntry(n);
    }

    TimestampOffset tryGetEntryFromCache(int n) {
        Optional<ByteBuf> rst = cache.get(cacheId, (long) n * ENTRY_SIZE, ENTRY_SIZE);
        if (rst.isPresent()) {
            ByteBuf buffer = rst.get();
            return new TimestampOffset(buffer.readLong(), baseOffset() + buffer.readInt());
        } else {
            return TimestampOffset.UNKNOWN;
        }
    }

    private TimestampOffset parseEntry(int n) {
        try {
            return parseEntry0(n);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private TimestampOffset parseEntry0(int n) throws ExecutionException, InterruptedException {
        int startOffset = n * ENTRY_SIZE;
        TimestampOffset timestampOffset = tryGetEntryFromCache(n);
        if (!TimestampOffset.UNKNOWN.equals(timestampOffset)) {
            return timestampOffset;
        }
        // cache missing, try to read from remote and put it to cache.
        // the index interval is 1MiB and the segment size is 1GB, so binary search only need 512 entries
        long endOffset = Math.min(entries() * ENTRY_SIZE, startOffset + ENTRY_SIZE * 512);
        if (endOffset > stream.confirmOffset()) {
            // the end offset is beyond the confirmed offset, so we need to wait for the last append to finish
            //  to ensure the data is available
            try {
                lastAppend.get();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        FetchResult rst = stream.fetch(startOffset, endOffset).get();
        List<RecordBatchWithContext> records = rst.recordBatchList();
        if (records.isEmpty()) {
            throw new IllegalStateException("fetch empty from stream " + stream + " at offset " + startOffset);
        }
        ByteBuf buf = Unpooled.buffer(records.size() * ENTRY_SIZE);
        records.forEach(record -> buf.writeBytes(record.rawPayload()));
        cache.put(cacheId, startOffset, buf);
        ByteBuf indexEntry = Unpooled.wrappedBuffer(records.get(0).rawPayload());
        timestampOffset = new TimestampOffset(indexEntry.readLong(), baseOffset() + indexEntry.readInt());
        rst.free();
        return timestampOffset;
    }

    private TimestampOffset lastEntryFromIndexFile() {
        if (entries() == 0) {
            return new TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset());
        } else {
            return entry(entries() - 1);
        }
    }

    void snapshot(TimestampOffset lastTimestampOffset) {
        if (lastTimestampOffset == null) {
            return;
        }
        setEntries((int) (stream.nextOffset() / ENTRY_SIZE));
        lastEntry(lastTimestampOffset);
    }

}
