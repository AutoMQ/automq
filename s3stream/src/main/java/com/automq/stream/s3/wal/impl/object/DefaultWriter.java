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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.ByteBufSeqAlloc;
import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.metrics.stats.StorageOperationStats;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.DefaultAppendResult;
import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.RecordOffset;
import com.automq.stream.s3.wal.RecoverResult;
import com.automq.stream.s3.wal.ReservationService;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.RuntimeIOException;
import com.automq.stream.s3.wal.exception.WALFencedException;
import com.automq.stream.s3.wal.impl.DefaultRecordOffset;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Systems;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;
import com.automq.stream.utils.threads.EventLoop;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.ByteBufAlloc.S3_WAL;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.DATA_FILE_ALIGN_SIZE;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.OBJECT_PATH_OFFSET_DELIMITER;

public class DefaultWriter implements Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultWriter.class);

    private static final long DEFAULT_LOCK_WARNING_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long DEFAULT_UPLOAD_WARNING_TIMEOUT = TimeUnit.SECONDS.toNanos(5);
    private static final String OBJECT_PATH_FORMAT = "%s%d" + OBJECT_PATH_OFFSET_DELIMITER + "%d"; // {objectPrefix}/{startOffset}-{endOffset}
    private static final ByteBufSeqAlloc BYTE_BUF_ALLOC = new ByteBufSeqAlloc(S3_WAL, 8);
    private static final ExecutorService UPLOAD_EXECUTOR = Threads.newFixedThreadPoolWithMonitor(Systems.CPU_CORES, "S3_WAL_UPLOAD", true, LOGGER);
    private static final ScheduledExecutorService SCHEDULE = Threads.newSingleThreadScheduledExecutor("S3_WAL_SCHEDULE", true, LOGGER);

    protected final ObjectWALConfig config;
    protected final Time time;
    protected final ObjectStorage objectStorage;
    protected final ReservationService reservationService;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private List<WALObject> previousObjects = new ArrayList<>();
    private final ConcurrentNavigableMap<Long, WALObject> lastRecordOffset2object = new ConcurrentSkipListMap<>();
    private final String nodePrefix;
    private final String objectPrefix;

    private final AtomicLong objectDataBytes = new AtomicLong();
    private final AtomicLong bufferedDataBytes = new AtomicLong();

    protected volatile boolean closed = true;
    protected volatile boolean fenced;

    private Bulk activeBulk = null;
    private Bulk lastInActiveBulk = null;
    private long lastBulkForceUploadNanos;
    private final long batchNanos;
    private final long minBulkUploadIntervalNanos;

    private final Queue<Bulk> waitingUploadBulks = new ConcurrentLinkedQueue<>();
    private final Queue<Bulk> uploadingBulks = new ConcurrentLinkedQueue<>();

    private CompletableFuture<Void> callbackCf = CompletableFuture.completedFuture(null);
    private final EventLoop callbackExecutor = new EventLoop("S3_WAL_CALLBACK");

    private final AtomicLong nextOffset = new AtomicLong();
    private final AtomicLong flushedOffset = new AtomicLong();
    private final AtomicLong trimOffset = new AtomicLong(-1);
    private CompletableFuture<Void> lastTrimCf = CompletableFuture.completedFuture(null);

    public DefaultWriter(Time time, ObjectStorage objectStorage, ObjectWALConfig config) {
        this.time = time;
        this.objectStorage = objectStorage;
        this.reservationService = config.reservationService();
        this.config = config;
        this.nodePrefix = ObjectUtils.nodePrefix(config.clusterId(), config.nodeId(), config.type());
        this.objectPrefix = nodePrefix + config.epoch() + "/wal/";
        this.batchNanos = TimeUnit.MILLISECONDS.toNanos(config.batchInterval());
        this.minBulkUploadIntervalNanos = Math.min(TimeUnit.MILLISECONDS.toNanos(10), batchNanos);
        this.lastBulkForceUploadNanos = time.nanoseconds();
        if (!(config.openMode() == OpenMode.READ_WRITE || config.openMode() == OpenMode.FAILOVER)) {
            throw new IllegalArgumentException("The open mode must be READ_WRITE or FAILOVER, but got " + config.openMode());
        }
    }

    public void start() {
        // Verify the permission.
        reservationService.verify(config.nodeId(), config.epoch(), config.openMode() == OpenMode.FAILOVER)
            .thenAccept(result -> {
                if (!result) {
                    fenced = true;
                    WALFencedException exception = new WALFencedException("Failed to verify the permission with node id: " + config.nodeId() + ", node epoch: " + config.epoch() + ", failover flag: " + config.openMode());
                    throw new CompletionException(exception);
                }
            })
            .join();
        List<WALObject> objects = objectStorage.list(nodePrefix).thenApply(ObjectUtils::parse).join();
        List<WALObject> overlapObjects = ObjectUtils.skipOverlapObjects(objects);
        if (!overlapObjects.isEmpty()) {
            objectStorage
                .delete(overlapObjects.stream()
                    .map(o -> new ObjectStorage.ObjectPath(o.bucketId(), o.path()))
                    .collect(Collectors.toList())
                )
                .thenAccept(nil -> LOGGER.info("Delete overlap objects: {}", overlapObjects));
        }
        long largestEpoch;
        if (!objects.isEmpty() && (largestEpoch = objects.get(objects.size() - 1).epoch()) > config.epoch()) {
            LOGGER.warn("Detect newer epoch={} WAL started, exit current WAL start", largestEpoch);
            fenced = true;
            return;
        }
        objects.forEach(object -> objectDataBytes.addAndGet(object.length()));

        previousObjects.addAll(objects);

        // TODO: force align before accept new data.
        flushedOffset.set(objects.isEmpty() ? 0 : objects.get(objects.size() - 1).endOffset());
        nextOffset.set(flushedOffset.get());

        startMonitor();

        closed = false;
    }

    @Override
    public void close() {
        closed = true;
        uploadActiveBulk();
        if (lastInActiveBulk != null) {
            try {
                lastInActiveBulk.completeCf.get();
            } catch (Throwable ex) {
                LOGGER.error("Failed to flush records when close.", ex);
            }
        }

        LOGGER.info("S3WAL Writer is closed.");
    }

    List<WALObject> objectList() throws WALFencedException {
        checkStatus();
        List<WALObject> list = new ArrayList<>(lastRecordOffset2object.size() + previousObjects.size());
        list.addAll(previousObjects);
        list.addAll(lastRecordOffset2object.values());
        return list;
    }

    protected void checkStatus() throws WALFencedException {
        if (closed) {
            throw new IllegalStateException("WAL is closed.");
        }

        if (fenced) {
            throw new WALFencedException("WAL is fenced.");
        }
    }

    protected void checkWriteStatus() throws WALFencedException {
        checkStatus();
    }

    public CompletableFuture<AppendResult> append(StreamRecordBatch streamRecordBatch) throws OverCapacityException {
        try {
            return append0(streamRecordBatch);
        } catch (Throwable ex) {
            streamRecordBatch.release();
            if (ex instanceof OverCapacityException) {
                throw (OverCapacityException) ex;
            } else {
                return CompletableFuture.failedFuture(ex);
            }
        }
    }

    @Override
    public RecordOffset confirmOffset() {
        return DefaultRecordOffset.of(config.epoch(), flushedOffset.get(), 0);
    }

    public CompletableFuture<AppendResult> append0(
        StreamRecordBatch streamRecordBatch) throws OverCapacityException, WALFencedException {
        checkWriteStatus();

        if (bufferedDataBytes.get() > config.maxUnflushedBytes()) {
            throw new OverCapacityException(String.format("Max unflushed bytes exceeded %s > %s.", bufferedDataBytes.get(), config.maxUnflushedBytes()));
        }

        int dataSize = streamRecordBatch.encoded().readableBytes() + RecordHeader.RECORD_HEADER_SIZE;
        if (dataSize > DATA_FILE_ALIGN_SIZE) {
            throw new IllegalStateException("Data size exceeded " + dataSize + " > " + DATA_FILE_ALIGN_SIZE);
        }

        Record record = new Record(streamRecordBatch, new CompletableFuture<>());
        lock.writeLock().lock();
        try {
            if (activeBulk == null) {
                activeBulk = new Bulk(nextOffset.get());
            }
            if (dataSize + activeBulk.size > DATA_FILE_ALIGN_SIZE) {
                uploadActiveBulk();
                this.activeBulk = new Bulk(nextOffset.get());
            }
            bufferedDataBytes.addAndGet(dataSize);
            activeBulk.add(record);
            if (activeBulk.size > config.maxBytesInBatch()) {
                uploadActiveBulk();
            }
        } finally {
            lock.writeLock().unlock();
        }
        return record.future.whenComplete((v, throwable) -> {
            bufferedDataBytes.addAndGet(-dataSize);
            if (throwable != null) {
                LOGGER.error("Failed to append record to S3 WAL", throwable);
            }
        });
    }

    private void forceUploadBulk(Bulk forceBulk) {
        lock.writeLock().lock();
        try {
            if (forceBulk == this.activeBulk) {
                uploadActiveBulk();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void uploadActiveBulk() {
        lock.writeLock().lock();
        try {
            if (activeBulk == null) {
                return;
            }
            waitingUploadBulks.add(activeBulk);
            nextOffset.set(ObjectUtils.ceilAlignOffset(nextOffset.get() + activeBulk.size));
            lastInActiveBulk = activeBulk;
            activeBulk = null;
        } finally {
            lock.writeLock().unlock();
        }
        tryUploadBulkInWaiting();
    }

    @VisibleForTesting
    CompletableFuture<Void> flush() {
        uploadActiveBulk();
        lock.writeLock().lock();
        try {
            return lastInActiveBulk == null ? CompletableFuture.completedFuture(null) : lastInActiveBulk.completeCf;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void tryUploadBulkInWaiting() {
        UPLOAD_EXECUTOR.submit(this::uploadBulk0);
    }

    private void uploadBulk0() {
        Bulk bulk;
        lock.writeLock().lock();
        try {
            if (uploadingBulks.size() >= config.maxInflightUploadCount()) {
                return;
            }
            bulk = waitingUploadBulks.poll();
            if (bulk == null) {
                return;
            }
            uploadingBulks.add(bulk);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            long startTime = time.nanoseconds();
            List<Record> records = bulk.records;
            // Order by <streamId, offset>
            records.sort((o1, o2) -> {
                StreamRecordBatch s1 = o1.streamRecordBatch;
                StreamRecordBatch s2 = o2.streamRecordBatch;
                int rst = Long.compare(s1.getStreamId(), s2.getStreamId());
                if (rst != 0) {
                    return rst;
                }
                rst = Long.compare(s1.getBaseOffset(), s2.getBaseOffset());
                return rst;
            });

            long firstOffset = bulk.baseOffset;
            long nextOffset = firstOffset;
            long lastRecordOffset = nextOffset;
            CompositeByteBuf dataBuffer = ByteBufAlloc.compositeByteBuffer();
            for (Record record : records) {
                record.offset = nextOffset;
                lastRecordOffset = record.offset;
                ByteBuf data = record.streamRecordBatch.encoded();
                ByteBuf header = BYTE_BUF_ALLOC.byteBuffer(RECORD_HEADER_SIZE);
                header = WALUtil.generateHeader(data, header, 0, nextOffset);
                nextOffset += record.size;
                dataBuffer.addComponent(true, header);
                dataBuffer.addComponent(true, data);
            }

            // Build object buffer.
            long dataLength = dataBuffer.readableBytes();
            nextOffset = ObjectUtils.ceilAlignOffset(nextOffset);
            long endOffset = nextOffset;

            CompositeByteBuf objectBuffer = ByteBufAlloc.compositeByteBuffer();
            WALObjectHeader header = new WALObjectHeader(firstOffset, dataLength, 0, config.nodeId(), config.epoch(), trimOffset.get());
            objectBuffer.addComponent(true, header.marshal());
            objectBuffer.addComponent(true, dataBuffer);

            // Trigger upload.
            int objectLength = objectBuffer.readableBytes();

            // Enable fast retry.
            ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions().enableFastRetry(true);
            String path = String.format(OBJECT_PATH_FORMAT, objectPrefix, firstOffset, endOffset);
            FutureUtil.propagate(objectStorage.write(writeOptions, path, objectBuffer), bulk.uploadCf);
            long finalLastRecordOffset = lastRecordOffset;
            bulk.uploadCf.whenCompleteAsync((rst, ex) -> {
                if (ex != null) {
                    fenced = true;
                    LOGGER.error("S3WAL upload {} fail", path, ex);
                } else {
                    StorageOperationStats.getInstance().appendWALWriteStats.record(time.nanoseconds() - startTime);
                    lastRecordOffset2object.put(finalLastRecordOffset, new WALObject(rst.bucket(), path, config.epoch(), firstOffset, endOffset, objectLength));
                    objectDataBytes.addAndGet(objectLength);
                }
                callback();
            }, callbackExecutor);
        } catch (Throwable ex) {
            bulk.uploadCf.completeExceptionally(ex);
        }
    }

    private void callback() {
        callbackCf = callbackCf.thenComposeAsync(nil -> {
            List<Bulk> completedBulks = new ArrayList<>();
            while (true) {
                Bulk bulk = uploadingBulks.peek();
                if (bulk == null || !bulk.uploadCf.isDone()) {
                    break;
                }
                uploadingBulks.poll();
                completedBulks.add(bulk);
            }
            if (completedBulks.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            // The inflight uploading bulks count was decreased, then trigger the upload of Bulk in waitingUploadBulks
            tryUploadBulkInWaiting();
            return reservationService.verify(config.nodeId(), config.epoch(), config.openMode() == OpenMode.FAILOVER)
                .whenComplete((rst, ex) -> {
                    if (ex != null) {
                        LOGGER.error("Unexpected S3WAL lease check fail. Make the WAL fenced", ex);
                        fenced = true;
                    } else if (!rst) {
                        LOGGER.warn("The S3WAL is fenced by another nodes. Fail the following append request");
                        fenced = true;
                    }
                }).exceptionally(ex -> false)
                .thenAcceptAsync(rst -> {
                    if (fenced) {
                        Throwable ex = new WALFencedException();
                        for (Bulk bulk : completedBulks) {
                            bulk.complete(ex);
                        }
                    } else {
                        for (Bulk bulk : completedBulks) {
                            flushedOffset.set(ObjectUtils.ceilAlignOffset(bulk.endOffset()));
                            bulk.complete(null);
                        }
                    }
                }, callbackExecutor);
        }, callbackExecutor).exceptionally(ex -> {
            LOGGER.error("Unexpected S3WAL callback fail", ex);
            return null;
        });
    }

    public CompletableFuture<Void> reset() throws WALFencedException {
        long nextOffset = this.nextOffset.get();
        if (nextOffset == 0) {
            return CompletableFuture.completedFuture(null);
        }
        // The next offset is the next record's offset.
        return trim0(nextOffset - 1);
    }

    // Trim objects where the last offset is less than or equal to the given offset.
    public CompletableFuture<Void> trim(RecordOffset recordOffset) throws WALFencedException {
        long newStartOffset = ((DefaultRecordOffset) recordOffset).offset();
        return trim0(newStartOffset);
    }

    @Override
    public Iterator<RecoverResult> recover() {
        try {
            return new RecoverIterator(objectList(), objectStorage, config.readAheadObjectCount());
        } catch (WALFencedException e) {
            LOGGER.error("Recover S3 WAL failed, due to unrecoverable exception.", e);
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    throw new RuntimeIOException(e);
                }

                @Override
                public RecoverResult next() {
                    throw new RuntimeIOException(e);
                }
            };
        }
    }

    // Trim objects where the last offset is less than or equal to the given offset.
    public CompletableFuture<Void> trim0(long inclusiveTrimRecordOffset) throws WALFencedException {
        checkStatus();
        List<ObjectStorage.ObjectPath> deleteObjectList = new ArrayList<>();
        AtomicLong deletedObjectSize = new AtomicLong();
        CompletableFuture<?> persistTrimOffsetCf;
        lock.writeLock().lock();
        try {
            if (trimOffset.get() >= inclusiveTrimRecordOffset) {
                return lastTrimCf;
            }
            trimOffset.set(inclusiveTrimRecordOffset);
            // We cannot force upload an empty wal object cause of the recover workflow don't accept an empty wal object.
            // So we use a fake record to trigger the wal object upload.
            persistTrimOffsetCf = append(new StreamRecordBatch(-1L, -1L, 0, 0, Unpooled.EMPTY_BUFFER));
            lastTrimCf = persistTrimOffsetCf.thenCompose(nil -> {
                Long lastFlushedRecordOffset = lastRecordOffset2object.isEmpty() ? null : lastRecordOffset2object.lastKey();
                if (lastFlushedRecordOffset != null) {
                    lastRecordOffset2object.headMap(inclusiveTrimRecordOffset, true)
                        .forEach((lastRecordOffset, object) -> {
                            if (Objects.equals(lastRecordOffset, lastFlushedRecordOffset)) {
                                // skip the last object to prevent wal offset reset back to zero
                                // when there is no object could be used to calculate the nextOffset after restart.
                                return;
                            }
                            deleteObjectList.add(new ObjectStorage.ObjectPath(object.bucketId(), object.path()));
                            deletedObjectSize.addAndGet(object.length());
                            lastRecordOffset2object.remove(lastRecordOffset);
                        });
                }

                if (!previousObjects.isEmpty()) {
                    boolean skipTheLastObject = deleteObjectList.isEmpty();
                    List<ObjectStorage.ObjectPath> list = new ArrayList<>(previousObjects.size());
                    for (int i = 0; i < previousObjects.size() - (skipTheLastObject ? 1 : 0); i++) {
                        WALObject object = previousObjects.get(i);
                        if (object.endOffset() > inclusiveTrimRecordOffset) {
                            break;
                        }
                        list.add(new ObjectStorage.ObjectPath(object.bucketId(), object.path()));
                        deletedObjectSize.addAndGet(object.length());
                    }
                    previousObjects = new ArrayList<>(previousObjects.subList(list.size(), previousObjects.size()));
                    deleteObjectList.addAll(list);
                }
                if (deleteObjectList.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }

                return objectStorage.delete(deleteObjectList).whenComplete((v, throwable) -> {
                    objectDataBytes.addAndGet(-1 * deletedObjectSize.get());
                    // Never fail the delete task, the under layer storage will retry forever.
                    if (throwable != null) {
                        LOGGER.error("Failed to delete objects when trim S3 WAL: {}", deleteObjectList, throwable);
                    }
                    SCHEDULE.schedule(() -> {
                        // - Try to Delete the objects again after 30 seconds to avoid object leak because of underlying fast retry
                        objectStorage.delete(deleteObjectList);
                    }, 10, TimeUnit.SECONDS);
                });
            });
            return lastTrimCf;
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void startMonitor() {
        SCHEDULE.scheduleWithFixedDelay(() -> {
            try {
                long count = uploadingBulks.stream()
                    .filter(bulk -> time.nanoseconds() - bulk.startNanos > DEFAULT_UPLOAD_WARNING_TIMEOUT)
                    .count();
                if (count > 0) {
                    LOGGER.error("Found {} pending upload tasks exceed 5s.", count);
                }
            } catch (Throwable ignore) {
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    class Bulk {
        private int size;
        private final long baseOffset;
        private final List<Record> records = new ArrayList<>(1024);
        private final long startNanos;
        final CompletableFuture<ObjectStorage.WriteResult> uploadCf = new CompletableFuture<>();
        final CompletableFuture<Void> completeCf = new CompletableFuture<>();

        public Bulk(long baseOffset) {
            this.startNanos = time.nanoseconds();
            this.baseOffset = baseOffset;
            long forceUploadNanos = lastBulkForceUploadNanos + batchNanos;
            long forceUploadDelayNanos = Math.max(
                // Try batch the requests in a short time window to save the PUT API.
                minBulkUploadIntervalNanos,
                forceUploadNanos - startNanos
            );
            lastBulkForceUploadNanos = startNanos + forceUploadDelayNanos;
            SCHEDULE.schedule(() -> forceUploadBulk(this), forceUploadDelayNanos, TimeUnit.NANOSECONDS);
        }

        public void add(Record record) {
            records.add(record);
            size += record.size;
        }

        public int size() {
            return size;
        }

        public long baseOffset() {
            return baseOffset;
        }

        public long endOffset() {
            return baseOffset + size;
        }

        public void complete(Throwable ex) {
            if (ex != null) {
                records.forEach(record -> record.future.completeExceptionally(ex));
            } else {
                records.forEach(record -> record.future.complete(
                    new DefaultAppendResult(DefaultRecordOffset.of(config.epoch(), record.offset, record.size))
                ));
            }
            completeCf.complete(null);
        }
    }

    protected static class Record {
        public final StreamRecordBatch streamRecordBatch;
        public final CompletableFuture<AppendResult> future;
        public final int size;
        public long offset;

        public Record(StreamRecordBatch streamRecordBatch, CompletableFuture<AppendResult> future) {
            this.streamRecordBatch = streamRecordBatch;
            this.future = future;
            this.size = streamRecordBatch.encoded().readableBytes() + RECORD_HEADER_SIZE;
        }
    }

}
