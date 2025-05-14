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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.Constants;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.common.RecordHeader;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALFencedException;
import com.automq.stream.s3.wal.metrics.ObjectWALMetricsManager;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

public class RecordAccumulator implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private static final long DEFAULT_LOCK_WARNING_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long DEFAULT_UPLOAD_WARNING_TIMEOUT = TimeUnit.SECONDS.toNanos(5);
    private static final String OBJECT_PATH_OFFSET_DELIMITER = "-";
    private static final String OBJECT_PATH_FORMAT = "%s%d" + OBJECT_PATH_OFFSET_DELIMITER + "%d"; // {objectPrefix}/{startOffset}-{endOffset}
    protected final ObjectWALConfig config;
    protected final Time time;
    protected final ObjectStorage objectStorage;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentNavigableMap<Long /* inclusive first offset */, UploadTask> uploadMap = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Pair<Long /* epoch */, Long /* exclusive end offset */>, WALObject> previousObjectMap = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Long /* exclusive end offset */, WALObject> objectMap = new ConcurrentSkipListMap<>();
    private final String nodePrefix;
    private final String objectPrefix;
    private final ScheduledExecutorService executorService;
    private final ScheduledExecutorService utilityService;
    private final ExecutorService callbackService;
    private final ConcurrentMap<CompletableFuture<Void>, Long> pendingFutureMap = new ConcurrentHashMap<>();
    private final AtomicLong objectDataBytes = new AtomicLong();
    private final AtomicLong bufferedDataBytes = new AtomicLong();
    protected volatile boolean closed = true;
    protected volatile boolean fenced;
    private final ConcurrentLinkedDeque<Record> bufferQueue = new ConcurrentLinkedDeque<>();
    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private final AtomicLong nextOffset = new AtomicLong();
    private final AtomicLong flushedOffset = new AtomicLong();
    private final AtomicLong trimOffset = new AtomicLong(-1);

    public RecordAccumulator(Time time, ObjectStorage objectStorage,
        ObjectWALConfig config) {
        this.time = time;
        this.objectStorage = objectStorage;
        this.config = config;
        this.nodePrefix = DigestUtils.md5Hex(String.valueOf(config.nodeId())).toUpperCase(Locale.ROOT) + "/" + Constants.DEFAULT_NAMESPACE + config.clusterId() + "/" + config.nodeId() + "/";
        this.objectPrefix = nodePrefix + config.epoch() + "/wal/";
        this.executorService = Threads.newSingleThreadScheduledExecutor("s3-wal-schedule", true, log);
        this.utilityService = Threads.newSingleThreadScheduledExecutor("s3-wal-utility", true, log);
        this.callbackService = Threads.newFixedThreadPoolWithMonitor(4, "s3-wal-callback", false, log);

        ObjectWALMetricsManager.setInflightUploadCountSupplier(() -> (long) pendingFutureMap.size());
        ObjectWALMetricsManager.setBufferedDataInBytesSupplier(bufferedDataBytes::get);
        ObjectWALMetricsManager.setObjectDataInBytesSupplier(objectDataBytes::get);
    }

    public void start() {
        objectStorage.list(nodePrefix)
            .thenAccept(objectList -> objectList.forEach(object -> {
                String path = object.key();
                String[] parts = path.split("/");
                try {
                    WALObject walObject;

                    long epoch = Long.parseLong(parts[parts.length - 3]);
                    // Skip the object if it belongs to a later epoch.
                    if (epoch > config.epoch()) {
                        return;
                    }

                    long length = object.size();

                    String rawOffset = parts[parts.length - 1];
                    if (rawOffset.contains(OBJECT_PATH_OFFSET_DELIMITER)) {
                        // new format: {startOffset}-{endOffset}
                        long startOffset = Long.parseLong(rawOffset.substring(0, rawOffset.indexOf(OBJECT_PATH_OFFSET_DELIMITER)));
                        long endOffset = Long.parseLong(rawOffset.substring(rawOffset.indexOf(OBJECT_PATH_OFFSET_DELIMITER) + 1));
                        walObject = new WALObject(object.bucketId(), path, startOffset, endOffset, length);
                    } else {
                        // old format: {startOffset}
                        long startOffset = Long.parseLong(rawOffset);
                        walObject = new WALObject(object.bucketId(), path, startOffset, length);
                    }

                    if (epoch != config.epoch()) {
                        previousObjectMap.put(Pair.of(epoch, walObject.endOffset()), walObject);
                    } else {
                        objectMap.put(walObject.endOffset(), walObject);
                    }
                    objectDataBytes.addAndGet(length);
                } catch (NumberFormatException e) {
                    // Ignore invalid path
                    log.warn("Found invalid wal object: {}", path);
                }
            }))
            .join();

        flushedOffset.set(objectMap.isEmpty() ? 0 : objectMap.lastKey());
        nextOffset.set(flushedOffset.get());

        // Trigger upload periodically.
        executorService.scheduleWithFixedDelay(() -> {
            long startTime = time.nanoseconds();
            if (fenced
                || bufferQueue.isEmpty()
                || System.currentTimeMillis() - lastUploadTimestamp < config.batchInterval()) {
                return;
            }

            lock.writeLock().lock();
            try {
                if (time.nanoseconds() - startTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                    log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: scheduled_upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTime));
                }

                if (System.currentTimeMillis() - lastUploadTimestamp >= config.batchInterval()) {
                    unsafeUpload(false);
                }
            } catch (Throwable ignore) {
            } finally {
                lock.writeLock().unlock();
            }
        }, config.batchInterval(), config.batchInterval(), TimeUnit.MILLISECONDS);

        utilityService.scheduleWithFixedDelay(() -> {
            try {
                long count = pendingFutureMap.values()
                    .stream()
                    .filter(uploadTime -> time.nanoseconds() - uploadTime > DEFAULT_UPLOAD_WARNING_TIMEOUT)
                    .count();
                if (count > 0) {
                    log.error("Found {} pending upload tasks exceed 5s.", count);
                }
            } catch (Throwable ignore) {
            }
        }, 1, 1, TimeUnit.SECONDS);

        closed = false;
    }

    @Override
    public void close() {
        closed = true;

        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.error("Main executor {} did not terminate in time", executorService);
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Failed to shutdown main executor service.", e);
                executorService.shutdownNow();
            }
        }

        lock.writeLock().lock();
        try {
            unsafeUpload(true);
        } catch (Throwable throwable) {
            log.error("Failed to flush records when close.", throwable);
        } finally {
            lock.writeLock().unlock();
        }

        // Wait for all upload tasks to complete.
        if (!pendingFutureMap.isEmpty()) {
            log.info("Wait for {} pending upload tasks to complete.", pendingFutureMap.size());
            try {
                CompletableFuture.allOf(pendingFutureMap.keySet().toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error("Failed to wait for pending upload tasks to complete.", e);
            }
        }

        if (utilityService != null && !utilityService.isShutdown()) {
            utilityService.shutdown();
            try {
                if (!utilityService.awaitTermination(1, TimeUnit.SECONDS)) {
                    log.error("Monitor executor {} did not terminate in time", executorService);
                    utilityService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Failed to shutdown monitor executor service.", e);
                utilityService.shutdownNow();
            }
        }

        if (callbackService != null && !callbackService.isShutdown()) {
            callbackService.shutdown();
            try {
                if (!callbackService.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.error("Callback executor {} did not terminate in time", executorService);
                    callbackService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Failed to shutdown callback executor service.", e);
                callbackService.shutdownNow();
            }
        }

        log.info("S3 WAL record accumulator is closed.");
    }

    public long nextOffset() {
        return nextOffset.get();
    }

    public long flushedOffset() {
        return flushedOffset.get();
    }

    public List<WALObject> objectList() throws WALFencedException {
        checkStatus();

        List<WALObject> list = new ArrayList<>(objectMap.size() + previousObjectMap.size());
        list.addAll(previousObjectMap.values());
        list.addAll(objectMap.values());
        return list;
    }

    // Visible for testing
    String objectPrefix() {
        return objectPrefix;
    }

    // Visible for testing
    ScheduledExecutorService executorService() {
        return executorService;
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
        if (config.failover()) {
            throw new IllegalStateException("WAL is in failover mode.");
        }

        checkStatus();
    }

    private boolean shouldUpload() {
        Record firstRecord = bufferQueue.peekFirst();
        if (firstRecord == null || uploadMap.size() >= config.maxInflightUploadCount()) {
            return false;
        }

        return System.currentTimeMillis() - lastUploadTimestamp >= config.batchInterval()
               || nextOffset.get() - firstRecord.offset > config.maxBytesInBatch();
    }

    public long append(long recordSize, Function<Long, ByteBuf> recordSupplier,
        CompletableFuture<AppendResult.CallbackResult> future) throws OverCapacityException, WALFencedException {
        long startTime = time.nanoseconds();
        checkWriteStatus();

        // Check if there is too much data in the S3 WAL.
        if (nextOffset.get() - flushedOffset.get() > config.maxUnflushedBytes()) {
            throw new OverCapacityException("Too many unflushed bytes.", true);
        }

        if (objectMap.size() + config.maxInflightUploadCount() >= 3000) {
            throw new OverCapacityException("Too many WAL objects.", false);
        }

        if (shouldUpload() && lock.writeLock().tryLock()) {
            try {
                if (time.nanoseconds() - startTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                    log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: append_upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTime));
                }

                if (shouldUpload()) {
                    unsafeUpload(false);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        long acquireAppendLockTime = time.nanoseconds();
        lock.readLock().lock();
        try {
            if (time.nanoseconds() - acquireAppendLockTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: append", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - acquireAppendLockTime));
            }

            long offset = nextOffset.getAndAdd(recordSize);
            future.whenComplete((v, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to append record to S3 WAL: {}", offset, throwable);
                } else {
                    ObjectWALMetricsManager.recordOperationDataSize(recordSize, "append");
                }
                ObjectWALMetricsManager.recordOperationLatency(time.nanoseconds() - acquireAppendLockTime, "append", throwable == null);
            });

            bufferQueue.offer(new Record(offset, recordSupplier.apply(offset), future));
            bufferedDataBytes.addAndGet(recordSize);

            return offset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public CompletableFuture<Void> reset() throws WALFencedException {
        checkStatus();

        if (objectMap.isEmpty() && previousObjectMap.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        long startTime = time.nanoseconds();
        AtomicLong deletedObjectSize = new AtomicLong();
        List<ObjectStorage.ObjectPath> deleteObjectList = new ArrayList<>();

        previousObjectMap.forEach((k, v) -> {
            deleteObjectList.add(new ObjectStorage.ObjectPath(v.bucketId(), v.path()));
            deletedObjectSize.addAndGet(v.length());
            previousObjectMap.remove(k);
        });
        objectMap.forEach((k, v) -> {
            deleteObjectList.add(new ObjectStorage.ObjectPath(v.bucketId(), v.path()));
            deletedObjectSize.addAndGet(v.length());
            objectMap.remove(k);
        });

        return objectStorage.delete(deleteObjectList)
            .whenComplete((v, throwable) -> {
                ObjectWALMetricsManager.recordOperationLatency(time.nanoseconds() - startTime, "reset", throwable == null);
                objectDataBytes.addAndGet(-1 * deletedObjectSize.get());

                // Never fail the delete task, the under layer storage will retry forever.
                if (throwable != null) {
                    log.error("Failed to delete objects when trim S3 WAL: {}", deleteObjectList, throwable);
                }
            });
    }

    // Trim objects where the last offset is less than or equal to the given offset.
    public CompletableFuture<Void> trim(long offset) throws WALFencedException {
        checkStatus();

        if (objectMap.isEmpty() || offset < objectMap.firstKey() || offset > flushedOffset.get()) {
            return CompletableFuture.completedFuture(null);
        }

        trimOffset.set(offset);
        long startTime = time.nanoseconds();

        List<ObjectStorage.ObjectPath> deleteObjectList = new ArrayList<>();
        AtomicLong deletedObjectSize = new AtomicLong();

        objectMap.headMap(offset, true)
            .forEach((k, v) -> {
                deleteObjectList.add(new ObjectStorage.ObjectPath(v.bucketId(), v.path()));
                deletedObjectSize.addAndGet(v.length());
                objectMap.remove(k);
            });

        if (deleteObjectList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return objectStorage.delete(deleteObjectList)
            .whenComplete((v, throwable) -> {
                ObjectWALMetricsManager.recordOperationLatency(time.nanoseconds() - startTime, "trim", throwable == null);
                objectDataBytes.addAndGet(-1 * deletedObjectSize.get());

                // Never fail the delete task, the under layer storage will retry forever.
                if (throwable != null) {
                    log.error("Failed to delete objects when trim S3 WAL: {}", deleteObjectList, throwable);
                }

                utilityService.schedule(() -> {
                    // Try to Delete the objects again after 30 seconds to avoid object leak because of underlying fast retry
                    objectStorage.delete(deleteObjectList);
                }, 30, TimeUnit.SECONDS);
            });
    }

    // Not thread safe, caller should hold lock.
    // Visible for testing.
    void unsafeUpload(boolean force) throws WALFencedException {
        if (!force) {
            checkWriteStatus();
        }

        if (bufferQueue.isEmpty()) {
            return;
        }

        int size = bufferQueue.size();
        PriorityQueue<Record> recordQueue = new PriorityQueue<>(size, Comparator.comparingLong(o -> o.offset));

        for (int i = 0; i < size; i++) {
            Record record = bufferQueue.poll();
            if (record != null) {
                recordQueue.offer(record);
            }
        }

        // Trigger upload until the buffer is empty.
        while (!recordQueue.isEmpty()) {
            unsafeUpload(recordQueue);
        }
    }

    // Not thread safe, caller should hold lock.
    private void unsafeUpload(PriorityQueue<Record> recordQueue) {
        long startTime = time.nanoseconds();

        // Build data buffer.
        CompositeByteBuf dataBuffer = ByteBufAlloc.compositeByteBuffer();
        List<Record> recordList = new LinkedList<>();

        long stickyRecordLength = 0;
        if (!recordQueue.isEmpty()) {
            Record firstRecord = recordQueue.peek();
            if (firstRecord.record.readerIndex() != 0) {
                stickyRecordLength = firstRecord.record.readableBytes();
            }
        }

        while (!recordQueue.isEmpty()) {
            // The retained bytes in the batch must larger than record header size.
            long retainedBytesInBatch = config.maxBytesInBatch() - dataBuffer.readableBytes() - WALObjectHeader.DEFAULT_WAL_HEADER_SIZE;
            if (config.strictBatchLimit() && retainedBytesInBatch <= RecordHeader.RECORD_HEADER_SIZE) {
                break;
            }

            Record record = recordQueue.poll();

            // Records larger than the batch size will be uploaded immediately.
            assert record != null;
            if (config.strictBatchLimit() && record.record.readableBytes() >= config.maxBytesInBatch() - WALObjectHeader.DEFAULT_WAL_HEADER_SIZE) {
                dataBuffer.addComponent(true, record.record);
                recordList.add(record);
                break;
            }

            if (config.strictBatchLimit() && record.record.readableBytes() > retainedBytesInBatch) {
                // The records will be split into multiple objects.
                ByteBuf slice = record.record.retainedSlice(0, (int) retainedBytesInBatch).asReadOnly();
                dataBuffer.addComponent(true, slice);

                // Update the record buffer and offset.
                record.record.skipBytes((int) retainedBytesInBatch);
                record.offset += retainedBytesInBatch;
                recordQueue.offer(record);
                break;
            }

            dataBuffer.addComponent(true, record.record);
            recordList.add(record);
        }

        // Build object buffer.
        long firstOffset = recordList.get(0).offset;
        long dataLength = dataBuffer.readableBytes();
        // Exclusive end offset
        long endOffset = firstOffset + dataLength;

        CompositeByteBuf objectBuffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(firstOffset, dataLength, stickyRecordLength, config.nodeId(), config.epoch(), trimOffset.get());
        objectBuffer.addComponent(true, header.marshal());
        objectBuffer.addComponent(true, dataBuffer);

        // Trigger upload.
        int objectLength = objectBuffer.readableBytes();
        uploadMap.put(firstOffset, new UploadTask(recordList));

        // Enable fast retry.
        ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions().enableFastRetry(true);
        String path = String.format(OBJECT_PATH_FORMAT, objectPrefix, firstOffset, endOffset);
        CompletableFuture<ObjectStorage.WriteResult> uploadFuture = objectStorage.write(writeOptions, path, objectBuffer);

        CompletableFuture<Void> finalFuture = recordUploadMetrics(uploadFuture, startTime, objectLength)
            .thenAccept(result -> {
                long lockStartTime = time.nanoseconds();
                lock.writeLock().lock();
                try {
                    if (time.nanoseconds() - lockStartTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                        log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - lockStartTime));
                    }

                    objectMap.put(endOffset, new WALObject(result.bucket(), path, firstOffset, endOffset, objectLength));
                    objectDataBytes.addAndGet(objectLength);

                    uploadMap.get(firstOffset).markFinished();
                    List<UploadTask> finishedTasks = new ArrayList<>();
                    // Remove consecutive completed tasks from head.
                    Map.Entry<Long, UploadTask> entry;
                    while ((entry = uploadMap.firstEntry()) != null && entry.getValue().isFinished()) {
                        finishedTasks.add(uploadMap.remove(entry.getKey()));
                    }

                    // Update flushed offset
                    if (!uploadMap.isEmpty()) {
                        flushedOffset.set(uploadMap.firstKey());
                    } else if (!bufferQueue.isEmpty()) {
                        flushedOffset.set(bufferQueue.getFirst().offset);
                    } else {
                        flushedOffset.set(nextOffset.get());
                    }

                    // Release lock and complete future in callback thread.
                    for (UploadTask task : finishedTasks) {
                        callbackService.submit(() ->
                            task.records().forEach(record -> record.future.complete(flushedOffset::get))
                        );
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            })
            .whenComplete((v, throwable) -> {
                bufferedDataBytes.addAndGet(-dataLength);
                throwable = ExceptionUtils.getRootCause(throwable);
                if (throwable instanceof WALFencedException) {
                    List<Record> uploadedRecords = uploadMap.remove(firstOffset).records();
                    Throwable finalThrowable = throwable;
                    // Release lock and complete future in callback thread.
                    callbackService.submit(() -> uploadedRecords.forEach(record -> record.future.completeExceptionally(finalThrowable)));
                } else if (throwable != null) {
                    // Never fail the write task, the under layer storage will retry forever.
                    log.error("[Bug] Failed to write records to S3: {}", firstOffset, throwable);
                }
            });

        pendingFutureMap.put(finalFuture, time.nanoseconds());
        finalFuture.whenComplete((v, throwable) -> pendingFutureMap.remove(finalFuture));
        lastUploadTimestamp = System.currentTimeMillis();
    }

    protected CompletableFuture<ObjectStorage.WriteResult> recordUploadMetrics(
        CompletableFuture<ObjectStorage.WriteResult> future,
        long startTime, long objectLength) {
        return future.whenComplete((result, throwable) -> {
            ObjectWALMetricsManager.recordOperationLatency(time.nanoseconds() - startTime, "upload", throwable == null);
            ObjectWALMetricsManager.recordOperationDataSize(objectLength, "upload");
        });
    }

    private static class UploadTask {
        private final List<Record> records;
        private boolean finished = false;

        public UploadTask(List<Record> records) {
            this.records = records;
        }

        public List<Record> records() {
            return records;
        }

        public void markFinished() {
            this.finished = true;
        }

        public boolean isFinished() {
            return finished;
        }
    }

    protected static class Record {
        public final ByteBuf record;
        public final CompletableFuture<AppendResult.CallbackResult> future;
        public long offset;

        public Record(long offset, ByteBuf record,
            CompletableFuture<AppendResult.CallbackResult> future) {
            this.offset = offset;
            this.record = record;
            this.future = future;
        }
    }

    public static class WALObject implements Comparable<WALObject> {
        private final short bucketId;
        private final String path;
        private final long startOffset;
        private final long endOffset;
        private final long length;

        public WALObject(short bucketId, String path, long startOffset, long length) {
            this.bucketId = bucketId;
            this.path = path;
            this.startOffset = startOffset;
            this.endOffset = WALObjectHeader.calculateEndOffsetV0(startOffset, length);
            this.length = length;
        }

        public WALObject(short bucketId, String path, long startOffset, long endOffset, long length) {
            this.bucketId = bucketId;
            this.path = path;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.length = length;
        }

        @Override
        public int compareTo(WALObject o) {
            return Long.compare(startOffset, o.startOffset);
        }

        public short bucketId() {
            return bucketId;
        }

        public String path() {
            return path;
        }

        public long startOffset() {
            return startOffset;
        }

        public long length() {
            return length;
        }

        public long endOffset() {
            return endOffset;
        }

        @Override
        public String toString() {
            return "WALObject{" +
                "bucketId=" + bucketId +
                ", path='" + path + '\'' +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", length=" + length +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof WALObject))
                return false;
            WALObject object = (WALObject) o;
            return bucketId == object.bucketId && startOffset == object.startOffset && endOffset == object.endOffset && length == object.length && Objects.equals(path, object.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketId, path, startOffset, endOffset, length);
        }
    }
}
