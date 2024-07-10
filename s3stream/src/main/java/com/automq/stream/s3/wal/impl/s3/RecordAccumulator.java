/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.impl.s3;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.exception.WALFencedException;
import com.automq.stream.s3.wal.metrics.ObjectStorageWALMetricsManager;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordAccumulator implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private static final long DEFAULT_LOCK_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long DEFAULT_UPLOAD_TIMEOUT = TimeUnit.SECONDS.toNanos(5);

    private final ReentrantLock lock = new ReentrantLock();
    private List<Record> bufferList = new LinkedList<>();
    private final ConcurrentNavigableMap<Long /* inclusive first offset */, List<Record>> uploadMap = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Long /* exclusive end offset */, WALObject> objectMap = new ConcurrentSkipListMap<>();

    protected final ObjectStorageWALConfig config;
    private final String nodePrefix;
    private final String objectPrefix;

    private final Time time;
    private final ObjectStorage objectStorage;

    private final ScheduledExecutorService executorService;
    private final ScheduledExecutorService monitorService;
    private final ExecutorService callbackService;

    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private final ConcurrentMap<CompletableFuture<Void>, Long> pendingFutureMap = new ConcurrentHashMap<>();
    private final AtomicLong objectDataBytes = new AtomicLong();
    private final AtomicLong bufferedDataBytes = new AtomicLong();

    private long nextOffset = 0;
    private long flushedOffset = 0;

    protected volatile boolean closed = true;
    protected volatile boolean fenced;

    public RecordAccumulator(Time time, ObjectStorage objectStorage,
        ObjectStorageWALConfig config) {
        this.time = time;
        this.objectStorage = objectStorage;
        this.config = config;
        this.nodePrefix = DigestUtils.md5Hex(String.valueOf(config.nodeId())).toUpperCase() + "/" + config.clusterId() + "/" + config.nodeId() + "/";
        this.objectPrefix = nodePrefix + config.epoch() + "/wal/";
        this.executorService = Threads.newSingleThreadScheduledExecutor("s3-wal-schedule", true, log);
        this.monitorService = Threads.newSingleThreadScheduledExecutor("s3-wal-monitor", true, log);
        this.callbackService = Threads.newFixedThreadPoolWithMonitor(4, "s3-wal-callback", false, log);

        ObjectStorageWALMetricsManager.setInflightUploadCountSupplier(() -> (long) pendingFutureMap.size());
        ObjectStorageWALMetricsManager.setBufferedDataInBytesSupplier(bufferedDataBytes::get);
        ObjectStorageWALMetricsManager.setObjectDataInBytesSupplier(objectDataBytes::get);
    }

    public void start() {
        objectStorage.list(nodePrefix)
            .thenAccept(objectList -> objectList.forEach(object -> {
                String path = object.key();
                String[] parts = path.split("/");
                try {
                    long firstOffset = Long.parseLong(parts[parts.length - 1]);
                    long length = object.size();
                    long lastOffset = firstOffset + length - 1;
                    objectMap.put(lastOffset, new WALObject(path, firstOffset, length));
                } catch (NumberFormatException e) {
                    // Ignore invalid path
                    log.warn("Found invalid wal object: {}", path);
                }
            }))
            .join();

        flushedOffset = objectMap.isEmpty() ? 0 : objectMap.lastKey();
        nextOffset = flushedOffset;

        // Trigger upload periodically.
        executorService.scheduleWithFixedDelay(() -> {
            long startTime = time.nanoseconds();
            lock.lock();
            try {
                if (time.nanoseconds() - startTime > DEFAULT_LOCK_TIMEOUT) {
                    log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: scheduled_upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTime));
                }

                if (fenced
                    || bufferList.isEmpty()
                    || System.currentTimeMillis() - lastUploadTimestamp < config.batchInterval()) {
                    return;
                }

                unsafeUpload(false);
            } catch (Throwable ignore) {
            } finally {
                lock.unlock();
            }
        }, config.batchInterval(), config.batchInterval(), TimeUnit.MILLISECONDS);

        monitorService.scheduleWithFixedDelay(() -> {
            try {
                long count = pendingFutureMap.values()
                    .stream()
                    .filter(uploadTime -> time.nanoseconds() - uploadTime > DEFAULT_UPLOAD_TIMEOUT)
                    .count();
                if (count > 0) {
                    log.warn("Found {} pending upload tasks exceed 5s.", count);
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

        lock.lock();
        try {
            unsafeUpload(true);
        } catch (Throwable throwable) {
            log.error("Failed to flush records when close.", throwable);
        } finally {
            lock.unlock();
        }

        // Wait for all upload tasks to complete.
        if (!pendingFutureMap.isEmpty()) {
            log.info("Wait for {} pending upload tasks to complete.", pendingFutureMap.size());
            CompletableFuture.allOf(pendingFutureMap.keySet().toArray(new CompletableFuture[0])).join();
        }

        if (monitorService != null && !monitorService.isShutdown()) {
            monitorService.shutdown();
            try {
                if (!monitorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.error("Monitor executor {} did not terminate in time", executorService);
                    monitorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Failed to shutdown monitor executor service.", e);
                monitorService.shutdownNow();
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
        return nextOffset;
    }

    public long flushedOffset() {
        return flushedOffset;
    }

    public List<WALObject> objectList() {
        long startTime = time.nanoseconds();
        lock.lock();
        try {
            if (time.nanoseconds() - startTime > DEFAULT_LOCK_TIMEOUT) {
                log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: list_objects", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTime));
            }

            return new ArrayList<>(objectMap.values());
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    protected ScheduledExecutorService executorService() {
        return executorService;
    }

    protected void checkStatus() {
        if (closed) {
            throw new IllegalStateException("WAL is closed.");
        }

        if (fenced) {
            throw new WALFencedException("WAL is fenced.");
        }
    }

    protected void checkWriteStatus() {
        if (config.failover()) {
            throw new IllegalStateException("WAL is in failover mode.");
        }

        checkStatus();
    }

    public long append(long recordSize, Function<Long, ByteBuf> recordSupplier,
        CompletableFuture<AppendResult.CallbackResult> future) throws OverCapacityException {
        checkWriteStatus();

        long startTime = time.nanoseconds();
        lock.lock();
        try {
            if (time.nanoseconds() - startTime > DEFAULT_LOCK_TIMEOUT) {
                log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: append", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTime));
            }

            if (nextOffset - flushedOffset > config.maxUnflushedBytes()) {
                throw new OverCapacityException("Too many unflushed bytes.");
            }

            if (!bufferList.isEmpty()
                && uploadMap.size() < config.maxInflightUploadCount()
                && (System.currentTimeMillis() - lastUploadTimestamp >= config.batchInterval()
                    || nextOffset - bufferList.get(0).offset > config.maxBytesInBatch())) {
                unsafeUpload(false);
            }

            long offset = nextOffset;
            future.whenComplete((v, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to append record to S3 WAL: {}", offset, throwable);
                } else {
                    ObjectStorageWALMetricsManager.recordOperationDataSize(recordSize, "append");
                }
                ObjectStorageWALMetricsManager.recordOperationLatency(time.nanoseconds() - startTime, "append", throwable == null);
            });

            bufferList.add(new Record(offset, recordSupplier.apply(offset), future));

            nextOffset += recordSize;
            bufferedDataBytes.addAndGet(recordSize);

            return offset;
        } finally {
            lock.unlock();
        }
    }

    // Trim objects where the last offset is less than or equal to the given offset.
    public CompletableFuture<Void> trim(long offset) {
        checkStatus();

        if (objectMap.isEmpty() || offset < objectMap.firstKey() || offset > flushedOffset) {
            return CompletableFuture.completedFuture(null);
        }

        long startTime = time.nanoseconds();

        List<ObjectStorage.ObjectPath> deleteObjectList = new ArrayList<>();
        AtomicLong deletedObjectSize = new AtomicLong();

        objectMap.headMap(offset, true)
            .forEach((k, v) -> {
                deleteObjectList.add(new ObjectStorage.ObjectPath((short) 0, v.path()));
                deletedObjectSize.addAndGet(v.length());
                objectMap.remove(k);
            });

        if (deleteObjectList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return objectStorage.delete(deleteObjectList)
            .whenComplete((v, throwable) -> {
                ObjectStorageWALMetricsManager.recordOperationLatency(time.nanoseconds() - startTime, "trim", throwable == null);
                objectDataBytes.addAndGet(-1 * deletedObjectSize.get());

                // Never fail the delete task, the under layer storage will retry forever.
                if (throwable != null) {
                    log.error("Failed to delete objects when trim S3 WAL: {}", deleteObjectList, throwable);
                }
            });
    }

    // Not thread safe, caller should hold lock.
    protected void unsafeUpload(boolean force) throws OverCapacityException {
        if (!force) {
            checkWriteStatus();
        }

        long startTime = time.nanoseconds();
        // No records to upload
        if (bufferList.isEmpty()) {
            return;
        }

        // TODO: The number of uploads in progress should not be greater than the number of active S3 connections.
        if (!force && uploadMap.size() > config.maxInflightUploadCount()) {
            throw new OverCapacityException("Too many pending upload");
        }

        Record firstRecord = bufferList.get(0);
        Record lastRecord = bufferList.get(bufferList.size() - 1);

        long firstOffset = firstRecord.offset;
        // Exclusive end offset
        long endOffset = lastRecord.offset + lastRecord.record.readableBytes();

        long dataLength = lastRecord.offset + lastRecord.record.readableBytes() - firstOffset;

        CompositeByteBuf buffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(firstOffset, dataLength, lastRecord.offset, config.nodeId(), config.epoch());
        buffer.addComponent(true, header.marshal());

        bufferList.forEach(record -> buffer.addComponent(true, record.record));
        int objectLength = buffer.readableBytes();

        // Clean up records.
        uploadMap.put(firstOffset, bufferList);
        bufferList = new LinkedList<>();

        String path = objectPrefix + firstOffset;
        CompletableFuture<ObjectStorage.WriteResult> uploadFuture = objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, path, buffer);

        CompletableFuture<Void> finalFuture = recordUploadMetrics(uploadFuture, startTime, objectLength)
            .thenAccept(v -> {
                long lockStartTime = time.nanoseconds();
                lock.lock();
                try {
                    if (time.nanoseconds() - lockStartTime > DEFAULT_LOCK_TIMEOUT) {
                        log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - lockStartTime));
                    }

                    objectMap.put(endOffset, new WALObject(path, firstOffset, objectLength));
                    objectDataBytes.addAndGet(objectLength);

                    List<Record> uploadedRecords = uploadMap.remove(firstOffset);

                    // Update flushed offset
                    if (!uploadMap.isEmpty()) {
                        flushedOffset = uploadMap.firstKey();
                    } else if (!bufferList.isEmpty()) {
                        flushedOffset = bufferList.get(0).offset;
                    } else {
                        flushedOffset = nextOffset;
                    }

                    // Release lock and complete future in callback thread.
                    callbackService.submit(() -> uploadedRecords.forEach(record -> record.future.complete(() -> flushedOffset)));
                } finally {
                    lock.unlock();
                }
            })
            .whenComplete((v, throwable) -> {
                bufferedDataBytes.addAndGet(-objectLength);
                if (throwable instanceof WALFencedException) {
                    List<Record> uploadedRecords = uploadMap.remove(firstOffset);
                    uploadedRecords.forEach(record -> record.future.completeExceptionally(throwable));
                } else if (throwable != null) {
                    // Never fail the write task, the under layer storage will retry forever.
                    log.error("[Bug] Failed to write records to S3: {}", firstOffset, throwable);
                }
            });

        pendingFutureMap.put(finalFuture, time.nanoseconds());
        finalFuture.whenComplete((v, throwable) -> pendingFutureMap.remove(finalFuture));
        lastUploadTimestamp = System.currentTimeMillis();
    }

    protected CompletableFuture<ObjectStorage.WriteResult> recordUploadMetrics(CompletableFuture<ObjectStorage.WriteResult> future,
        long startTime, long objectLength) {
        return future.whenComplete((result, throwable) -> {
            ObjectStorageWALMetricsManager.recordOperationLatency(time.nanoseconds() - startTime, "upload", throwable == null);
            ObjectStorageWALMetricsManager.recordOperationDataSize(objectLength, "upload");
        });
    }

    protected static class Record {
        public final long offset;
        public final ByteBuf record;
        public final CompletableFuture<AppendResult.CallbackResult> future;

        public Record(long offset, ByteBuf record,
            CompletableFuture<AppendResult.CallbackResult> future) {
            this.offset = offset;
            this.record = record;
            this.future = future;
        }
    }

    public static class WALObject implements Comparable<WALObject> {
        private final String path;
        private final long startOffset;
        private final long length;

        public WALObject(String path, long startOffset, long length) {
            this.path = path;
            this.startOffset = startOffset;
            this.length = length;
        }

        @Override
        public int compareTo(WALObject o) {
            return Long.compare(startOffset, o.startOffset);
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
    }
}
