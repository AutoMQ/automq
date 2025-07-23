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
import com.automq.stream.s3.wal.metrics.ObjectWALMetricsManager;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Threads;
import com.automq.stream.utils.Time;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import static com.automq.stream.s3.ByteBufAlloc.S3_WAL;
import static com.automq.stream.s3.wal.common.RecordHeader.RECORD_HEADER_SIZE;
import static com.automq.stream.s3.wal.impl.object.ObjectUtils.OBJECT_PATH_OFFSET_DELIMITER;

public class DefaultWriter implements Writer {
    private static final Logger log = LoggerFactory.getLogger(DefaultWriter.class);

    private static final long DEFAULT_LOCK_WARNING_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long DEFAULT_UPLOAD_WARNING_TIMEOUT = TimeUnit.SECONDS.toNanos(5);
    private static final String OBJECT_PATH_FORMAT = "%s%d" + OBJECT_PATH_OFFSET_DELIMITER + "%d"; // {objectPrefix}/{startOffset}-{endOffset}
    private static final ByteBufSeqAlloc BYTE_BUF_ALLOC = new ByteBufSeqAlloc(S3_WAL, 8);

    protected final ObjectWALConfig config;
    protected final Time time;
    protected final ObjectStorage objectStorage;
    protected final ReservationService reservationService;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private List<WALObject> previousObjects = new ArrayList<>();
    private final Queue<UploadTask> uploadQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentNavigableMap<Long, WALObject> lastRecordOffset2object = new ConcurrentSkipListMap<>();
    private final String nodePrefix;
    private final String objectPrefix;
    private final ScheduledExecutorService executorService;
    private final ScheduledExecutorService utilityService;
    private final ExecutorService callbackService;
    private final ConcurrentMap<CompletableFuture<Void>, Long> uploadingTasks = new ConcurrentHashMap<>();
    private final AtomicLong objectDataBytes = new AtomicLong();
    private final AtomicLong bufferedDataBytes = new AtomicLong();
    private final AtomicLong waitingFlushDirtyBytes = new AtomicLong();
    protected volatile boolean closed = true;
    protected volatile boolean fenced;
    private final BlockingQueue<Record> bufferQueue = new LinkedBlockingQueue<>();
    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private final AtomicLong nextOffset = new AtomicLong();
    private final AtomicLong flushedOffset = new AtomicLong();
    private final AtomicLong trimOffset = new AtomicLong(-1);

    public DefaultWriter(Time time, ObjectStorage objectStorage, ObjectWALConfig config) {
        this.time = time;
        this.objectStorage = objectStorage;
        this.reservationService = config.reservationService();
        this.config = config;
        this.nodePrefix = ObjectUtils.nodePrefix(config.clusterId(), config.nodeId(), config.type());
        this.objectPrefix = nodePrefix + config.epoch() + "/wal/";
        this.executorService = Threads.newSingleThreadScheduledExecutor("s3-wal-schedule", true, log);
        this.utilityService = Threads.newSingleThreadScheduledExecutor("s3-wal-utility", true, log);
        // TODO: orderly callback
        this.callbackService = Threads.newFixedThreadPoolWithMonitor(1, "s3-wal-callback", false, log);

        ObjectWALMetricsManager.setInflightUploadCountSupplier(() -> (long) uploadingTasks.size());
        ObjectWALMetricsManager.setBufferedDataInBytesSupplier(bufferedDataBytes::get);
        ObjectWALMetricsManager.setObjectDataInBytesSupplier(objectDataBytes::get);
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
                .thenAccept(nil -> log.info("Delete overlap objects: {}", overlapObjects));
        }
        long largestEpoch;
        if (!objects.isEmpty() && (largestEpoch = objects.get(objects.size() - 1).epoch()) > config.epoch()) {
            log.warn("Detect newer epoch={} WAL started, exit current WAL start", largestEpoch);
            fenced = true;
            return;
        }
        objects.forEach(object -> objectDataBytes.addAndGet(object.length()));

        previousObjects.addAll(objects);

        // TODO: force align before accept new data.
        flushedOffset.set(objects.isEmpty() ? 0 : objects.get(objects.size() - 1).endOffset());
        nextOffset.set(flushedOffset.get());

        startPeriodUpload();
        startMonitor();


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
        if (!uploadingTasks.isEmpty()) {
            log.info("Wait for {} pending upload tasks to complete.", uploadingTasks.size());
            try {
                CompletableFuture.allOf(uploadingTasks.keySet().toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
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
        if (config.openMode() != OpenMode.READ_WRITE) {
            throw new IllegalStateException("WAL is in failover mode.");
        }

        checkStatus();
    }

    private boolean shouldUpload() {
        if (uploadQueue.size() >= config.maxInflightUploadCount()) {
            return false;
        }

        return System.currentTimeMillis() - lastUploadTimestamp >= config.batchInterval()
            || waitingFlushDirtyBytes.get() > config.maxBytesInBatch();
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
    public long confirmOffset() {
        return flushedOffset.get();
    }

    public CompletableFuture<AppendResult> append0(StreamRecordBatch streamRecordBatch) throws OverCapacityException, WALFencedException {
        long startTime = time.nanoseconds();
        checkWriteStatus();

        // Check if there is too much data in the S3 WAL.
        if (nextOffset.get() - flushedOffset.get() > config.maxUnflushedBytes()) {
            throw new OverCapacityException("Too many unflushed bytes.", true);
        }

        if (lastRecordOffset2object.size() + config.maxInflightUploadCount() >= 3000) {
            throw new OverCapacityException("Too many WAL objects.", false);
        }

        int dataSize = streamRecordBatch.encoded().readableBytes() + RecordHeader.RECORD_HEADER_SIZE;

        waitingFlushDirtyBytes.addAndGet(dataSize);
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

        CompletableFuture<AppendResult> future = new CompletableFuture<>();

        long acquireAppendLockTime = time.nanoseconds();
        lock.readLock().lock();
        try {
            if (time.nanoseconds() - acquireAppendLockTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: append", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - acquireAppendLockTime));
            }

            future.whenComplete((v, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to append record to S3 WAL", throwable);
                } else {
                    ObjectWALMetricsManager.recordOperationDataSize(dataSize, "append");
                }
                ObjectWALMetricsManager.recordOperationLatency(time.nanoseconds() - acquireAppendLockTime, "append", throwable == null);
            });

            bufferQueue.offer(new Record(streamRecordBatch, future));
            bufferedDataBytes.addAndGet(dataSize);

            return future;
        } finally {
            lock.readLock().unlock();
        }
    }

    public CompletableFuture<Void> reset() throws WALFencedException {
        return trim0(nextOffset.get());
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
            log.error("Recover S3 WAL failed, due to unrecoverable exception.", e);
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
    public CompletableFuture<Void> trim0(long newStartOffset) throws WALFencedException {
        checkStatus();
        List<ObjectStorage.ObjectPath> deleteObjectList = new ArrayList<>();
        AtomicLong deletedObjectSize = new AtomicLong();
        long startTime = time.nanoseconds();
        CompletableFuture<?> persistTrimOffsetCf;
        lock.writeLock().lock();
        try {
            if (trimOffset.get() >= newStartOffset) {
                return CompletableFuture.completedFuture(null);
            }
            trimOffset.set(newStartOffset);
            // We cannot force upload an empty wal object cause of the recover workflow don't accept an empty wal object.
            // So we use a fake record to trigger the wal object upload.
            persistTrimOffsetCf = append(new StreamRecordBatch(-1L, -1L, 0, 0, Unpooled.EMPTY_BUFFER));
            unsafeUpload(false);
        } catch (Throwable e) {
            persistTrimOffsetCf = CompletableFuture.failedFuture(e);
        } finally {
            lock.writeLock().unlock();
        }

        return persistTrimOffsetCf.thenCompose(nil -> {
            Long lastFlushedRecordOffset = lastRecordOffset2object.lastKey();
            if (lastFlushedRecordOffset != null) {
                lastRecordOffset2object.headMap(newStartOffset, true)
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
                    if (object.endOffset() > newStartOffset) {
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
        });
    }

    private void startPeriodUpload() {
        executorService.scheduleWithFixedDelay(() -> {
            long startTime = time.nanoseconds();
            if (fenced || bufferQueue.isEmpty() || System.currentTimeMillis() - lastUploadTimestamp < config.batchInterval()) {
                return;
            }

            lock.writeLock().lock();
            try {
                if (time.nanoseconds() - startTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                    log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: scheduled_upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTime));
                }

                if (System.currentTimeMillis() - lastUploadTimestamp >= config.batchInterval()) {
                    // TODO: async the cost part
                    unsafeUpload(false);
                }
            } catch (Throwable ignore) {
            } finally {
                lock.writeLock().unlock();
            }
        }, config.batchInterval(), Math.max(config.batchInterval() / 10, 1), TimeUnit.MILLISECONDS);
    }

    private void startMonitor() {
        utilityService.scheduleWithFixedDelay(() -> {
            try {
                long count = uploadingTasks.values()
                    .stream()
                    .filter(uploadTime -> time.nanoseconds() - uploadTime > DEFAULT_UPLOAD_WARNING_TIMEOUT)
                    .count();
                if (count > 0) {
                    log.error("Found {} pending upload tasks exceed 5s.", count);
                }
            } catch (Throwable ignore) {
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    // Not thread safe, caller should hold lock.
    // Visible for testing.
    CompletableFuture<Void> unsafeUpload(boolean force) throws WALFencedException {
        if (!force) {
            checkWriteStatus();
        }

        if (bufferQueue.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        int size = bufferQueue.size();
        List<Record> records = new ArrayList<>(size);
        bufferQueue.drainTo(records);
        waitingFlushDirtyBytes.set(0);
        // TODO: limit the records batch lower than DATA_FILE_ALIGN_SIZE
        return unsafeUpload(records);
    }

    // Not thread safe, caller should hold lock.
    private CompletableFuture<Void> unsafeUpload(List<Record> records) {
        long startTime = time.nanoseconds();
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

        long firstOffset = this.nextOffset.get();
        long nextOffset = firstOffset;
        long lastRecordOffset = nextOffset;
        CompositeByteBuf dataBuffer = ByteBufAlloc.compositeByteBuffer();
        for (Record record : records) {
            record.offset = nextOffset;
            lastRecordOffset = record.offset;
            ByteBuf data = record.streamRecordBatch.encoded();
            ByteBuf header = BYTE_BUF_ALLOC.byteBuffer(RECORD_HEADER_SIZE);
            header = WALUtil.generateHeader(data, header, 0, nextOffset, true);
            record.size = data.readableBytes() + header.readableBytes();
            nextOffset += record.size;
            dataBuffer.addComponent(true, header);
            dataBuffer.addComponent(true, data);
        }

        // Build object buffer.
        long dataLength = dataBuffer.readableBytes();
        nextOffset = ObjectUtils.ceilAlignOffset(nextOffset + WALObjectHeader.WAL_HEADER_SIZE_V1);
        long endOffset = nextOffset;
        this.nextOffset.set(nextOffset);

        CompositeByteBuf objectBuffer = ByteBufAlloc.compositeByteBuffer();
        WALObjectHeader header = new WALObjectHeader(firstOffset, dataLength, 0, config.nodeId(), config.epoch(), trimOffset.get());
        objectBuffer.addComponent(true, header.marshal());
        objectBuffer.addComponent(true, dataBuffer);

        // Trigger upload.
        int objectLength = objectBuffer.readableBytes();

        // Enable fast retry.
        ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions().enableFastRetry(true);
        String path = String.format(OBJECT_PATH_FORMAT, objectPrefix, firstOffset, endOffset);
        CompletableFuture<ObjectStorage.WriteResult> uploadFuture = objectStorage.write(writeOptions, path, objectBuffer);

        UploadTask uploadTask = new UploadTask(records, endOffset, uploadFuture);
        uploadQueue.add(uploadTask);

        long finalLastRecordOffset = lastRecordOffset;
        CompletableFuture<Void> finalFuture = recordUploadMetrics(uploadFuture, startTime, objectLength)
            .thenCompose(writeResult -> {
                long commitStartTime = time.nanoseconds();
                return reservationService.verify(config.nodeId(), config.epoch(), false)
                    .whenComplete((result, throwable) ->
                        ObjectWALMetricsManager.recordOperationLatency(time.nanoseconds() - commitStartTime, "commit", throwable == null))
                    .thenApply(result -> {
                        if (!result) {
                            fenced = true;
                            WALFencedException exception = new WALFencedException("Failed to verify the permission with node id: " + config.nodeId() + ", node epoch: " + config.epoch() + ", failover flag: " + config.openMode());
                            throw new CompletionException(exception);
                        }
                        return writeResult;
                    });
            })
            .thenAccept(result -> {
                long lockStartTime = time.nanoseconds();
                lock.writeLock().lock();
                try {
                    if (time.nanoseconds() - lockStartTime > DEFAULT_LOCK_WARNING_TIMEOUT) {
                        log.warn("Failed to acquire lock in {}ms, cost: {}ms, operation: upload", TimeUnit.NANOSECONDS.toMillis(DEFAULT_LOCK_WARNING_TIMEOUT), TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - lockStartTime));
                    }
                    lastRecordOffset2object.put(finalLastRecordOffset, new WALObject(result.bucket(), path, config.epoch(), firstOffset, endOffset, objectLength));
                    objectDataBytes.addAndGet(objectLength);

                    while (true) {
                        UploadTask task = uploadQueue.peek();
                        if (task == null || !task.cf.isDone()) {
                            break;
                        }
                        uploadQueue.poll();
                        flushedOffset.set(task.endOffset());
                        // Release lock and complete future in callback thread.
                        callbackService.submit(() ->
                            task.records().forEach(
                                record -> record.future.complete(new DefaultAppendResult(DefaultRecordOffset.of(record.offset, record.size)))
                            )
                        );
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            })
            .whenComplete((v, ex) -> {
                bufferedDataBytes.addAndGet(-dataLength);
                ex = ExceptionUtils.getRootCause(ex);
                if (ex == null) {
                    return;
                }
                fenced = true;
                if (!(ex instanceof WALFencedException)) {
                    // Never fail the write task, the under layer storage will retry forever.
                    log.error("[BUG] Failed to write records to S3: {}", firstOffset, ex);
                }
                lock.writeLock().lock();
                try {
                    for (; ; ) {
                        UploadTask task = uploadQueue.poll();
                        if (task == null) {
                            break;
                        }
                        Throwable finalThrowable = ex;
                        // Release lock and complete future in callback thread.
                        callbackService.submit(() -> task.records.forEach(record -> record.future.completeExceptionally(finalThrowable)));
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            });

        uploadingTasks.put(finalFuture, time.nanoseconds());
        finalFuture.whenComplete((v, throwable) -> uploadingTasks.remove(finalFuture));
        lastUploadTimestamp = System.currentTimeMillis();
        return finalFuture;
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
        private final long endOffset;
        private final CompletableFuture<ObjectStorage.WriteResult> cf;

        public UploadTask(List<Record> records, long endOffset, CompletableFuture<ObjectStorage.WriteResult> cf) {
            this.records = records;
            this.endOffset = endOffset;
            this.cf = cf;
        }

        public List<Record> records() {
            return records;
        }

        public long endOffset() {
            return endOffset;
        }

        public CompletableFuture<ObjectStorage.WriteResult> cf() {
            return cf;
        }
    }

    protected static class Record {
        public final StreamRecordBatch streamRecordBatch;
        public final CompletableFuture<AppendResult> future;
        public long offset;
        public int size;

        public Record(StreamRecordBatch streamRecordBatch, CompletableFuture<AppendResult> future) {
            this.streamRecordBatch = streamRecordBatch;
            this.future = future;
        }
    }

}
