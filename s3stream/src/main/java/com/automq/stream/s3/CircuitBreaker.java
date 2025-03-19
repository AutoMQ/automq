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

package com.automq.stream.s3;

import com.automq.stream.s3.compact.CompactOperations;
import com.automq.stream.s3.exceptions.CompactedObjectsNotFoundException;
import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectAttributes;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import com.automq.stream.s3.operator.Writer;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import io.netty.buffer.ByteBuf;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CircuitBreaker {
    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreaker.class);
    private static final int READ_BLOCK_SIZE = 32 * 1024 * 1024;
    private final ScheduledExecutorService scheduler = Threads.newSingleThreadScheduledExecutor("OBJECT_STORAGE_CIRCUIT_BREAKER", true, LOGGER);
    private final AtomicLong tasksEpoch = new AtomicLong();
    private volatile long processedTasksEpoch = 0;
    private CompletableFuture<Void> lastTransitionCf = CompletableFuture.completedFuture(null);
    private CircuitStatus circuitStatus = CircuitStatus.OPEN;

    private final long halfOpen2OpenWatchMs;
    private final ObjectStorage mainStorage;
    private final ObjectStorage fallbackStorage;
    private final NodeCircuitStatusManagerStub nodeStatusManagerStub;
    private final ObjectManager objectManager;

    public CircuitBreaker(ObjectStorage mainStorage, ObjectStorage fallbackStorage,
        NodeCircuitStatusManagerStub nodeStatusManagerStub, ObjectManager objectManager, long halfOpen2OpenWatchMs) {
        this.mainStorage = mainStorage;
        this.fallbackStorage = fallbackStorage;
        this.nodeStatusManagerStub = nodeStatusManagerStub;
        this.objectManager = objectManager;
        this.halfOpen2OpenWatchMs = halfOpen2OpenWatchMs;
        init();
    }

    private synchronized void init() {
        try {
            List<ObjectInfo> objects = this.fallbackStorage.list("").get();
            if (objects.isEmpty()) {
                transitionTo(CircuitStatus.OPEN);
            } else {
                tasksEpoch.incrementAndGet();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        scheduler.scheduleWithFixedDelay(this::run, 10 + ThreadLocalRandom.current().nextInt(10), 1, TimeUnit.SECONDS);
    }

    public synchronized CircuitStatus status() {
        return circuitStatus;
    }

    public synchronized void addFallbackTask(CompletableFuture<Void> cf) {
        tasksEpoch.incrementAndGet();
        cf.whenComplete((nil, ex) -> {
            synchronized (CircuitBreaker.this) {
                tasksEpoch.incrementAndGet();
            }
        });
    }

    public synchronized CompletableFuture<Void> transitionTo(CircuitStatus to) {
        CompletableFuture<Void> retCf = new CompletableFuture<>();
        lastTransitionCf.whenComplete((nil, ex) -> {
            try {
                transitionTo0(to, retCf);
            } catch (Throwable e) {
                retCf.completeExceptionally(e);
            }
        });
        this.lastTransitionCf = retCf;
        return retCf;
    }

    private synchronized void transitionTo0(CircuitStatus to, CompletableFuture<Void> retCf) {
        CircuitStatus from = circuitStatus;
        if (from == CircuitStatus.OPEN && to == CircuitStatus.HALF_OPEN) {
            throw new IllegalStateException("Cannot transition from OPEN to HALF_OPEN");
        }
        if (from != to) {
            LOGGER.info("[CIRCUIT_STATUS_TRANSITION],from={},to={}", from, to);
            FutureUtil.propagate(nodeStatusManagerStub.transitionTo(to).thenAccept(nil -> {
                synchronized (CircuitBreaker.this) {
                    this.circuitStatus = to;
                }
            }), retCf);
        } else {
            retCf.complete(null);
        }
    }

    public void run() {
        try {
            long taskEpochSnapshot;
            synchronized (this) {
                taskEpochSnapshot = this.tasksEpoch.get();
                if (taskEpochSnapshot == processedTasksEpoch) {
                    return;
                }
            }
            run0();
            processedTasksEpoch = taskEpochSnapshot;
            synchronized (this) {
                if (processedTasksEpoch == this.tasksEpoch.get()) {
                    scheduler.schedule(() -> recheckAndOpen(taskEpochSnapshot), halfOpen2OpenWatchMs, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("[CIRCUIT_BREAKER_RUN_FAIL]", e);
        }
    }

    private void recheckAndOpen(long processedTasksEpoch) {
        synchronized (this) {
            if (processedTasksEpoch == this.tasksEpoch.get()) {
                // means there is no new fallback task
                transitionTo(CircuitStatus.OPEN);
            }
        }
    }

    private void run0() throws Throwable {
        if (status() == CircuitStatus.OPEN) {
            return;
        }
        // list all objects in fallback storage
        Queue<ObjectInfo> fallbackObjects = new LinkedList<>(fallbackStorage.list("").get());
        while (!fallbackObjects.isEmpty()) {
            ObjectInfo object = fallbackObjects.peek();
            TimerUtil timer = new TimerUtil();
            long oldObjectId = ObjectUtils.parseObjectId(0, object.key());
            long newObjectId = objectManager.prepareObject(1, TimeUnit.HOURS.toMillis(12)).get();
            String newObjectKey = ObjectUtils.genKey(0, newObjectId);
            Writer writer = mainStorage.writer(new ObjectStorage.WriteOptions(), newObjectKey);
            try (
                ObjectReader reader = new ObjectReader.DefaultObjectReader(
                    new S3ObjectMetadata(oldObjectId, object.size(), ObjectAttributes.builder().type(ObjectAttributes.Type.Normal)
                        .bucket(object.bucketId()).build().attributes()),
                    fallbackStorage
                )
            ) {
                ObjectReader.BasicObjectInfo basicObjectInfo = reader.basicObjectInfo().get();

                long nextReadStartPosition = 0;
                while (nextReadStartPosition < object.size()) {
                    long end = Math.min(nextReadStartPosition + READ_BLOCK_SIZE, object.size());
                    ByteBuf data = fallbackStorage.rangeRead(new ObjectStorage.ReadOptions().bucket(object.bucketId()), object.key(), nextReadStartPosition, end).get();
                    writer.write(data);
                    nextReadStartPosition = end;
                }
                writer.close().get();

                List<StreamOffsetRange> streamOffsetRanges = basicObjectInfo.indexBlock().streamOffsetRanges();
                int newObjectAttributes = ObjectAttributes.builder().bucket(writer.bucketId()).build().attributes();
                if (streamOffsetRanges.size() > 1) {
                    CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
                    request.setObjectId(newObjectId);
                    request.setOrderId(-1);
                    request.setObjectSize(object.size());
                    request.setStreamRanges(streamOffsetRanges
                        .stream()
                        .map(s -> new ObjectStreamRange(s.streamId(), -1L, s.startOffset(), s.endOffset(), -1)).
                        collect(Collectors.toList()));
                    request.setCompactedObjectIds(List.of(oldObjectId));
                    request.setAttributes(newObjectAttributes);
                    sendCompactRequest(request);
                } else {
                    StreamOffsetRange stream = streamOffsetRanges.get(0);
                    CompactStreamObjectRequest request = new CompactStreamObjectRequest(
                        newObjectId,
                        object.size(),
                        stream.streamId(),
                        -1L,
                        stream.startOffset(),
                        stream.endOffset(),
                        List.of(oldObjectId),
                        List.of(CompactOperations.DELETE),
                        newObjectAttributes
                    );
                    sendCompactRequest(request);
                }
                fallbackStorage.delete(List.of(object));
                fallbackObjects.poll();
            } catch (Throwable e) {
                // TODO: writer support abort
                writer.release();
                LOGGER.error("[FALLBACK_TO_MAIN],[FAIL],object={}", object, e);
                throw e;
            }
            long elapsedMs = timer.elapsedAs(TimeUnit.MILLISECONDS);
            LOGGER.info("[FALLBACK_TO_MAIN],object={},cost={}ms", object, elapsedMs);
            if (elapsedMs > 30000) {
                transitionTo(CircuitStatus.CLOSED).get();
            } else {
                transitionTo(CircuitStatus.HALF_OPEN).get();
            }
        }
    }

    private void sendCompactRequest(Object request) throws Throwable {
        try {
            if (request instanceof CompactStreamObjectRequest) {
                objectManager.compactStreamObject((CompactStreamObjectRequest) request).get();
            } else {
                objectManager.commitStreamSetObject((CommitStreamSetObjectRequest) request).get();
            }
        } catch (Exception e) {
            Throwable cause = FutureUtil.cause(e);
            if (!(cause instanceof CompactedObjectsNotFoundException)) {
                throw cause;
            }
        }
    }

    public enum CircuitStatus {
        OPEN, HALF_OPEN, CLOSED
    }

    public interface NodeCircuitStatusManagerStub {
        CompletableFuture<Void> transitionTo(CircuitStatus status);
    }
}
