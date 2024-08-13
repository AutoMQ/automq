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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.utils.FutureUtil;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DeleteObjectsAccumulator {
    static final Logger LOGGER = LoggerFactory.getLogger(DeleteObjectsAccumulator.class);
    public static final int DEFAULT_DELETE_OBJECTS_MAX_BATCH_SIZE = 1000;
    public static final int DEFAULT_DELETE_OBJECTS_MAX_CONCURRENT_REQUEST_NUMBER = 100;
    private final Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction;
    private final ConcurrentLinkedDeque<PendingDeleteRequest> deleteRequestQueue = new ConcurrentLinkedDeque<>();
    private final ReentrantLock queueLock = new ReentrantLock();


    private final int maxBatchSize;
    private final Semaphore concurrentRequestLimiter;

    public DeleteObjectsAccumulator(Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction) {
        this(DEFAULT_DELETE_OBJECTS_MAX_BATCH_SIZE, DEFAULT_DELETE_OBJECTS_MAX_CONCURRENT_REQUEST_NUMBER, deleteObjectsFunction);
    }

    public DeleteObjectsAccumulator(int maxBatchSize,
        int maxConcurrentRequestNumber,
        Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction) {
        this.deleteObjectsFunction = deleteObjectsFunction;
        this.maxBatchSize = maxBatchSize;
        this.concurrentRequestLimiter = new Semaphore(maxConcurrentRequestNumber);
    }

    static class PendingDeleteRequest {
        List<ObjectStorage.ObjectPath> deleteObjectPath;
        CompletableFuture<Void> future;

        public PendingDeleteRequest(List<ObjectStorage.ObjectPath> deleteObjectPath, CompletableFuture<Void> future) {
            this.deleteObjectPath = deleteObjectPath;
            this.future = future;
        }
    }

    @VisibleForTesting
    public int availablePermits() {
        return this.concurrentRequestLimiter.availablePermits();
    }

    /**
     * Batch delete objects, if the number of objects is greater than {@link #maxBatchSize}, they will be deleted in batches
     * The number of delete requests initiated at the same time will be limited by {@link #concurrentRequestLimiter}
     *
     * @param objectPaths list of object paths to delete
     * @param cf          CompletableFuture to complete when all objects are deleted, or an exception occurs
     */
    public void batchDeleteObjects(List<ObjectStorage.ObjectPath> objectPaths, CompletableFuture<Void> cf) {

        // batch delete objects
        ArrayList<CompletableFuture<Void>> subBatchCfList = new ArrayList<>();
        ArrayList<List<ObjectStorage.ObjectPath>> subBatchKeyList = new ArrayList<>();
        int startIndex = 0;
        while (startIndex < objectPaths.size()) {
            int endIndex = Math.min(startIndex + this.maxBatchSize, objectPaths.size());
            List<ObjectStorage.ObjectPath> subBatchList = objectPaths.subList(startIndex, endIndex);
            CompletableFuture<Void> subBatchCf = new CompletableFuture<>();
            subBatchCfList.add(subBatchCf);
            subBatchKeyList.add(subBatchList);
            startIndex = endIndex;
        }

        // submit delete requests or add to queue
        queueLock.lock();
        try {
            for (int i = 0; i < subBatchCfList.size(); i++) {
                List<ObjectStorage.ObjectPath> subBatchList = subBatchKeyList.get(i);
                CompletableFuture<Void> subBatchCf = subBatchCfList.get(i);
                // if there are pending requests, add to queue
                if (!deleteRequestQueue.isEmpty()) {
                    deleteRequestQueue.add(new PendingDeleteRequest(subBatchList, subBatchCf));
                    // try to submit pending requests
                } else if (!submitDeleteObjectsRequest(subBatchList, List.of(subBatchCf))) {
                    // if not submitted, add to queue
                    deleteRequestQueue.add(new PendingDeleteRequest(subBatchList, subBatchCf));
                }
            }
        } finally {
            queueLock.unlock();
        }

        CompletableFuture.allOf(subBatchCfList.toArray(new CompletableFuture[0]))
            .thenApply(nil -> {
                cf.complete(null);
                return null;
            }).exceptionally(cf::completeExceptionally);
    }

    private boolean submitDeleteObjectsRequest(List<ObjectStorage.ObjectPath> objectPaths, List<CompletableFuture<Void>> subBatchCf) {
        List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());
        TimerUtil timerUtil = new TimerUtil();
        if (!concurrentRequestLimiter.tryAcquire()) {
            return false;
        }
        deleteObjectsFunction.apply(objectKeys)
            .whenComplete((res, e) -> concurrentRequestLimiter.release())
            .thenAccept(nil -> {
                LOGGER.info("Delete objects finished, count: {}, cost: {}ms", objectKeys.size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
                S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                FutureUtil.complete(subBatchCf.iterator(), null);
                handleDeleteRequestQueue();
            }).exceptionally(ex -> {
                if (ex instanceof AbstractObjectStorage.DeleteObjectsException) {
                    AbstractObjectStorage.DeleteObjectsException deleteObjectsException = (AbstractObjectStorage.DeleteObjectsException) ex;
                    LOGGER.warn("Delete objects failed, count: {}, cost: {}, failedKeys: {}",
                        deleteObjectsException.getFailedKeys().size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS),
                        deleteObjectsException.getFailedKeys());
                } else {
                    S3OperationStats.getInstance().deleteObjectsStats(false)
                        .record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    LOGGER.info("Delete objects failed, count: {}, cost: {}, ex: {}",
                        objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), ex.getMessage());
                }
                FutureUtil.completeExceptionally(subBatchCf.iterator(), ex);
                handleDeleteRequestQueue();
                return null;
            });
        return true;
    }

    private void handleDeleteRequestQueue() {
        List<PendingDeleteRequest> readyToSubmitReq = new ArrayList<>();
        queueLock.lock();
        try {
            int accumulatedSize = 0;
            while (true) {
                if (concurrentRequestLimiter.availablePermits() > 0) {
                    PendingDeleteRequest pendingDeleteRequest = deleteRequestQueue.peek();
                    if (pendingDeleteRequest == null) {
                        break;
                    }
                    if (accumulatedSize + pendingDeleteRequest.deleteObjectPath.size() > maxBatchSize) {
                        break;
                    } else {
                        accumulatedSize += pendingDeleteRequest.deleteObjectPath.size();
                        readyToSubmitReq.add(deleteRequestQueue.poll());
                    }
                } else {
                    break;
                }
            }
            if (!readyToSubmitReq.isEmpty()) {
                submitPendingRequests(readyToSubmitReq);
            }
        } finally {
            queueLock.unlock();
        }
    }

    private void submitPendingRequests(List<PendingDeleteRequest> pendingRequests) {
        // merge all delete requests
        List<ObjectStorage.ObjectPath> combinedPaths = new ArrayList<>();
        List<CompletableFuture<Void>> combinedFutures = new ArrayList<>();

        for (PendingDeleteRequest request : pendingRequests) {
            combinedPaths.addAll(request.deleteObjectPath);
            combinedFutures.add(request.future);
        }

        // submit delete requests
        boolean isSubmit = submitDeleteObjectsRequest(combinedPaths, combinedFutures);

        // if not submitted, add back to queue
        if (!isSubmit) {
            ListIterator<PendingDeleteRequest> iterator = pendingRequests.listIterator(pendingRequests.size());
            while (iterator.hasPrevious()) {
                PendingDeleteRequest pendingRequest = iterator.previous();
                deleteRequestQueue.addFirst(pendingRequest);
            }
        }
    }

}
