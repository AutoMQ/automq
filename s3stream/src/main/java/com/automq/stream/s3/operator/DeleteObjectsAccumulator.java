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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
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
    private static final long DELETE_OBJECTS_RETRY_BASE_DELAY_MS = 1000;
    private static final long DELETE_OBJECTS_RETRY_MAX_DELAY_MS = 30 * 1000;
    private static final long DELETE_OPERATION_LOG_INTERVAL = 60 * 1000;
    private final Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction;
    private final ConcurrentLinkedDeque<PendingDeleteRequest> deleteRequestQueue = new ConcurrentLinkedDeque<>();
    private final DeleteOperationSummary deleteOperationSummary = new DeleteOperationSummary();
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
        int retryAttempt;

        public PendingDeleteRequest(List<ObjectStorage.ObjectPath> deleteObjectPath, CompletableFuture<Void> future) {
            this(deleteObjectPath, future, 0);
        }

        public PendingDeleteRequest(List<ObjectStorage.ObjectPath> deleteObjectPath, CompletableFuture<Void> future,
            int retryAttempt) {
            this.deleteObjectPath = deleteObjectPath;
            this.future = future;
            this.retryAttempt = retryAttempt;
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
                } else if (!submitDeleteObjectsRequest(List.of(new PendingDeleteRequest(subBatchList, subBatchCf)))) {
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

    private boolean submitDeleteObjectsRequest(List<PendingDeleteRequest> requests) {
        if (!concurrentRequestLimiter.tryAcquire()) {
            return false;
        }
        List<ObjectStorage.ObjectPath> objectPaths = requests.stream()
            .flatMap(request -> request.deleteObjectPath.stream())
            .collect(Collectors.toList());
        List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());
        TimerUtil timerUtil = new TimerUtil();
        deleteObjectsFunction.apply(objectKeys).whenComplete((res, e) -> concurrentRequestLimiter.release())
            .thenAccept(nil -> {
                deleteOperationSummary.recordDeleteOperation(objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), true, Collections.emptyList());
                S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                completeRequests(requests);
                handleDeleteRequestQueue();
            }).exceptionally(ex -> {
                Throwable cause = ex.getCause();
                if (cause instanceof DeleteObjectsException) {
                    DeleteObjectsException deleteObjectsException = (DeleteObjectsException) cause;
                    deleteOperationSummary.recordDeleteOperation(objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), false, deleteObjectsException.getFailedKeys());
                    handleDeleteObjectsException(requests, deleteObjectsException);
                } else {
                    S3OperationStats.getInstance().deleteObjectsStats(false).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
                    deleteOperationSummary.recordDeleteOperation(objectKeys.size(), timerUtil.elapsedAs(TimeUnit.NANOSECONDS), false, Collections.emptyList());
                    completeRequestsExceptionally(requests, ex);
                }
                handleDeleteRequestQueue();
                return null;
            });
        return true;
    }

    private void completeRequests(List<PendingDeleteRequest> requests) {
        FutureUtil.complete(requests.stream().map(request -> request.future).iterator(), null);
    }

    private void completeRequestsExceptionally(List<PendingDeleteRequest> requests, Throwable ex) {
        FutureUtil.completeExceptionally(requests.stream().map(request -> request.future).iterator(), ex);
    }

    private void handleDeleteObjectsException(List<PendingDeleteRequest> requests,
        DeleteObjectsException ex) {
        Set<String> retriableKeys = ex.getRetriableKeys().keySet();
        Set<String> failedKeys = ex.getFailedKeyErrors().keySet();
        for (PendingDeleteRequest request : requests) {
            List<ObjectStorage.ObjectPath> requestRetriablePaths = request.deleteObjectPath.stream()
                .filter(path -> retriableKeys.contains(path.key()))
                .collect(Collectors.toList());
            Map<String, DeleteObjectError> requestFailedKeys = request.deleteObjectPath.stream()
                .filter(path -> failedKeys.contains(path.key()))
                .collect(Collectors.toMap(ObjectStorage.ObjectPath::key, path -> ex.getFailedKeyErrors().get(path.key())));

            if (requestRetriablePaths.isEmpty()) {
                if (!requestFailedKeys.isEmpty()) {
                    completeRequestWithFailedKeys(request.future, requestFailedKeys);
                } else {
                    request.future.complete(null);
                }
            } else if (requestFailedKeys.isEmpty()) {
                scheduleRetry(new PendingDeleteRequest(requestRetriablePaths, request.future, request.retryAttempt + 1));
            } else {
                CompletableFuture<Void> retryFuture = new CompletableFuture<>();
                retryFuture.whenComplete((nil, retryEx) -> {
                    Map<String, DeleteObjectError> failedKeyErrors = new HashMap<>(requestFailedKeys);
                    DeleteObjectsException retryDeleteException = deleteObjectsException(retryEx);
                    if (retryDeleteException != null) {
                        failedKeyErrors.putAll(retryDeleteException.getFailedKeyErrors());
                    }
                    completeRequestWithFailedKeys(request.future, failedKeyErrors);
                });
                scheduleRetry(new PendingDeleteRequest(requestRetriablePaths, retryFuture, request.retryAttempt + 1));
            }
        }
    }

    private void scheduleRetry(PendingDeleteRequest request) {
        Threads.COMMON_SCHEDULER.schedule(() -> submitOrQueue(request), retryDelayMs(request.retryAttempt), TimeUnit.MILLISECONDS);
    }

    private long retryDelayMs(int retryAttempt) {
        int shift = Math.max(0, Math.min(retryAttempt - 1, 5));
        return Math.min(DELETE_OBJECTS_RETRY_BASE_DELAY_MS << shift, DELETE_OBJECTS_RETRY_MAX_DELAY_MS);
    }

    private DeleteObjectsException deleteObjectsException(Throwable ex) {
        if (ex instanceof DeleteObjectsException deleteObjectsException) {
            return deleteObjectsException;
        }
        if (ex != null && ex.getCause() instanceof DeleteObjectsException deleteObjectsException) {
            return deleteObjectsException;
        }
        return null;
    }

    private void completeRequestWithFailedKeys(CompletableFuture<Void> future, Map<String, DeleteObjectError> failedKeys) {
        future.completeExceptionally(new DeleteObjectsException(
            "Failed to delete objects", Collections.emptySet(), Collections.emptyMap(), failedKeys));
    }

    private void submitOrQueue(PendingDeleteRequest request) {
        queueLock.lock();
        try {
            if (!deleteRequestQueue.isEmpty() || !submitDeleteObjectsRequest(List.of(request))) {
                deleteRequestQueue.add(request);
            }
        } finally {
            queueLock.unlock();
        }
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
        // submit delete requests
        boolean isSubmit = submitDeleteObjectsRequest(pendingRequests);

        // if not submitted, add back to queue
        if (!isSubmit) {
            ListIterator<PendingDeleteRequest> iterator = pendingRequests.listIterator(pendingRequests.size());
            while (iterator.hasPrevious()) {
                PendingDeleteRequest pendingRequest = iterator.previous();
                deleteRequestQueue.addFirst(pendingRequest);
            }
        }
    }

    private static class DeleteOperationSummary {
        int totalCount;
        int failureCount;
        long totalCost;
        long lastLogTime = System.currentTimeMillis();

        public synchronized void recordDeleteOperation(int count, long cost, boolean success, List<String> failedKeys) {
            totalCount += count;
            totalCost += cost;
            if (!success) {
                failureCount += failedKeys.size();
            }
            long currentTime = System.currentTimeMillis();
            long realDeleteOperationLogInterval = currentTime - lastLogTime;
            if (realDeleteOperationLogInterval > DELETE_OPERATION_LOG_INTERVAL) {
                logDeleteOperationSummary(realDeleteOperationLogInterval);
                clearDeleteOperationSummary(currentTime);
            }
        }

        private void logDeleteOperationSummary(long realDeleteOperationLogInterval) {
            LOGGER.info("Summary of delete operations in the past {}ms: Total count {}, Failure: {}, Total cost: {}ns",
                realDeleteOperationLogInterval, totalCount, failureCount, totalCost);
        }

        private void clearDeleteOperationSummary(long currentTime) {
            totalCount = 0;
            totalCost = 0;
            failureCount = 0;
            lastLogTime = currentTime;
        }
    }
}
