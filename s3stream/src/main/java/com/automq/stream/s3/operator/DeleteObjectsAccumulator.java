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
import com.automq.stream.utils.Threads;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DeleteObjectsAccumulator implements Runnable {
    static final Logger LOGGER = LoggerFactory.getLogger(DeleteObjectsAccumulator.class);
    public static final int DEFAULT_DELETE_OBJECTS_MAX_BATCH_SIZE = 1000;
    public static final int DEFAULT_DELETE_OBJECTS_MAX_CONCURRENT_REQUEST_NUMBER = 100;
    private final ExecutorService batchDeleteCheckExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-batch-delete-executor", true, LOGGER);
    private final Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction;
    private final BlockingQueue<PendingDeleteRequest> deleteBatchQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

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

    public void start() {
        this.batchDeleteCheckExecutor.submit(this);
    }

    private void submitDeleteObjectsRequest(List<ObjectStorage.ObjectPath> objectPaths,
                                            CompletableFuture<Void> subBatchCf) throws InterruptedException {
        List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());

        TimerUtil timerUtil = new TimerUtil();

        concurrentRequestLimiter.acquire();
        deleteObjectsFunction.apply(objectKeys).whenComplete((res, e) -> {
            concurrentRequestLimiter.release();
        }).thenAccept(nil -> {
            LOGGER.info("Delete objects finished, count: {}, cost: {}ms", objectKeys.size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
            S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            subBatchCf.complete(null);
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
            subBatchCf.completeExceptionally(ex);

            return null;
        });
    }

    public void run() {
        while (running.get()) {
            PendingDeleteRequest item;
            try {
                item = deleteBatchQueue.poll(500, TimeUnit.MICROSECONDS);
                if (item == null) {
                    continue;
                }
                ArrayList<CompletableFuture<Void>> subBatchCfList = new ArrayList<>();
                int startIndex = 0;
                while (startIndex < item.deleteObjectPath.size()) {
                    int endIndex = Math.min(startIndex + this.maxBatchSize, item.deleteObjectPath.size());
                    List<ObjectStorage.ObjectPath> subBatchList = item.deleteObjectPath.subList(startIndex, endIndex);
                    CompletableFuture<Void> subBatchCf = new CompletableFuture<>();
                    subBatchCfList.add(subBatchCf);
                    submitDeleteObjectsRequest(subBatchList, subBatchCf);
                    startIndex = endIndex;
                }
                CompletableFuture.allOf(subBatchCfList.toArray(new CompletableFuture[0]))
                    .thenApply(nil -> {
                        item.future.complete(null);
                        return null;
                    }).exceptionally(ex -> item.future.completeExceptionally(ex));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void stop() {
        this.running.set(false);
        this.batchDeleteCheckExecutor.shutdownNow();
    }

    @VisibleForTesting
    public int availablePermits() {
        return this.concurrentRequestLimiter.availablePermits();
    }

    /**
     * batchOrSubmitDeleteRequests
     * the request will be limited by max concurrent request here
     */
    public void batchOrSubmitDeleteRequests(List<ObjectStorage.ObjectPath> objectPaths,
                                            CompletableFuture<Void> ioCf) {
        deleteBatchQueue.add(new PendingDeleteRequest(objectPaths, ioCf));
    }

}
