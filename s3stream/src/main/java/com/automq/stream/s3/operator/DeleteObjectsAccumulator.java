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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metrics.TimerUtil;
import com.automq.stream.s3.metrics.stats.S3OperationStats;
import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DeleteObjectsAccumulator implements Runnable {
    static final Logger LOGGER = LoggerFactory.getLogger(DeleteObjectsAccumulator.class);
    public static final int DEFAULT_DELETE_OBJECTS_MAX_BATCH_TIME_MS = 1000;
    public static final int DEFAULT_DELETE_OBJECTS_MAX_BATCH_SIZE = 1000;
    public static final int DEFAULT_DELETE_OBJECTS_API_RATE = -1;
    private final ExecutorService batchDeleteCheckExecutor = Threads.newFixedThreadPoolWithMonitor(1,
        "s3-batch-delete-executor", true, LOGGER);
    private final Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction;
    private final BlockingQueue<PendingDeleteRequest> deleteBatchQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private RateLimiter deleteRateLimiter;

    private long maxBatchTimeMs;
    private long maxBatchSize;
    private long maxDeleteRate;

    public DeleteObjectsAccumulator(Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction) {
        this(DEFAULT_DELETE_OBJECTS_MAX_BATCH_TIME_MS,
            DEFAULT_DELETE_OBJECTS_MAX_BATCH_SIZE,
            DEFAULT_DELETE_OBJECTS_API_RATE,
            deleteObjectsFunction);
    }

    public DeleteObjectsAccumulator(long maxBatchTimeMs,
                                    long maxBatchSize,
                                    long maxDeleteRate,
                                    Function<List<String>, CompletableFuture<Void>> deleteObjectsFunction) {
        this.deleteObjectsFunction = deleteObjectsFunction;
        this.maxBatchTimeMs = maxBatchTimeMs;
        this.maxBatchSize = maxBatchSize;
        this.maxDeleteRate = maxDeleteRate;
        if (this.maxDeleteRate != -1) {
            deleteRateLimiter = RateLimiter.create(maxDeleteRate);
        } else {
            deleteRateLimiter = RateLimiter.create(Double.MAX_VALUE);
        }
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
                                            List<CompletableFuture<Void>> futures) {
        if (this.deleteRateLimiter != null) {
            this.deleteRateLimiter.acquire();
        }

        List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());

        TimerUtil timerUtil = new TimerUtil();

        deleteObjectsFunction.apply(objectKeys).thenAccept(nil -> {
            LOGGER.info("Delete objects finished, count: {}, cost: {}ms", objectKeys.size(), timerUtil.elapsedAs(TimeUnit.MILLISECONDS));
            S3OperationStats.getInstance().deleteObjectsStats(true).record(timerUtil.elapsedAs(TimeUnit.NANOSECONDS));
            FutureUtil.complete(futures.iterator(), null);
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
            FutureUtil.completeExceptionally(futures.iterator(), ex);

            return null;
        });
    }

    public void run() {
        ArrayList<ObjectStorage.ObjectPath> deleteKeys = new ArrayList<>(1000);
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();

        while (running.get()) {
            PendingDeleteRequest item = null;
            try {
                item = deleteBatchQueue.poll(this.maxBatchTimeMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            // poll max time out but have remaining in batch.
            if (item == null) {
                if (!deleteKeys.isEmpty()) {
                    submitDeleteObjectsRequest(deleteKeys, futures);
                    deleteKeys = new ArrayList<>(1000);
                    futures = new ArrayList<>();
                }

                continue;
            }

            // enqueue batch
            deleteKeys.addAll(item.deleteObjectPath);
            futures.add(item.future);

            // submit batch
            if (deleteKeys.size() >= this.maxBatchSize) {
                submitDeleteObjectsRequest(deleteKeys, futures);
                deleteKeys = new ArrayList<>(1000);
                futures = new ArrayList<>();
            }
        }
    }

    public long getMaxDeleteRate() {
        return maxDeleteRate;
    }

    public void setMaxDeleteRate(long maxDeleteRate) {
        this.maxDeleteRate = maxDeleteRate;
        this.deleteRateLimiter.setRate(maxDeleteRate);
    }

    public long getMaxBatchTimeMs() {
        return maxBatchTimeMs;
    }

    public void setMaxBatchTimeMs(long maxBatchTimeMs) {
        this.maxBatchTimeMs = maxBatchTimeMs;
    }

    public long getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(long maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public void stop() {
        this.running.set(false);
        this.batchDeleteCheckExecutor.shutdownNow();
    }

    /**
     * batchOrSubmitDeleteRequests
     * If deleteOptions expect no batch just do normal deleteObjects immediately.
     * else the time-based and size-based logic will trigger the final delete logic.
     */
    public void batchOrSubmitDeleteRequests(ObjectStorage.DeleteOptions options,
                                            List<ObjectStorage.ObjectPath> objectPaths,
                                            CompletableFuture<Void> ioCf) {
        if (options.canWaitForBatch()) {
            deleteBatchQueue.add(new PendingDeleteRequest(objectPaths, ioCf));
            return;
        }

        submitDeleteObjectsRequest(objectPaths, List.of(ioCf));
    }

}
