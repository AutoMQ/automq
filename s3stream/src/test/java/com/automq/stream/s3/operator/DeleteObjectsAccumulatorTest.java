/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.operator;

import com.automq.stream.utils.FutureUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
public class DeleteObjectsAccumulatorTest {

    private List<ObjectStorage.ObjectPath> mockObjectPath(int number, short bucketId, String key) {
        return IntStream.range(0, number)
            .mapToObj(i -> new ObjectStorage.ObjectPath(bucketId, key + "_" + i))
            .collect(Collectors.toList());
    }

    @Test
    void testNormalSmallTrafficDeleteCanPass() {
        Function<List<String>, CompletableFuture<Void>> deleteFunction
            = path -> CompletableFuture.completedFuture(null);

        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(deleteFunction);
        accumulator.start();

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(100, (short) 0, "testNormalNoBatchDelete");
        accumulator.batchOrSubmitDeleteRequests(objectPaths, completableFuture);

        completableFuture.join();
    }

    @Test
    void testBatchSmallBatchDelete() {
        int delayMs = ThreadLocalRandom.current().nextInt(100);
        AtomicInteger totalDeleteObjectNumber = new AtomicInteger();
        Function<List<String>, CompletableFuture<Void>> deleteFunction = path -> {
            totalDeleteObjectNumber.addAndGet(path.size());
            return new CompletableFuture<Void>().completeOnTimeout(null, delayMs, TimeUnit.MILLISECONDS);
        };

        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(deleteFunction);
        accumulator.start();


        List<CompletableFuture<Void>> allIoCf = new ArrayList<>();

        int ioNumber = 2000;
        int ioSize = 100;

        for (int i = 0; i < ioNumber; i++) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            allIoCf.add(completableFuture);

            List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i);

            accumulator.batchOrSubmitDeleteRequests(objectPaths, completableFuture);
        }

        try {
            CompletableFuture.allOf(allIoCf.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        assertEquals(ioSize * ioNumber, totalDeleteObjectNumber.get());
    }

    @Test
    void testDeleteRequestExceededLimitCanRecoverWhenRequestReturned() throws InterruptedException {
        AtomicInteger totalDeleteObjectNumber = new AtomicInteger();
        Set<String> allDeleteKeys = new ConcurrentSkipListSet<>();

        CompletableFuture<Void> waitForDone = new CompletableFuture<>();

        Function<List<String>, CompletableFuture<Void>> deleteFunction = path -> {
            totalDeleteObjectNumber.addAndGet(path.size());
            allDeleteKeys.addAll(path);

            CompletableFuture<Void> future = new CompletableFuture<>();
            FutureUtil.propagate(waitForDone, future);
            return future;
        };

        DeleteObjectsAccumulator accumulator =
            new DeleteObjectsAccumulator(1000, 10, deleteFunction);
        accumulator.start();

        List<CompletableFuture<Void>> allIoCf = new ArrayList<>();

        int ioNumber = 1000;
        int ioSize = 1;

        Set<String> allKeys = new HashSet<>();
        for (int i = 0; i < ioNumber; i++) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            allIoCf.add(completableFuture);

            List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i);

            List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());
            allKeys.addAll(objectKeys);

            accumulator.batchOrSubmitDeleteRequests(objectPaths, completableFuture);
            TimeUnit.MICROSECONDS.sleep(1);
        }

        assertEquals(0, accumulator.availablePermits());
        for (CompletableFuture<Void> cf : allIoCf) {
            assertFalse(cf.isDone());
        }

        // trigger done
        waitForDone.complete(null);

        // total number of io request is small then one batch but can be still completed.
        try {
            CompletableFuture.allOf(allIoCf.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        // all path deleted.
        assertEquals(ioSize * ioNumber, totalDeleteObjectNumber.get());
        assertEquals(allKeys, allDeleteKeys);
    }

    @Test
    void testDeleteExceptionPassToIoCompletableFuture() {
        int ioNumber = 4000;
        int ioSize = 100;

        Exception e = new RuntimeException("mock exception");

        Map<String, CompletableFuture<Void>> exceptionPath2Cf = new ConcurrentHashMap<>();

        AtomicInteger totalDeleteObjectNumber = new AtomicInteger();
        Function<List<String>, CompletableFuture<Void>> deleteFunction = path -> {
            if (totalDeleteObjectNumber.addAndGet(path.size()) > ioNumber / 2 * ioSize) {
                CompletableFuture<Void> exceptionCf = CompletableFuture.failedFuture(e);
                for (String s : path) {
                    exceptionPath2Cf.put(s, exceptionCf);
                }
                return exceptionCf;
            }

            return CompletableFuture.completedFuture(null);
        };

        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(deleteFunction);
        accumulator.start();

        Map<CompletableFuture<Void>, List<String>> taskAndCf = new HashMap<>();

        CountDownLatch latch = new CountDownLatch(ioNumber);

        for (int i = 0; i < ioNumber; i++) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFuture.whenComplete((Void, __) -> latch.countDown());

            List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i);
            List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());

            taskAndCf.put(completableFuture, objectKeys);

            accumulator.batchOrSubmitDeleteRequests(objectPaths, completableFuture);
        }


        try {
            latch.await();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        taskAndCf.forEach((cf, path) -> {
            assertTrue(cf.isDone());

            if (cf.isCompletedExceptionally()) {
                try {
                    cf.join();
                } catch (Exception ex) {
                    // check exception is get
                    assertEquals(e, ex.getCause());
                }

                // check the cf are the same
                for (String s : path) {
                    assertTrue(exceptionPath2Cf.containsKey(s));
                }
            } else {
                for (String s : path) {
                    assertFalse(exceptionPath2Cf.containsKey(s));
                }
            }
        });


        assertEquals(ioSize * ioNumber, totalDeleteObjectNumber.get());
    }

    @Test
    void testHighTrafficBatchDelete() {
        AtomicInteger totalDeleteObjectNumber = new AtomicInteger();
        int delayMs = ThreadLocalRandom.current().nextInt(100);
        Function<List<String>, CompletableFuture<Void>> deleteFunction = path -> {
            totalDeleteObjectNumber.addAndGet(path.size());
            return new CompletableFuture<Void>().completeOnTimeout(null, delayMs, TimeUnit.MILLISECONDS);
        };

        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(deleteFunction);
        accumulator.start();

        Queue<CompletableFuture<Void>> allIoCf = new ConcurrentLinkedQueue<>();

        int ioNumber = 10000;
        int ioSize = 1;
        int threadNumber = 5;

        CountDownLatch latch = new CountDownLatch(threadNumber * ioSize * ioNumber);

        ExecutorService executorService = Executors.newFixedThreadPool(threadNumber);
        for (int j = 0; j < 5; j++) {
            int finalJ = j;
            executorService.submit(() -> {
                for (int i = 0; i < ioNumber; i++) {
                    CompletableFuture<Void> completableFuture = new CompletableFuture<>();

                    // add timeout for check request can be done.
                    completableFuture.orTimeout(2, TimeUnit.MINUTES);
                    allIoCf.add(completableFuture);

                    List<ObjectStorage.ObjectPath> objectPaths =
                        mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i + "_" + finalJ);

                    accumulator.batchOrSubmitDeleteRequests(objectPaths, completableFuture);
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            CompletableFuture.allOf(allIoCf.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        for (CompletableFuture<Void> voidCompletableFuture : allIoCf) {
            assertTrue(voidCompletableFuture.isDone());
            assertFalse(voidCompletableFuture.isCompletedExceptionally());
        }

        assertEquals(threadNumber * ioSize * ioNumber, totalDeleteObjectNumber.get());

        executorService.shutdown();
    }

}
