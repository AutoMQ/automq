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

import com.automq.stream.utils.FutureUtil;
import com.automq.stream.utils.Threads;

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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.automq.stream.s3.operator.DeleteObjectsAccumulator.LOGGER;
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
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(100, (short) 0, "testNormalNoBatchDelete");
        accumulator.batchDeleteObjects(objectPaths, completableFuture);
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
        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(1000, 100, deleteFunction);
        List<CompletableFuture<Void>> allIoCf = new ArrayList<>();

        int ioNumber = 2000;
        int ioSize = 100;

        for (int i = 0; i < ioNumber; i++) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            allIoCf.add(completableFuture);

            List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i);

            accumulator.batchDeleteObjects(objectPaths, completableFuture);
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
            new DeleteObjectsAccumulator(deleteFunction);

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

            accumulator.batchDeleteObjects(objectPaths, completableFuture);
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

        Map<CompletableFuture<Void>, List<String>> taskAndCf = new HashMap<>();

        CountDownLatch latch = new CountDownLatch(ioNumber);

        for (int i = 0; i < ioNumber; i++) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFuture.whenComplete((Void, __) -> latch.countDown());

            List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i);
            List<String> objectKeys = objectPaths.stream().map(ObjectStorage.ObjectPath::key).collect(Collectors.toList());

            taskAndCf.put(completableFuture, objectKeys);
            accumulator.batchDeleteObjects(objectPaths, completableFuture);
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

        // max batch size is 100, max concurrent request number is 10
        int maxBathSize = 100;
        int maxConcurrentRequestNumber = 10;
        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(maxBathSize, maxConcurrentRequestNumber, deleteFunction);

        Queue<CompletableFuture<Void>> allIoCf = new ConcurrentLinkedQueue<>();

        // submit 20 batch delete requests at the same time, each batch delete request contains 1000 objects
        int batchSize = 1000;
        int batchNumber = 20;

        CountDownLatch latch = new CountDownLatch(batchNumber);

        ExecutorService executorService = Threads.newFixedFastThreadLocalThreadPoolWithMonitor(batchNumber, "delete-obj-accumulator-thread", true, LOGGER);
        for (int j = 0; j < batchNumber; j++) {
            int finalJ = j;
            executorService.submit(() -> {
                List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(batchSize, (short) 0, "testBatchSmallBatchDelete" + "_" + finalJ);
                CompletableFuture<Void> completableFuture = new CompletableFuture<>();
                completableFuture.orTimeout(30, TimeUnit.SECONDS);
                allIoCf.add(completableFuture);
                accumulator.batchDeleteObjects(objectPaths, completableFuture);
                latch.countDown();
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
        assertEquals(batchNumber * batchSize, totalDeleteObjectNumber.get());
        executorService.shutdown();
    }

    @Test
    void testBatchDeleteOrderPreservation() throws InterruptedException {
        int delayMs = ThreadLocalRandom.current().nextInt(10);
        AtomicInteger totalDeleteObjectNumber = new AtomicInteger();
        AtomicInteger totalCallDeleteFun = new AtomicInteger();
        List<List<String>> capturedArguments = new CopyOnWriteArrayList<>();

        Function<List<String>, CompletableFuture<Void>> deleteFunction = path -> {
            totalDeleteObjectNumber.addAndGet(path.size());
            totalCallDeleteFun.getAndIncrement();
            capturedArguments.add(new ArrayList<>(path));
            return new CompletableFuture<Void>().completeOnTimeout(null, delayMs, TimeUnit.MILLISECONDS);
        };

        DeleteObjectsAccumulator accumulator = new DeleteObjectsAccumulator(100, 10, deleteFunction);
        List<CompletableFuture<Void>> allIoCf = new ArrayList<>();

        int ioNumber = 2000;
        int ioSize = 20;

        for (int i = 0; i < ioNumber; i++) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            allIoCf.add(completableFuture);

            List<ObjectStorage.ObjectPath> objectPaths = mockObjectPath(ioSize, (short) 0, "testBatchSmallBatchDelete" + "_" + i);
            Thread.sleep(2);
            accumulator.batchDeleteObjects(objectPaths, completableFuture);
        }

        try {
            CompletableFuture.allOf(allIoCf.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        assertEquals(ioSize * ioNumber, totalDeleteObjectNumber.get());

        // Verify the order within each batch
        for (List<String> batch : capturedArguments) {
            List<Integer> indices = batch.stream()
                .map(key -> Integer.parseInt(key.split("_")[2]))
                .collect(Collectors.toList());
            for (int i = 0; i < ioSize; i++) {
                assertEquals(i, indices.get(i));
            }
        }

        // Verify the overall order of batches
        List<Integer> allIndices = capturedArguments.stream()
            .flatMap(List::stream)
            .map(key -> Integer.parseInt(key.split("_")[1]))
            .collect(Collectors.toList());
        int index = 0;
        for (int i = 0; i < ioNumber; i++, index += ioSize) {
            assertEquals(i, allIndices.get(index));
        }
    }

}
