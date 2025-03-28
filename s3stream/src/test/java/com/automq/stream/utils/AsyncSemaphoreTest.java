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

package com.automq.stream.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AsyncSemaphoreTest {

    private AsyncSemaphore semaphore;

    @BeforeEach
    void setup() {
        semaphore = new AsyncSemaphore(10); // Initialize with 10 permits
    }

    @Test
    public void testAcquireWithSufficientPermits() throws ExecutionException, InterruptedException, TimeoutException {
        // Test that permits can be acquired when sufficient permits are available
        AtomicInteger executedTaskNum = new AtomicInteger(0);
        
        CompletableFuture<Void> task1 = new CompletableFuture<>();
        boolean acquired1 = semaphore.acquire(3, () -> {
            executedTaskNum.addAndGet(1);
            return task1;
        }, Runnable::run);
        assertTrue(acquired1); // Task can acquire permits
        assertEquals(7, semaphore.permits()); // Permits should be decreased
        assertEquals(1, executedTaskNum.get()); // Task should execute
        assertFalse(semaphore.requiredRelease()); // No release required

        CompletableFuture<Void> task2 = new CompletableFuture<>();
        boolean acquired2 = semaphore.acquire(3, () -> {
            executedTaskNum.addAndGet(1);
            return task2;
        }, Runnable::run);
        assertTrue(acquired2); // Task can acquire permits
        assertEquals(4, semaphore.permits()); // Permits should be decreased
        assertEquals(2, executedTaskNum.get()); // Task should execute
        assertFalse(semaphore.requiredRelease()); // No release required

        CompletableFuture<Void> task3 = new CompletableFuture<>();
        boolean acquired3 = semaphore.acquire(4, () -> {
            executedTaskNum.addAndGet(1);
            return task3;
        }, Runnable::run);
        assertTrue(acquired3); // Task can acquire permits
        assertEquals(0, semaphore.permits()); // Permits should be decreased
        assertEquals(3, executedTaskNum.get()); // Task should execute
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        // Release permits and ensure they are restored
        task1.complete(null); // Release permits from task1
        assertEquals(3, semaphore.permits());
        task2.complete(null); // Release permits from task2
        assertEquals(6, semaphore.permits());
        task3.complete(null); // Release permits from task3
        assertEquals(10, semaphore.permits());
    }

    @Test
    public void testAcquireDecreaseToNegativePermits() throws ExecutionException, InterruptedException, TimeoutException {
        // Test that permits can decrease to negative values
        AtomicInteger executedTaskNum = new AtomicInteger(0);

        CompletableFuture<Void> task1 = new CompletableFuture<>();
        boolean acquired1 = semaphore.acquire(15, () -> {
            executedTaskNum.addAndGet(1);
            return task1;
        }, Runnable::run);
        assertTrue(acquired1); // Task can acquire permits
        assertEquals(-5, semaphore.permits()); // Permits should be negative
        assertEquals(1, executedTaskNum.get()); // Task should execute
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        // Test that a second task is queued when permits are negative
        CompletableFuture<Void> task2 = new CompletableFuture<>();
        boolean acquired2 = semaphore.acquire(5, () -> {
            executedTaskNum.addAndGet(1);
            return task2;
        }, Runnable::run);
        assertFalse(acquired2); // Task should be queued
        assertEquals(-5, semaphore.permits()); // Permits should remain unchanged
        assertEquals(1, executedTaskNum.get()); // Task should not execute yet
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        // Release permits and ensure the queued task executes
        task1.complete(null); // Release permits from task1
        assertEquals(5, semaphore.permits()); // Permits should be restored and acquired by task2
        assertEquals(2, executedTaskNum.get()); // Task2 should execute
        assertFalse(semaphore.requiredRelease()); // No release required

        task2.complete(null); // Release permits from task2
        assertEquals(10, semaphore.permits()); // Permits should be restored
    }

    @Test
    public void testReleaseWithoutLeaks() throws ExecutionException, InterruptedException, TimeoutException {
        // Test that all releases are correctly accounted for, even if there are exceptions when acquiring
        AtomicInteger executedTaskNum = new AtomicInteger(0);

        CompletableFuture<Void> task1 = new CompletableFuture<>();
        boolean acquired1 = semaphore.acquire(5, () -> {
            executedTaskNum.addAndGet(1);
            return task1;
        }, Runnable::run);
        assertTrue(acquired1); // Task can acquire permits
        assertEquals(5, semaphore.permits()); // Permits should be decreased
        assertEquals(1, executedTaskNum.get()); // Task should execute
        assertFalse(semaphore.requiredRelease()); // No release required

        CompletableFuture<Void> task2 = new CompletableFuture<>();
        boolean acquired2 = semaphore.acquire(10, () -> {
            executedTaskNum.addAndGet(1);
            return task2;
        }, Runnable::run);
        assertTrue(acquired2); // Task can acquire permits
        assertEquals(-5, semaphore.permits()); // Permits should be negative
        assertEquals(2, executedTaskNum.get()); // Task should execute
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        RuntimeException task3Exception = new RuntimeException("Task 3 exception");
        CompletableFuture<Void> task3 = new CompletableFuture<>();
        boolean acquired3 = semaphore.acquire(1, () -> {
            executedTaskNum.addAndGet(1);
            throw task3Exception;
        }, Runnable::run);
        assertFalse(acquired3); // Task should be queued
        assertEquals(-5, semaphore.permits()); // Permits should remain unchanged
        assertEquals(2, executedTaskNum.get()); // Task should not execute yet
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        // Release permits from task1
        task1.complete(null);
        assertEquals(0, semaphore.permits()); // Permits should be restored and not acquired by task3
        assertEquals(2, executedTaskNum.get()); // Task3 should not execute
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        // Release permits from task2
        task2.complete(null);
        // Permits should be restored and acquired by task3, but task3 throw an exception, so the permits should be restored
        assertEquals(10, semaphore.permits());
        assertEquals(3, executedTaskNum.get()); // Task3 should execute
        assertFalse(semaphore.requiredRelease()); // No release required

        // Release permits from task3
        task3.completeExceptionally(task3Exception);
        assertEquals(10, semaphore.permits()); // Permits should be restored even if task3 completes exceptionally
    }

    @Test
    public void testSequentialAcquireOrder() throws ExecutionException, InterruptedException, TimeoutException {
        // Test that sequentially issued acquire requests are executed in order after permits become sufficient
        AtomicInteger executionOrder = new AtomicInteger(0);

        CompletableFuture<Void> task1 = new CompletableFuture<>();
        boolean acquired1 = semaphore.acquire(12, () -> {
            executionOrder.compareAndSet(0, 1);
            return task1;
        }, Runnable::run);
        assertTrue(acquired1); // Task 1 can acquire permits
        assertEquals(-2, semaphore.permits()); // Permits should be negative
        assertEquals(1, executionOrder.get()); // Task 1 should execute
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        CompletableFuture<Void> task2 = new CompletableFuture<>();
        boolean acquired2 = semaphore.acquire(6, () -> {
            executionOrder.compareAndSet(1, 2);
            return task2;
        }, Runnable::run);
        assertFalse(acquired2); // Task 2 should be queued
        assertEquals(-2, semaphore.permits()); // Permits should remain unchanged
        assertEquals(1, executionOrder.get()); // Task 2 should not execute yet
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        CompletableFuture<Void> task3 = new CompletableFuture<>();
        boolean acquired3 = semaphore.acquire(8, () -> {
            executionOrder.compareAndSet(2, 3);
            return task3;
        }, Runnable::run);
        assertFalse(acquired3); // Task 3 should be queued
        assertEquals(-2, semaphore.permits()); // Permits should remain unchanged
        assertEquals(1, executionOrder.get()); // Task 3 should not execute yet
        assertTrue(semaphore.requiredRelease()); // Release required due to non-positive permits

        // Release permits from task1 and ensure task2 executes first
        task1.complete(null);
        assertEquals(4, semaphore.permits()); // Permits should be restored and acquired by task2
        assertEquals(2, executionOrder.get()); // Task 2 should execute first
        assertTrue(semaphore.requiredRelease()); // Release required due to non-empty queue

        // Release permits from task2 and ensure task3 executes next
        task2.complete(null);
        assertEquals(2, semaphore.permits()); // Permits should be restored and acquired by task3
        assertEquals(3, executionOrder.get()); // Task 3 should execute next
        assertFalse(semaphore.requiredRelease()); // No release required

        // Release permits from task3
        task3.complete(null);
        assertEquals(10, semaphore.permits()); // Permits should be restored
    }
}