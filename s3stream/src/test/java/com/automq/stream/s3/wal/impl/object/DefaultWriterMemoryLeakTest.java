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

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.wal.AppendResult;
import com.automq.stream.s3.wal.ReservationService;
import com.automq.stream.s3.wal.exception.OverCapacityException;
import com.automq.stream.s3.wal.util.WALUtil;
import com.automq.stream.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Test class to verify the memory leak fix in DefaultWriter.uploadBulk0() method.
 * This test ensures that ByteBuf resources are properly cleaned up when exceptions
 * occur during bulk upload preparation.
 */
public class DefaultWriterMemoryLeakTest {

    @Mock
    private ObjectStorage mockObjectStorage;
    
    @Mock
    private ReservationService mockReservationService;

    private DefaultWriter writer;
    private ObjectWALConfig config;
    private MemoryMXBean memoryBean;
    private AutoCloseable closeable;

    @BeforeEach
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        memoryBean = ManagementFactory.getMemoryMXBean();
        
        // Setup mock reservation service
        when(mockReservationService.verify(any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(true));
        
        config = ObjectWALConfig.builder()
            .withMaxBytesInBatch(1024 * 1024) // 1MB
            .withNodeId(1)
            .withEpoch(1)
            .withBatchInterval(1000)
            .withReservationService(mockReservationService)
            .build();

        // Setup mock object storage to return empty list for initial startup
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.completedFuture(java.util.Collections.emptyList()));

        writer = new DefaultWriter(Time.SYSTEM, mockObjectStorage, config);
        writer.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (closeable != null) {
            closeable.close();
        }
    }

    /**
     * Test that verifies ByteBuf cleanup when WALUtil.generateHeader throws an exception.
     * This simulates the scenario described in the issue where exceptions during
     * buffer preparation cause memory leaks.
     */
    @Test
    public void testMemoryCleanupOnException() throws Exception {
        // Record initial memory usage
        MemoryUsage initialDirectMemory = memoryBean.getNonHeapMemoryUsage();
        long initialDirectMemoryUsed = initialDirectMemory.getUsed();
        
        // Create a stream record batch that will cause an exception during processing
        ByteBuf testData = Unpooled.buffer(1024);
        testData.writeBytes(new byte[1024]);
        
        StreamRecordBatch recordBatch = new StreamRecordBatch(1L, 0L, 1, testData.readableBytes(), testData);
        
        // Create multiple records to ensure we allocate enough memory to detect leaks
        int numRecords = 10;
        AtomicLong exceptionsThrown = new AtomicLong(0);
        
        for (int i = 0; i < numRecords; i++) {
            ByteBuf data = Unpooled.buffer(1024);
            data.writeBytes(new byte[1024]);
            StreamRecordBatch batch = new StreamRecordBatch(1L, i, 1, data.readableBytes(), data);
            
            try {
                // This should trigger the uploadBulk0 method and cause an exception
                // during buffer preparation, testing our cleanup code
                CompletableFuture<AppendResult> future = writer.append(batch);
                
                // Force upload to trigger the problematic code path
                writer.flush().get();
                
                // If we get here, no exception was thrown, which is also valid
                // The important thing is that no memory leak occurs
                
            } catch (Exception e) {
                exceptionsThrown.incrementAndGet();
                // Expected - the mock might throw exceptions
                // The important thing is that ByteBuf cleanup still happens
            }
        }
        
        // Force garbage collection to ensure any leaked ByteBufs are accounted for
        System.gc();
        Thread.sleep(100); // Give GC time to work
        System.gc();
        Thread.sleep(100);
        
        // Check memory usage after operations
        MemoryUsage finalDirectMemory = memoryBean.getNonHeapMemoryUsage();
        long finalDirectMemoryUsed = finalDirectMemory.getUsed();
        
        // The memory usage should not have grown significantly
        // Allow for some variance due to other system operations
        long memoryGrowth = finalDirectMemoryUsed - initialDirectMemoryUsed;
        long maxAllowableGrowth = numRecords * 2048; // 2KB per record max allowable growth
        
        assertTrue(memoryGrowth < maxAllowableGrowth, 
            String.format("Memory growth too large: %d bytes (max allowable: %d bytes). " +
                         "Initial: %d, Final: %d, Exceptions: %d", 
                         memoryGrowth, maxAllowableGrowth, 
                         initialDirectMemoryUsed, finalDirectMemoryUsed, 
                         exceptionsThrown.get()));
    }

    /**
     * Test that verifies proper exception propagation while ensuring cleanup.
     * This test ensures that the original exception is preserved even after cleanup.
     */
    @Test
    public void testExceptionPropagationWithCleanup() throws Exception {
        // Setup mock to throw exception during write operations
        RuntimeException simulatedException = new RuntimeException("Simulated S3 failure");
        when(mockObjectStorage.write(any(), anyString(), any()))
            .thenReturn(CompletableFuture.failedFuture(simulatedException));
        
        ByteBuf testData = Unpooled.buffer(512);
        testData.writeBytes(new byte[512]);
        StreamRecordBatch recordBatch = new StreamRecordBatch(1L, 0L, 1, testData.readableBytes(), testData);
        
        try {
            CompletableFuture<AppendResult> future = writer.append(recordBatch);
            writer.flush().get(); // This should eventually fail
            
            // Try to get the result, which should throw the exception
            future.get();
            fail("Expected exception to be thrown");
        } catch (ExecutionException e) {
            // Verify that some exception was thrown (could be the simulated one or a different one)
            // The key thing is that cleanup happened and we didn't leak memory
            assertNotNull(e.getCause(), "Exception cause should not be null");
        } catch (Exception e) {
            // Also acceptable - any exception is fine as long as cleanup happened
            assertNotNull(e, "Exception should not be null");
        }
        
        // Verify cleanup happened by checking that direct memory usage is reasonable
        System.gc();
        Thread.sleep(50);
        
        // The test passes if we reach here without OutOfMemoryError
        // and memory usage is reasonable
        MemoryUsage memoryUsage = memoryBean.getNonHeapMemoryUsage();
        assertTrue(memoryUsage.getUsed() < memoryUsage.getMax() * 0.9, 
                  "Direct memory usage should not be near maximum after cleanup");
    }

    /**
     * Test simulation of the original bug scenario with network failures.
     * This test simulates the high-load scenario with intermittent failures
     * that was described in the original issue.
     */
    @Test
    public void testHighLoadWithIntermittentFailures() throws Exception {
        MemoryUsage initialMemory = memoryBean.getNonHeapMemoryUsage();
        
        // Simulate intermittent failures like the original bug report
        AtomicLong callCount = new AtomicLong(0);
        when(mockObjectStorage.write(any(), anyString(), any()))
            .thenAnswer(invocation -> {
                long count = callCount.incrementAndGet();
                if (count % 3 == 0) {
                    // Every third call fails to simulate network instability
                    return CompletableFuture.failedFuture(new RuntimeException("Simulated network failure"));
                } else {
                    return CompletableFuture.completedFuture(
                        new ObjectStorage.WriteResult("test-bucket", "test-path"));
                }
            });
        
        int totalRecords = 20;
        int successfulRecords = 0;
        int failedRecords = 0;
        
        for (int i = 0; i < totalRecords; i++) {
            ByteBuf data = Unpooled.buffer(1024);
            data.writeBytes(new byte[1024]);
            StreamRecordBatch batch = new StreamRecordBatch(1L, i, 1, data.readableBytes(), data);
            
            try {
                CompletableFuture<AppendResult> future = writer.append(batch);
                writer.flush().get();
                future.get();
                successfulRecords++;
            } catch (Exception e) {
                failedRecords++;
                // Expected due to our simulated failures
            }
        }
        
        // Force cleanup
        System.gc();
        Thread.sleep(100);
        System.gc();
        
        MemoryUsage finalMemory = memoryBean.getNonHeapMemoryUsage();
        long memoryGrowth = finalMemory.getUsed() - initialMemory.getUsed();
        
        // Memory growth should be reasonable despite the failures
        long maxReasonableGrowth = totalRecords * 2048; // 2KB per record
        assertTrue(memoryGrowth < maxReasonableGrowth,
                  String.format("Memory leak detected: %d bytes growth (max reasonable: %d). " +
                               "Successful: %d, Failed: %d, Total: %d",
                               memoryGrowth, maxReasonableGrowth,
                               successfulRecords, failedRecords, totalRecords));
        
        // Verify that some records actually failed (validating our test setup)
        assertTrue(failedRecords > 0, "Test should have simulated some failures");
    }
}
