package com.automq.stream.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.automq.stream.s3.model.StreamRecordBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

@ExtendWith(MockitoExtension.class)
public class WALConfirmOffsetCalculatorTest {

    @Mock
    private StreamRecordBatch mockRecord;

    private S3Storage.WALConfirmOffsetCalculator calculator;

    @BeforeEach
    public void setUp() {
        calculator = new S3Storage.WALConfirmOffsetCalculator();
    }

    @Test
    public void testInitialState() {
        // Verify the initial state is NOOP_OFFSET
        assertEquals(S3Storage.WALConfirmOffsetCalculator.NOOP_OFFSET, calculator.get());
    }

    @Test
    public void testCalculateWithEmptyQueue() {
        // Test behavior when no requests are added
        calculator.update();
        assertEquals(S3Storage.WALConfirmOffsetCalculator.NOOP_OFFSET, calculator.get());
    }

    @Test
    public void testCalculateWithSingleUnconfirmedRequest() {
        // Add a single unconfirmed request
        S3Storage.WalWriteRequest request = createRequest(1, 100);
        calculator.add(request);
        
        // Calculate and verify no offset is confirmed
        calculator.update();
        assertEquals(S3Storage.WALConfirmOffsetCalculator.NOOP_OFFSET, calculator.get());
    }

    @Test
    public void testCalculateWithSingleConfirmedRequest() {
        // Add a single confirmed request
        S3Storage.WalWriteRequest request = createRequest(1, 100);
        request.confirmed = true;
        calculator.add(request);
        
        // Calculate and verify the offset is confirmed
        calculator.update();
        assertEquals(100, calculator.get());
    }

    @Test
    public void testCalculateWithMultipleSequentialConfirmedRequests() {
        // Add multiple confirmed requests with sequential offsets
        for (int i = 1; i <= 5; i++) {
            S3Storage.WalWriteRequest request = createRequest(i, i * 100);
            request.confirmed = true;
            calculator.add(request);
        }
        
        // Calculate and verify all offsets are confirmed
        calculator.update();
        assertEquals(500, calculator.get());
    }

    @Test
    public void testCalculateWithGapInConfirmedRequests() {
        // Add requests with a gap in confirmed status
        List<S3Storage.WalWriteRequest> requests = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            S3Storage.WalWriteRequest request = createRequest(i, i * 100);
            request.confirmed = i != 3; // Request with offset 300 is not confirmed
            requests.add(request);
            calculator.add(request);
        }
        
        // Calculate and verify offset is confirmed up to the gap
        calculator.update();
        assertEquals(200, calculator.get());
        
        // Now confirm the gap request
        requests.get(2).confirmed = true;
        
        // Calculate again and verify all offsets are confirmed
        calculator.update();
        assertEquals(500, calculator.get());
    }

    @Test
    public void testCalculateWithMultipleOutOfOrderConfirmations() {
        // Add requests that are confirmed out of order
        List<S3Storage.WalWriteRequest> requests = new ArrayList<>();
        
        // Add 5 requests with increasing offsets (100, 200, 300, 400, 500)
        for (int i = 1; i <= 5; i++) {
            S3Storage.WalWriteRequest request = createRequest(i, i * 100);
            requests.add(request);
            calculator.add(request);
        }
        
        // Confirm them out of order: 2, 4, 1, 5, 3
        requests.get(1).confirmed = true; // Offset 200
        requests.get(3).confirmed = true; // Offset 400
        
        calculator.update();
        assertEquals(S3Storage.WALConfirmOffsetCalculator.NOOP_OFFSET, calculator.get());
        
        requests.get(0).confirmed = true; // Offset 100
        
        calculator.update();
        assertEquals(100, calculator.get());
        
        requests.get(4).confirmed = true; // Offset 500
        
        calculator.update();
        assertEquals(100, calculator.get());
        
        requests.get(2).confirmed = true; // Offset 300
        
        calculator.update();
        assertEquals(500, calculator.get());
    }

    @Test
    public void testCalculateRemovesProcessedRequests() {
        // Add multiple requests
        List<S3Storage.WalWriteRequest> requests = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            S3Storage.WalWriteRequest request = createRequest(i, i * 100);
            requests.add(request);
            calculator.add(request);
        }
        
        // Confirm the first three
        for (int i = 0; i < 3; i++) {
            requests.get(i).confirmed = true;
        }
        
        // Calculate (this should remove confirmed requests)
        calculator.update();
        assertEquals(300, calculator.get());
        
        // Add more requests
        for (int i = 6; i <= 8; i++) {
            S3Storage.WalWriteRequest request = createRequest(i, i * 100);
            request.confirmed = true;
            calculator.add(request);
        }
        
        // Confirm remaining original requests
        requests.get(3).confirmed = true;
        requests.get(4).confirmed = true;
        
        // Calculate again
        calculator.update();
        assertEquals(800, calculator.get());
    }

    @Test
    public void testConcurrentAddAndCalculate() throws InterruptedException {
        int numThreads = 10;
        int requestsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicBoolean error = new AtomicBoolean(false);
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        
        // Submit tasks for adding requests
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Lock lock = calculator.addLock();
                    
                    for (int i = 0; i < requestsPerThread; i++) {
                        lock.lock();
                        try {
                            int offset = threadId * requestsPerThread + i;
                            S3Storage.WalWriteRequest request = createRequest(offset, offset);
                            request.confirmed = true;
                            calculator.add(request);
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (Exception e) {
                    error.set(true);
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        
        // Start concurrent operations
        startLatch.countDown();
        
        // Periodically calculate while threads are adding
        for (int i = 0; i < 5; i++) {
            Thread.sleep(10);
            calculator.update();
        }
        
        // Wait for all threads to complete
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();
        
        // Verify no errors occurred
        assertFalse(error.get(), "Errors occurred during concurrent operations");
        
        // Calculate one final time
        calculator.update();
        
        // The highest offset should be (numThreads * requestsPerThread - 1)
        assertEquals(numThreads * requestsPerThread - 1, calculator.get());
    }

    @Test
    public void testAddLockIsReadLock() {
        Lock lock = calculator.addLock();
        // Verify multiple threads can acquire the lock simultaneously
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(5);
        AtomicBoolean error = new AtomicBoolean(false);
        
        ExecutorService executor = Executors.newFixedThreadPool(5);
        
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Lock threadLock = calculator.addLock();
                    threadLock.lock();
                    try {
                        // Hold the lock for a bit
                        Thread.sleep(100);
                    } finally {
                        threadLock.unlock();
                        doneLatch.countDown();
                    }
                } catch (Exception e) {
                    error.set(true);
                    e.printStackTrace();
                }
            });
        }
        
        startLatch.countDown();
        try {
            // If this is a read lock, all threads should complete quickly
            assertTrue(doneLatch.await(1, TimeUnit.SECONDS), 
                    "Not all threads completed in time, suggesting lock is not a read lock");
            assertFalse(error.get(), "Errors occurred during lock testing");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testUnconfirmedRequestsDoNotAffectConfirmedOffset() {
        // Add a mix of confirmed and unconfirmed requests with interleaved offsets
        for (int i = 1; i <= 10; i++) {
            S3Storage.WalWriteRequest request = createRequest(i, i * 10);
            // Confirm even-numbered requests only
            request.confirmed = (i % 2 == 0);
            calculator.add(request);
        }
        
        calculator.update();
        
        // Since odd-numbered requests (10, 30, 50, 70, 90) are unconfirmed,
        // the confirmed offset should be 0 (the default, since we have gaps from the beginning)
        assertEquals(S3Storage.WALConfirmOffsetCalculator.NOOP_OFFSET, calculator.get());
        
        // Now confirm the first request
        S3Storage.WalWriteRequest additionalRequest = createRequest(1, 10);
        additionalRequest.confirmed = true;
        calculator.add(additionalRequest);
        
        calculator.update();
        
        // Now we should have confirmed up to offset 20 (since 1 and 2 are confirmed)
        assertEquals(20, calculator.get());
    }

    @Test
    public void testWithVeryLargeOffset() {
        // Test with a large offset value to ensure no overflow occurs
        long largeOffset = Long.MAX_VALUE - 100;
        S3Storage.WalWriteRequest request = createRequest(1, largeOffset);
        request.confirmed = true;
        calculator.add(request);
        
        calculator.update();
        assertEquals(largeOffset, calculator.get());
    }

    @Test
    public void testWithZeroOffset() {
        // Test with offset 0, which is a valid offset
        S3Storage.WalWriteRequest request = createRequest(1, 0);
        request.confirmed = true;
        calculator.add(request);
        
        calculator.update();
        assertEquals(0, calculator.get());
    }

    // Helper method to create a request with specified stream ID and offset
    private S3Storage.WalWriteRequest createRequest(long streamId, long offset) {
        when(mockRecord.getStreamId()).thenReturn(streamId);
        S3Storage.WalWriteRequest request = new S3Storage.WalWriteRequest(mockRecord, offset, null, null);
        return request;
    }

    // Helper assertion method to make test failures more descriptive
    private void assertFalse(boolean condition, String message) {
        if (condition) {
            throw new AssertionError(message);
        }
    }
}
