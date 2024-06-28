package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.utils.threads.EventLoop;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class AsyncSemaphoreTest {

    @Test
    public void testEventLoopWillPollIfPermitsAvailable() throws InterruptedException {
        AsyncSemaphore semaphore = new AsyncSemaphore(100);
        EventLoop eventLoop = new EventLoop("testAcquireAndRelease");

        CompletableFuture<Void> task = new CompletableFuture<>();

        for (int i = 0; i < 10; i++) {
            boolean acquire = semaphore.acquire(10, () -> task, eventLoop);
            assertTrue(acquire);
        }

        // no more permits until the future complete
        assertEquals(0, semaphore.permits());

        List<CompletableFuture<Void>> task2 = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            task2.add(cf);
            boolean acquire = semaphore.acquire(10, () -> cf, eventLoop);
            assertFalse(acquire);
        }

        // not acquired task should be in semaphore task queue.
        assertEquals(semaphore.pendingTaskNumber(), 10);

        task.complete(null);

        Thread.sleep(500);

        // when previous task finished the available permits will be polled by eventLoop.
        assertEquals(0, semaphore.permits());
        assertEquals(10, eventLoop.finishedTask());

        // when all task finished the permits will be released
        task2.forEach(cf -> cf.complete(null));
        assertEquals(100, semaphore.permits());

        eventLoop.shutdownGracefully();
    }

    @Test
    public void testConcurrentAcquireAndRelease() throws InterruptedException {
        AsyncSemaphore semaphore = new AsyncSemaphore(1024);
        EventLoop eventLoop = new EventLoop("testAcquireAndRelease");

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        AtomicLong taskNumber = new AtomicLong(0);

        CountDownLatch latch = new CountDownLatch(1024 * 10);

        for (int j = 0; j < 10; j++) {
            executorService.submit(() -> {
                for (int i = 0; i < 1024; i++) {
                    long permits = ThreadLocalRandom.current().nextInt(512);
                    semaphore.acquire(permits, () -> {
                        CompletableFuture<Void> cf = new CompletableFuture<>();
                        cf.completeOnTimeout(null, permits, TimeUnit.NANOSECONDS).whenComplete((n, e) -> {
                            taskNumber.incrementAndGet();
                            latch.countDown();
                        });
                        return cf;
                    }, eventLoop);
                }

            });
        }

        latch.await();
        assertEquals(1024 * 10, taskNumber.get());
        eventLoop.shutdownGracefully();
    }

}
