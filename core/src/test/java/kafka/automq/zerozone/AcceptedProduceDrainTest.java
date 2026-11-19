/*
 * Copyright 2026, AutoMQ HK Limited. Licensed under Apache-2.0.
 */

package kafka.automq.zerozone;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protects registration completion, bounded fallback, and permanent admission closure. */
@Tag("S3Unit")
@Timeout(10)
class AcceptedProduceDrainTest {
    /** Given accepted work, stopping admission still permits its registration to finish. */
    @Test
    void testAcceptedRegistrationFinishesBeforeDrain() throws Exception {
        AcceptedProduceDrain drain = new AcceptedProduceDrain();
        assertTrue(drain.begin());
        drain.stop();
        assertFalse(drain.begin());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch waiting = new CountDownLatch(1);
        try {
            Future<?> completion = executor.submit(() -> {
                waiting.countDown();
                drain.await(TimeUnit.SECONDS.toNanos(5));
                return null;
            });
            assertTrue(waiting.await(5, TimeUnit.SECONDS));
            assertFalse(completion.isDone());
            drain.finish();
            completion.get(5, TimeUnit.SECONDS);
            drain.stop();
            drain.await(0);
            assertFalse(drain.begin());
        } finally {
            executor.shutdownNow();
        }
    }

    /** Given concurrent accepted requests, all reservations—not only the latest—must finish. */
    @Test
    void testConcurrentRegistrationsAndRepeatedDrain() throws Exception {
        AcceptedProduceDrain drain = new AcceptedProduceDrain();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<Boolean>> admitted = new ArrayList<>();
        try {
            for (int i = 0; i < 64; i++) {
                admitted.add(executor.submit(drain::begin));
            }
            for (Future<Boolean> admission : admitted) {
                assertTrue(admission.get(5, TimeUnit.SECONDS));
            }
            drain.stop();
            drain.stop();
            assertFalse(drain.begin());
            List<Future<?>> finished = new ArrayList<>();
            for (int i = 0; i < 63; i++) {
                finished.add(executor.submit(drain::finish));
            }
            for (Future<?> finish : finished) {
                finish.get(5, TimeUnit.SECONDS);
            }
            assertThrows(TimeoutException.class, () -> drain.await(0));
            drain.finish();
            drain.await(0);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Given stalled registration, timeout must not release its reservation or reopen admission. */
    @Test
    void testTimeoutDoesNotPretendRegistrationCompleted() throws Exception {
        AcceptedProduceDrain drain = new AcceptedProduceDrain();
        assertTrue(drain.begin());
        drain.stop();
        assertThrows(TimeoutException.class, () -> drain.await(0));
        assertFalse(drain.begin());
        drain.finish();
        drain.await(0);
        assertThrows(IllegalStateException.class, drain::finish);
    }

    /** Given interrupted shutdown, propagate interruption without completing accepted work. */
    @Test
    void testInterruptionPreservesReservation() throws Exception {
        AcceptedProduceDrain drain = new AcceptedProduceDrain();
        assertTrue(drain.begin());
        drain.stop();
        Thread.currentThread().interrupt();
        try {
            assertThrows(InterruptedException.class, () -> drain.await(TimeUnit.SECONDS.toNanos(1)));
            assertThrows(TimeoutException.class, () -> drain.await(0));
            drain.finish();
            drain.await(0);
        } finally {
            Thread.interrupted();
        }
    }
}
