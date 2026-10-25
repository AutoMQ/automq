/*
 * Copyright 2026, AutoMQ HK Limited.
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

package kafka.automq.zerozone;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogCaptureAppender.Event;
import org.apache.kafka.common.utils.MockTime;

import org.apache.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
@Timeout(30)
public class SnapshotSubscriptionFailureLoggerTest {
    private final Node node = new Node(1, "localhost", 9092);
    private final MockTime time = new MockTime(0, 0, 0);
    private SnapshotSubscriptionFailureLogger failureLogger;
    private LogCaptureAppender appender;

    @BeforeEach
    public void setup() {
        appender = LogCaptureAppender.createAndRegister();
        appender.setClassLogger(SnapshotSubscriptionFailureLoggerTest.class, Level.INFO);
        failureLogger = new SnapshotSubscriptionFailureLogger(node, time, LoggerFactory.getLogger(SnapshotSubscriptionFailureLoggerTest.class));
    }

    @AfterEach
    public void cleanup() {
        appender.close();
    }

    /** The first failure reports immediately, including the destination and the complete throwable/cause stack. */
    @Test
    public void testFirstFailureReportsImmediately() {
        IOException cause = failure("connection lost");
        cause.initCause(new IllegalArgumentException("nested cause"));
        failureLogger.onFailure(new CompletionException(cause));

        assertEquals(1, errors().size());
        Event report = errors().get(0);
        assertTrue(report.getMessage().contains("node=" + node));
        assertCounts(report, 1, 1, 0, 0);
        assertEquals(CompletionException.class.getName(), report.getThrowableClassName().orElseThrow());
        String stack = report.getThrowableInfo().orElseThrow();
        assertTrue(stack.contains("java.io.IOException: connection lost"));
        assertTrue(stack.contains("Sender.send(Sender.java:10)"));
        assertTrue(stack.contains("Caller.request(Caller.java:20)"));
        assertTrue(stack.contains("Caused by: java.lang.IllegalArgumentException: nested cause"));
    }

    /** Failures inside a window are counted and emitted together when a later callback reaches the deadline. */
    @Test
    public void testSuppressionAndDeadlineReport() {
        failureLogger.onFailure(failure("connection lost"));
        time.sleep(200);
        failureLogger.onFailure(failure("connection lost"));
        time.sleep(799);
        failureLogger.onFailure(failure("connection lost"));
        assertEquals(1, errors().size());

        time.sleep(1);
        failureLogger.onFailure(failure("connection lost"));
        assertEquals(2, errors().size());
        assertCounts(errors().get(1), 4, 3, 1_000, 0);
        assertTrue(errors().get(1).getMessage().contains("topFrame=Sender.send(Sender.java:10)]=4"));

        time.sleep(2_500);
        failureLogger.onFailure(failure("connection lost"));
        assertCounts(errors().get(2), 5, 1, 2_500, 0);
    }

    /** Subsequent report windows double from one second and remain capped at sixty seconds. */
    @Test
    public void testExponentialBackoffCapsAtSixtySeconds() {
        failureLogger.onFailure(failure("connection lost"));
        long totalFailures = 1;
        long[] intervals = {1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000};
        for (long interval : intervals) {
            int reports = errors().size();
            time.sleep(interval - 1);
            failureLogger.onFailure(failure("connection lost"));
            assertEquals(reports, errors().size());
            time.sleep(1);
            failureLogger.onFailure(failure("connection lost"));
            totalFailures += 2;
            assertEquals(reports + 1, errors().size());
            assertCounts(errors().get(reports), totalFailures, 2, interval, 0);
        }
    }

    /** Ten fingerprints are retained; every additional occurrence is counted in overflow, including repeats. */
    @Test
    public void testFingerprintLimitAndOverflow() {
        for (int i = 0; i < 13; i++) {
            failureLogger.onFailure(failure("failure-" + i));
        }
        failureLogger.onFailure(failure("failure-0"));
        failureLogger.onFailure(failure("failure-12"));
        time.sleep(1_000);
        failureLogger.onFailure(failure("failure-1"));

        Event report = errors().get(1);
        assertCounts(report, 16, 15, 1_000, 4);
        assertEquals(10, report.getMessage().split("exceptionClass=", -1).length - 1);
        assertTrue(report.getMessage().contains("message=failure-0, topFrame=Sender.send(Sender.java:10)]=2"));
        assertTrue(report.getMessage().contains("message=failure-1, topFrame=Sender.send(Sender.java:10)]=2"));
        assertFalse(report.getMessage().contains("message=failure-10,"));
        assertFalse(report.getMessage().contains("message=failure-12,"));
    }

    /** Fingerprints use the unwrapped exception class, normalized message, and top frame, not lower frames. */
    @Test
    public void testFingerprintComponents() {
        failureLogger.onFailure(failure(" connection\n\t lost "));
        failureLogger.onFailure(new CompletionException(failure("connection lost")));
        IllegalStateException otherClass = new IllegalStateException("connection lost");
        otherClass.setStackTrace(failure("unused").getStackTrace());
        failureLogger.onFailure(otherClass);
        failureLogger.onFailure(failure("different message"));
        IOException otherFrame = failure("connection lost");
        otherFrame.setStackTrace(new StackTraceElement[] {new StackTraceElement("OtherSender", "send", "OtherSender.java", 11)});
        failureLogger.onFailure(otherFrame);
        IOException otherLowerFrame = failure("connection lost");
        otherLowerFrame.setStackTrace(new StackTraceElement[] {
            otherLowerFrame.getStackTrace()[0], new StackTraceElement("OtherCaller", "run", "OtherCaller.java", 30)
        });
        failureLogger.onFailure(otherLowerFrame);
        time.sleep(1_000);
        failureLogger.onFailure(failure("connection lost"));

        String report = errors().get(1).getMessage();
        assertEquals(4, report.split("exceptionClass=", -1).length - 1);
        assertTrue(report.contains("exceptionClass=java.io.IOException, message=connection lost, topFrame=Sender.send(Sender.java:10)]=4"));
        assertTrue(report.contains("exceptionClass=java.lang.IllegalStateException"));
        assertTrue(report.contains("message=different message"));
        assertTrue(report.contains("topFrame=OtherSender.send(OtherSender.java:11)"));
    }

    /** Missing messages/frames are supported, and retained normalized messages have a fixed size bound. */
    @Test
    public void testNullAndLongMessages() {
        IOException empty = new IOException();
        empty.setStackTrace(new StackTraceElement[0]);
        failureLogger.onFailure(empty);
        failureLogger.onFailure(failure("x".repeat(10_000)));
        failureLogger.onSuccess();

        String report = recoveries().get(0).getMessage();
        assertTrue(report.contains("message=null, topFrame=null"));
        assertTrue(report.contains("message=" + "x".repeat(512) + ","));
        assertFalse(report.contains("x".repeat(513)));
    }

    /** An aggregate carries the latest throwable's entire stack even when its fingerprint overflows the map. */
    @Test
    public void testAggregateUsesLatestException() {
        for (int i = 0; i < 11; i++) {
            failureLogger.onFailure(failure("earlier-" + i));
        }
        time.sleep(1_000);
        failureLogger.onFailure(new CompletionException(failure("latest failure")));

        Event report = errors().get(1);
        assertCounts(report, 12, 11, 1_000, 2);
        String stack = report.getThrowableInfo().orElseThrow();
        assertTrue(stack.contains("java.io.IOException: latest failure"));
        assertTrue(stack.contains("Sender.send(Sender.java:10)"));
        assertTrue(stack.contains("Caller.request(Caller.java:20)"));
        assertFalse(stack.contains("earlier-"));
    }

    /** Success flushes suppressed counts before the deadline and resets the episode and backoff. */
    @Test
    public void testRecoveryFlushesAndResets() {
        for (int i = 0; i < 11; i++) {
            failureLogger.onFailure(failure("previous-" + i));
        }
        time.sleep(1_000);
        failureLogger.onFailure(failure("previous-0"));
        time.sleep(500);
        failureLogger.onFailure(failure("previous-0"));
        time.sleep(100);
        failureLogger.onSuccess();

        assertEquals(1, recoveries().size());
        Event recovery = recoveries().get(0);
        assertEquals("INFO", recovery.getLevel());
        assertTrue(recovery.getMessage().contains("node=" + node));
        assertField(recovery, "unreportedFailures", 1);
        assertField(recovery, "totalFailures", 13);
        assertField(recovery, "failureDurationMs", 1_600);
        assertField(recovery, "overflowCount", 1);
        assertEquals(10, recovery.getMessage().split("exceptionClass=", -1).length - 1);
        assertFalse(recovery.getThrowableInfo().isPresent());
        failureLogger.onSuccess();
        assertEquals(1, recoveries().size());

        failureLogger.onFailure(failure("new episode"));
        assertEquals(3, errors().size());
        assertCounts(errors().get(2), 1, 1, 0, 0);
        assertFalse(errors().get(2).getMessage().contains("previous-"));
        time.sleep(999);
        failureLogger.onFailure(failure("new episode"));
        assertEquals(3, errors().size());
        time.sleep(1);
        failureLogger.onFailure(failure("new episode"));
        assertCounts(errors().get(3), 3, 2, 1_000, 0);
    }

    /** Success without suppressed failures emits no recovery noise but still resets the episode. */
    @Test
    public void testRecoveryWithoutUnreportedFailures() {
        failureLogger.onSuccess();
        failureLogger.onFailure(failure("first episode"));
        failureLogger.onSuccess();
        failureLogger.onFailure(failure("second episode"));

        assertTrue(recoveries().isEmpty());
        assertEquals(2, errors().size());
        assertCounts(errors().get(1), 1, 1, 0, 0);
    }

    /** Concurrent sender/event-loop completions cannot lose counts or emit multiple first-failure logs. */
    @Test
    public void testConcurrentFailures() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<CompletableFuture<Void>> tasks = IntStream.range(0, 4).mapToObj(i -> CompletableFuture.runAsync(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CompletionException(e);
                }
                for (int j = 0; j < 100; j++) {
                    failureLogger.onFailure(failure("connection lost"));
                }
            }, executor)).collect(Collectors.toList());
            start.countDown();
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).get();
        } finally {
            executor.shutdownNow();
        }
        assertEquals(1, errors().size());
        failureLogger.onSuccess();
        Event recovery = recoveries().get(0);
        assertField(recovery, "unreportedFailures", 399);
        assertField(recovery, "totalFailures", 400);
        assertTrue(recovery.getMessage().contains("topFrame=Sender.send(Sender.java:10)]=400"));
    }

    private static IOException failure(String message) {
        IOException exception = new IOException(message);
        exception.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("Sender", "send", "Sender.java", 10),
            new StackTraceElement("Caller", "request", "Caller.java", 20)
        });
        return exception;
    }

    private List<Event> errors() {
        return appender.getEvents().stream().filter(e -> e.getMessage().startsWith("[SNAPSHOT_SUBSCRIBE_ERROR]")).collect(Collectors.toList());
    }

    private List<Event> recoveries() {
        return appender.getEvents().stream().filter(e -> e.getMessage().startsWith("[SNAPSHOT_SUBSCRIBE_RECOVERED]")).collect(Collectors.toList());
    }

    private static void assertCounts(Event report, long total, long unreported, long period, long overflow) {
        assertEquals("ERROR", report.getLevel());
        assertField(report, "totalFailures", total);
        assertField(report, "failuresSinceLastReport", unreported);
        assertField(report, "reportPeriodMs", period);
        assertField(report, "overflowCount", overflow);
    }

    private static void assertField(Event report, String field, long value) {
        String expected = "," + field + "=" + value;
        assertTrue(report.getMessage().contains(expected + ",") || report.getMessage().endsWith(expected), report.getMessage());
    }
}
