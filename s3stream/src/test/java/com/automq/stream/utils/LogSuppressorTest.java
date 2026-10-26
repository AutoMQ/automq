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

package com.automq.stream.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Verifies bounded failure reporting, recovery, and compatibility with fixed-interval suppression. */
@Tag("S3Unit")
@Timeout(30)
public class LogSuppressorTest {
    private final String node = "destination-1";
    private final MockTime time = new MockTime(0, 0, 0);
    private final List<Event> events = new CopyOnWriteArrayList<>();
    private Logger logger;
    private LogSuppressor failureLogger;

    @BeforeEach
    public void setup() {
        logger = mock(Logger.class, invocation -> {
            String level = invocation.getMethod().getName();
            Object[] rawArgs = invocation.getRawArguments();
            if ((level.equals("error") || level.equals("warn") || level.equals("info")) && rawArgs.length == 2) {
                FormattingTuple tuple = MessageFormatter.arrayFormat((String) rawArgs[0], (Object[]) rawArgs[1]);
                events.add(new Event(level.toUpperCase(Locale.ROOT), tuple.getMessage(), tuple.getThrowable()));
            }
            return null;
        });
        failureLogger = LogSuppressor.builder(logger).timeSource(time::hiResClockMs).build();
    }

    /** The first failure reports immediately, including the destination and the complete throwable/cause stack. */
    @Test
    public void testFirstFailureReportsImmediately() {
        IOException cause = failure("connection lost");
        cause.initCause(new IllegalArgumentException("nested cause"));
        reportFailure(new CompletionException(cause));

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
        reportFailure(failure("connection lost"));
        time.sleep(200);
        reportFailure(failure("connection lost"));
        time.sleep(799);
        reportFailure(failure("connection lost"));
        assertEquals(1, errors().size());

        time.sleep(1);
        reportFailure(failure("connection lost"));
        assertEquals(2, errors().size());
        assertCounts(errors().get(1), 4, 3, 1_000, 0);
        assertTrue(errors().get(1).getMessage().contains("topFrame=Sender.send(Sender.java:10)]=4"));

        time.sleep(2_500);
        reportFailure(failure("connection lost"));
        assertCounts(errors().get(2), 5, 1, 2_500, 0);
    }

    /** Subsequent report windows double from one second and remain capped at sixty seconds. */
    @Test
    public void testExponentialBackoffCapsAtSixtySeconds() {
        reportFailure(failure("connection lost"));
        long totalFailures = 1;
        long[] intervals = {1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000};
        for (long interval : intervals) {
            int reports = errors().size();
            time.sleep(interval - 1);
            reportFailure(failure("connection lost"));
            assertEquals(reports, errors().size());
            time.sleep(1);
            reportFailure(failure("connection lost"));
            totalFailures += 2;
            assertEquals(reports + 1, errors().size());
            assertCounts(errors().get(reports), totalFailures, 2, interval, 0);
        }
    }

    /** Ten fingerprints are retained; every additional occurrence is counted in overflow, including repeats. */
    @Test
    public void testFingerprintLimitAndOverflow() {
        for (int i = 0; i < 13; i++) {
            reportFailure(failure("failure-" + i));
        }
        reportFailure(failure("failure-0"));
        reportFailure(failure("failure-12"));
        time.sleep(1_000);
        reportFailure(failure("failure-1"));

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
        reportFailure(failure(" connection\n\t lost "));
        reportFailure(new CompletionException(failure("connection lost")));
        IllegalStateException otherClass = new IllegalStateException("connection lost");
        otherClass.setStackTrace(failure("unused").getStackTrace());
        reportFailure(otherClass);
        reportFailure(failure("different message"));
        IOException otherFrame = failure("connection lost");
        otherFrame.setStackTrace(new StackTraceElement[] {new StackTraceElement("OtherSender", "send", "OtherSender.java", 11)});
        reportFailure(otherFrame);
        IOException otherLowerFrame = failure("connection lost");
        otherLowerFrame.setStackTrace(new StackTraceElement[] {
            otherLowerFrame.getStackTrace()[0], new StackTraceElement("OtherCaller", "run", "OtherCaller.java", 30)
        });
        reportFailure(otherLowerFrame);
        time.sleep(1_000);
        reportFailure(failure("connection lost"));

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
        reportFailure(empty);
        reportFailure(failure("x".repeat(10_000)));
        recover();

        String report = recoveries().get(0).getMessage();
        assertTrue(report.contains("message=null, topFrame=null"));
        assertTrue(report.contains("message=" + "x".repeat(512) + ","));
        assertFalse(report.contains("x".repeat(513)));
    }

    /** An aggregate carries the latest throwable's entire stack even when its fingerprint overflows the map. */
    @Test
    public void testAggregateUsesLatestException() {
        for (int i = 0; i < 11; i++) {
            reportFailure(failure("earlier-" + i));
        }
        time.sleep(1_000);
        reportFailure(new CompletionException(failure("latest failure")));

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
            reportFailure(failure("previous-" + i));
        }
        time.sleep(1_000);
        reportFailure(failure("previous-0"));
        time.sleep(500);
        reportFailure(failure("previous-0"));
        time.sleep(100);
        recover();

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
        recover();
        assertEquals(1, recoveries().size());

        reportFailure(failure("new episode"));
        assertEquals(3, errors().size());
        assertCounts(errors().get(2), 1, 1, 0, 0);
        assertFalse(errors().get(2).getMessage().contains("previous-"));
        time.sleep(999);
        reportFailure(failure("new episode"));
        assertEquals(3, errors().size());
        time.sleep(1);
        reportFailure(failure("new episode"));
        assertCounts(errors().get(3), 3, 2, 1_000, 0);
    }

    /** Success without suppressed failures emits no recovery noise but still resets the episode. */
    @Test
    public void testRecoveryWithoutUnreportedFailures() {
        recover();
        reportFailure(failure("first episode"));
        recover();
        reportFailure(failure("second episode"));

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
                    reportFailure(failure("connection lost"));
                }
            }, executor)).collect(Collectors.toList());
            start.countDown();
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).get();
        } finally {
            executor.shutdownNow();
        }
        assertEquals(1, errors().size());
        recover();
        Event recovery = recoveries().get(0);
        assertField(recovery, "unreportedFailures", 399);
        assertField(recovery, "totalFailures", 400);
        assertTrue(recovery.getMessage().contains("topFrame=Sender.send(Sender.java:10)]=400"));
    }

    /** Builder instances snapshot configuration and keep independent counts, clocks, and deadlines. */
    @Test
    public void testBuilderConfigurationAndIndependentScopes() {
        LogSuppressor.Builder builder = LogSuppressor.builder(logger).timeSource(time::hiResClockMs)
            .exponentialBackoff(100, 3, 500).maxFingerprints(2);
        LogSuppressor first = builder.build();
        LogSuppressor second = builder.build();
        builder.exponentialBackoff(10_000, 2, 20_000).maxFingerprints(1);
        first.error("first", failure("a"));
        first.error("first", failure("b"));
        first.error("first", failure("c"));
        second.error("second", failure("a"));
        assertEquals(2, events.size());
        assertCounts(events.get(1), 1, 1, 0, 0);
        time.sleep(100);
        first.error("first", failure("a"));
        assertCounts(events.get(2), 4, 3, 100, 1);
        time.sleep(299);
        first.error("first", failure("a"));
        assertEquals(3, events.size());
        time.sleep(1);
        first.error("first", failure("a"));
        assertCounts(events.get(3), 6, 2, 300, 1);
        time.sleep(500);
        first.error("first", failure("a"));
        assertCounts(events.get(4), 7, 1, 500, 1);
    }

    /** Legacy callers keep fixed suppression and their original level, message prefix, and throwable. */
    @Test
    public void testLegacyConstructorBehavior() {
        LogSuppressor suppressed = new LogSuppressor(logger, Long.MAX_VALUE);
        suppressed.error("hidden", failure("error"));
        suppressed.warn("hidden");
        suppressed.info("hidden");
        assertTrue(events.isEmpty());
        assertThrows(IllegalStateException.class, () -> suppressed.recover("recovered"));

        LogSuppressor immediate = new LogSuppressor(logger, -1);
        IOException exception = failure("visible");
        immediate.error("value={}", "context", exception);
        immediate.warn("warning");
        immediate.info("information");
        assertEquals(3, events.size());
        assertEquals("[SUPPRESSED_TIME=0] value=context", events.get(0).getMessage());
        assertSame(exception, events.get(0).throwable());
        assertEquals("WARN", events.get(1).getLevel());
        assertEquals("INFO", events.get(2).getLevel());
    }

    /** Aggregation supports all existing levels and includes template context when no throwable is supplied. */
    @Test
    public void testAggregatedLevelsAndMessagesWithoutExceptions() {
        failureLogger.warn("warning id={}", 1);
        failureLogger.warn("warning id={}", 2);
        time.sleep(1_000);
        failureLogger.info("information id={}", 3);
        assertEquals(2, events.size());
        assertEquals("WARN", events.get(0).getLevel());
        assertEquals("INFO", events.get(1).getLevel());
        assertTrue(events.get(1).getMessage().startsWith("information id=3,totalFailures=3,"));
        assertTrue(events.get(1).getMessage().contains("message=warning id={}, topFrame=null]=2"));
        assertFalse(events.get(1).getThrowableInfo().isPresent());
    }

    /** Invalid aggregation bounds fail at construction rather than corrupting runtime deadlines or memory limits. */
    @Test
    public void testInvalidBuilderConfiguration() {
        assertThrows(NullPointerException.class, () -> LogSuppressor.builder(null));
        assertThrows(NullPointerException.class, () -> LogSuppressor.builder(logger).timeSource(null));
        assertThrows(IllegalArgumentException.class, () -> LogSuppressor.builder(logger).exponentialBackoff(0, 2, 100));
        assertThrows(IllegalArgumentException.class, () -> LogSuppressor.builder(logger).exponentialBackoff(1, 0, 100));
        assertThrows(IllegalArgumentException.class, () -> LogSuppressor.builder(logger).exponentialBackoff(100, 2, 99));
        assertThrows(IllegalArgumentException.class, () -> LogSuppressor.builder(logger).maxFingerprints(0));
    }

    /** Metadata can be appended to covariant argument arrays, and null varargs remain valid SLF4J input. */
    @Test
    public void testArgumentArrayCompatibility() {
        failureLogger.error("request={}", (Object[]) new String[] {"first"});
        failureLogger.error("request={}", (Object[]) new String[] {"second"});
        failureLogger.recover("recovered={}", (Object[]) new String[] {"destination"});
        assertTrue(events.get(0).getMessage().startsWith("request=first,totalFailures=1,"));
        assertTrue(events.get(1).getMessage().startsWith("recovered=destination,unreportedFailures=1,"));
        failureLogger.error("plain", (Object[]) null);
        failureLogger.error("plain", (Object[]) null);
        failureLogger.recover("recovered", (Object[]) null);
        assertTrue(events.get(2).getMessage().startsWith("plain,totalFailures=1,"));
        assertTrue(events.get(3).getMessage().startsWith("recovered,unreportedFailures=1,"));
    }

    private void reportFailure(Throwable exception) {
        failureLogger.error("[REQUEST_ERROR],node={}", node, exception);
    }

    private void recover() {
        failureLogger.recover("[REQUEST_RECOVERED],node={}", node);
    }

    private record Event(String level, String message, Throwable throwable) {
        String getLevel() {
            return level;
        }

        String getMessage() {
            return message;
        }

        Optional<String> getThrowableClassName() {
            return Optional.ofNullable(throwable).map(t -> t.getClass().getName());
        }

        Optional<String> getThrowableInfo() {
            if (throwable == null) {
                return Optional.empty();
            }
            StringWriter stack = new StringWriter();
            throwable.printStackTrace(new PrintWriter(stack));
            return Optional.of(stack.toString());
        }
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
        return events.stream().filter(e -> e.getMessage().startsWith("[REQUEST_ERROR]")).collect(Collectors.toList());
    }

    private List<Event> recoveries() {
        return events.stream().filter(e -> e.getMessage().startsWith("[REQUEST_RECOVERED]")).collect(Collectors.toList());
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
