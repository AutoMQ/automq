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

package kafka.server.retrystorm;

import kafka.automq.retrystorm.RetryStormBackoffConfig;
import kafka.automq.retrystorm.RetryStormBackoffStateStore;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Covers response gate integration across extractor, policy, logger, scheduler, and final send callback. */
@Tag("S3Unit")
public class RetryStormResponseGateTest {

    /** Given a valid response summary, gate bypasses scheduler and sends immediately. */
    @Test
    public void testImmediatePathSendsNow() {
        AtomicInteger sends = new AtomicInteger(0);
        CapturingScheduler scheduler = new CapturingScheduler();
        RetryStormResponseGate gate = newGate(
            responseSummary(new ResourceResult("topic-0", true, false, false)),
            scheduler,
            RetryStormBackoffLogger.NOOP
        );

        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L),
            "request", "response", sends::incrementAndGet);

        assertEquals(1, sends.get());
        assertEquals(0, scheduler.scheduled.get());
    }

    /** Given repeated transient errors, gate logs and schedules the second response while first is immediate. */
    @Test
    public void testDelayedPathSchedulesAndLogs() {
        AtomicInteger sends = new AtomicInteger(0);
        CapturingScheduler scheduler = new CapturingScheduler();
        CapturingLogger logger = new CapturingLogger();
        ResponseSummary summary = responseSummary(new ResourceResult("topic-0", false, true, true));
        RetryStormResponseGate gate = newGate(summary, scheduler, logger);

        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L),
            "request", "response", sends::incrementAndGet);
        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1001L),
            "request", "response", sends::incrementAndGet);

        assertEquals(1, sends.get());
        assertEquals(1, scheduler.scheduled.get());
        assertEquals(1, logger.logged.get());
    }

    private static RetryStormResponseGate newGate(ResponseSummary summary,
                                                  CapturingScheduler scheduler,
                                                  RetryStormBackoffLogger logger) {
        RetryStormBackoffPolicy policy = new RetryStormBackoffPolicy(
            new RetryStormBackoffConfig(true, 1000L),
            new RetryStormBackoffStateStore()
        );
        Map<ApiKeys, RetryStormResponseSummaryExtractor> extractors = Map.of(
            ApiKeys.PRODUCE,
            (request, response) -> summary
        );
        return new RetryStormResponseGate(policy, scheduler, logger, extractors);
    }

    private static ResponseSummary responseSummary(ResourceResult resource) {
        return new ResponseSummary(List.of(resource));
    }

    private static class CapturingScheduler extends RetryStormDelayedResponseScheduler {
        private final AtomicInteger scheduled = new AtomicInteger(0);

        private CapturingScheduler() {
            super(10L);
        }

        @Override
        public void schedule(Object request, Object response, long delayMs, String reason, Runnable sendNow) {
            scheduled.incrementAndGet();
        }
    }

    private static class CapturingLogger implements RetryStormBackoffLogger {
        private final AtomicInteger logged = new AtomicInteger(0);

        @Override
        public void logDelayed(RetryStormRequestContext context, ResponseSummary responseSummary, BackoffDecision decision) {
            logged.incrementAndGet();
        }
    }
}
