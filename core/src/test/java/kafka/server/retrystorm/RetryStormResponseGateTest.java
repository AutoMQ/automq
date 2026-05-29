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
import kafka.server.ResourceErrorExtractor;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Covers response gate integration across extracted error view, policy, logger, scheduler, and final send callback. */
@Tag("S3Unit")
public class RetryStormResponseGateTest {

    /** Given an empty error view, gate bypasses policy and sends immediately. */
    @Test
    public void testImmediatePathSendsNow() {
        AtomicInteger sends = new AtomicInteger(0);
        CapturingScheduler scheduler = new CapturingScheduler();
        RetryStormResponseGate gate = newGate(scheduler, RetryStormBackoffLogger.NOOP);

        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L),
            ResourceErrorExtractor.ResourceErrorView.empty(), OptionalLong.empty(), sends::incrementAndGet);

        assertEquals(1, sends.get());
        assertEquals(0, scheduler.scheduled.get());
    }

    /** Given repeated transient all-error views, gate schedules the second response while first is immediate. */
    @Test
    public void testDelayedPathSchedules() {
        AtomicInteger sends = new AtomicInteger(0);
        CapturingScheduler scheduler = new CapturingScheduler();
        RetryStormResponseGate gate = newGate(scheduler, RetryStormBackoffLogger.NOOP);
        ResourceErrorExtractor.ResourceErrorView allError = allErrorView();

        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L),
            allError, OptionalLong.empty(), sends::incrementAndGet);
        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1001L),
            allError, OptionalLong.empty(), sends::incrementAndGet);

        assertEquals(1, sends.get());
        assertEquals(1, scheduler.scheduled.get());
    }

    /** Given partial success has errors but is not all-error, gate sends immediately without updating state. */
    @Test
    public void testNonAllErrorViewDoesNotUpdatePolicyState() {
        AtomicInteger sends = new AtomicInteger(0);
        CapturingScheduler scheduler = new CapturingScheduler();
        RetryStormResponseGate gate = newGate(scheduler, RetryStormBackoffLogger.NOOP);
        ResourceErrorExtractor.ResourceErrorView partial =
            new ResourceErrorExtractor.ResourceErrorView(false, List.of(
                new ResourceErrorExtractor.ResourceError(Errors.NOT_LEADER_OR_FOLLOWER.code(), "topic-0")
            ));
        ResourceErrorExtractor.ResourceErrorView allError = allErrorView();

        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L),
            partial, OptionalLong.empty(), sends::incrementAndGet);
        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1001L),
            allError, OptionalLong.empty(), sends::incrementAndGet);

        assertEquals(2, sends.get());
        assertEquals(0, scheduler.scheduled.get());
    }

    /** Given policy evaluation fails, gate falls back to immediate send without scheduling. */
    @Test
    public void testPolicyFailureFallsBackToImmediateSend() {
        AtomicInteger sends = new AtomicInteger(0);
        CapturingScheduler scheduler = new CapturingScheduler();
        RetryStormResponseGate gate = newGate(scheduler, RetryStormBackoffLogger.NOOP);
        ResourceErrorExtractor.ResourceErrorView invalidErrorView =
            new ResourceErrorExtractor.ResourceErrorView(true, List.of(
                new ResourceErrorExtractor.ResourceError(Short.MIN_VALUE, "topic-0")
            ));

        gate.sendOrDelay(new RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L),
            invalidErrorView, OptionalLong.empty(), sends::incrementAndGet);

        assertEquals(1, sends.get());
        assertEquals(0, scheduler.scheduled.get());
    }

    private static RetryStormResponseGate newGate(CapturingScheduler scheduler,
                                                  RetryStormBackoffLogger ignoredLogger) {
        RetryStormBackoffPolicy policy = new RetryStormBackoffPolicy(
            new RetryStormBackoffConfig(true, 1000L),
            new RetryStormBackoffStateStore()
        );
        return new RetryStormResponseGate(policy, scheduler);
    }

    private static ResourceErrorExtractor.ResourceErrorView allErrorView() {
        return new ResourceErrorExtractor.ResourceErrorView(true, List.of(
            new ResourceErrorExtractor.ResourceError(Errors.NOT_LEADER_OR_FOLLOWER.code(), "topic-0")
        ));
    }

    private static class CapturingScheduler extends RetryStormDelayedResponseScheduler {
        private final AtomicInteger scheduled = new AtomicInteger(0);

        private CapturingScheduler() {
            super(10L);
        }

        @Override
        public void schedule(long delayMs, Runnable sendNow) {
            scheduled.incrementAndGet();
        }
    }
}
