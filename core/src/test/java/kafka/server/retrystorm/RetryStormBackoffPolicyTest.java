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

import kafka.automq.AutoMQConfig;
import kafka.automq.retrystorm.RetryStormBackoffConfig;
import kafka.automq.retrystorm.RetryStormBackoffStateStore;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers response-level retry storm policy decisions and resource aggregation semantics. */
@Tag("S3Unit")
public class RetryStormBackoffPolicyTest {

    /** Given backoff is disabled, errors return immediately and do not seed later enabled decisions. */
    @Test
    public void testDisabledConfigReturnsImmediateWithoutStateUpdate() {
        RetryStormBackoffConfig config = new RetryStormBackoffConfig(false, 1000L);
        RetryStormBackoffPolicy policy = newPolicy(config);
        BackoffContext context = new BackoffContext("connection-1", 1000L);
        ResponseSummary response = responseSummary(resource("topic-0", false, true, true));

        assertEquals(BackoffAction.IMMEDIATE, policy.evaluate(ApiKeys.PRODUCE, new RequestSummary(), response, context).action());

        config.update(Map.of(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG, true));
        assertEquals(BackoffAction.IMMEDIATE, policy.evaluate(
            ApiKeys.PRODUCE,
            new RequestSummary(),
            response,
            new BackoffContext("connection-1", 1001L)
        ).action());
    }

    /** Given a resource reaches delay, a later valid result clears state so the next error is immediate. */
    @Test
    public void testValidResultClearsState() {
        RetryStormBackoffPolicy policy = newPolicy();
        ResponseSummary error = responseSummary(resource("topic-0", false, true, true));
        ResponseSummary valid = responseSummary(resource("topic-0", true, false, false));

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, error, 1000L).action());
        assertEquals(BackoffAction.DELAYED, evaluate(policy, error, 1001L).action());
        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, valid, 1002L).action());
        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, error, 1003L).action());
    }

    /** Given repeated delayable-transient errors for one resource, the second failure delays. */
    @Test
    public void testDelayableTransientSecondFailureDelays() {
        RetryStormBackoffPolicy policy = newPolicy();
        ResponseSummary response = responseSummary(resource("topic-0", false, true, true));

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, response, 1000L).action());
        BackoffDecision decision = evaluate(policy, response, 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(1000L, decision.delayMs());
        assertTrue(decision.reason().contains("delayable-transient"));
    }

    /** Given protective-only errors for one resource, the sixth failure in the window delays. */
    @Test
    public void testProtectiveSixthFailureDelays() {
        RetryStormBackoffPolicy policy = newPolicy();
        ResponseSummary response = responseSummary(resource("topic-0", false, false, true));

        for (int i = 0; i < 5; i++) {
            assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, response, 1000L + i).action());
        }
        BackoffDecision decision = evaluate(policy, response, 1005L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertTrue(decision.reason().contains("protective-error"));
    }

    /** Given a batch with one resource already delaying, response-level delay uses that resource decision. */
    @Test
    public void testBatchAggregatesDelayedResources() {
        RetryStormBackoffPolicy policy = newPolicy(new RetryStormBackoffConfig(true, 250L));
        policy.evaluate(
            ApiKeys.PRODUCE,
            new RequestSummary(),
            responseSummary(resource("topic-0", false, true, true)),
            new BackoffContext("connection-1", 1000L)
        );

        BackoffDecision decision = policy.evaluate(
            ApiKeys.PRODUCE,
            new RequestSummary(),
            responseSummary(
                resource("topic-0", false, true, true),
                resource("topic-1", false, true, true)
            ),
            new BackoffContext("connection-1", 1001L)
        );
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(250L, decision.delayMs());
        assertTrue(decision.reason().contains("delayable-transient"));
    }

    /** Given mixed transient and non-transient errors, the whole response uses protective counting only. */
    @Test
    public void testMixedTransientAndNonTransientBatchUsesProtectiveThresholdOnly() {
        RetryStormBackoffPolicy policy = newPolicy();
        ResponseSummary response = responseSummary(
            resource("topic-0", false, true, true),
            resource("topic-1", false, false, true)
        );

        for (int i = 0; i < 5; i++) {
            assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, response, 1000L + i).action());
        }
        BackoffDecision decision = evaluate(policy, response, 1005L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals("protective-error", decision.reason());
    }

    /** Given separate protective-only resources reach threshold, response-level delay uses the delayed resource. */
    @Test
    public void testBatchAggregatesProtectiveResourceReasons() {
        RetryStormBackoffPolicy policy = newPolicy(new RetryStormBackoffConfig(true, 500L));
        for (int i = 0; i < 5; i++) {
            assertEquals(BackoffAction.IMMEDIATE, policy.evaluate(
                ApiKeys.PRODUCE,
                new RequestSummary(),
                responseSummary(resource("topic-1", false, false, true)),
                new BackoffContext("connection-1", 1000L + i)
            ).action());
        }

        BackoffDecision decision = policy.evaluate(
            ApiKeys.PRODUCE,
            new RequestSummary(),
            responseSummary(
                resource("topic-0", false, false, true),
                resource("topic-1", false, false, true)
            ),
            new BackoffContext("connection-1", 1006L)
        );
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(500L, decision.delayMs());
        assertEquals("protective-error", decision.reason());
    }

    /** Given an extractor delay cap, response delay is bounded below the configured maximum. */
    @Test
    public void testResponseDelayCapLimitsDecisionDelay() {
        RetryStormBackoffPolicy policy = newPolicy();
        ResponseSummary response = new ResponseSummary(
            List.of(resource("topic-0", false, true, true)),
            OptionalLong.of(25L)
        );

        assertEquals(BackoffAction.IMMEDIATE, evaluateFetch(policy, response, 1000L).action());
        BackoffDecision decision = evaluateFetch(policy, response, 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(25L, decision.delayMs());
    }

    /** Given a zero delay cap, policy updates state but returns immediate until an uncapped response arrives. */
    @Test
    public void testZeroResponseDelayCapUpdatesStateButReturnsImmediate() {
        RetryStormBackoffPolicy policy = newPolicy();
        ResponseSummary capped = new ResponseSummary(
            List.of(resource("topic-0", false, true, true)),
            OptionalLong.of(0L)
        );
        ResponseSummary uncapped = responseSummary(resource("topic-0", false, true, true));

        assertEquals(BackoffAction.IMMEDIATE, evaluateFetch(policy, capped, 1000L).action());
        assertEquals(BackoffAction.IMMEDIATE, evaluateFetch(policy, capped, 1001L).action());
        BackoffDecision decision = evaluateFetch(policy, uncapped, 1002L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(1000L, decision.delayMs());
    }

    private static BackoffDecision evaluate(RetryStormBackoffPolicy policy, ResponseSummary response, long nowMs) {
        return policy.evaluate(ApiKeys.PRODUCE, new RequestSummary(), response, new BackoffContext("connection-1", nowMs));
    }

    private static BackoffDecision evaluateFetch(RetryStormBackoffPolicy policy, ResponseSummary response, long nowMs) {
        return policy.evaluate(ApiKeys.FETCH, new RequestSummary(), response, new BackoffContext("connection-1", nowMs));
    }

    private static ResponseSummary responseSummary(ResourceResult... resources) {
        return new ResponseSummary(List.of(resources));
    }

    private static ResourceResult resource(String resourceKey, boolean valid, boolean delayableTransient, boolean protective) {
        return new ResourceResult(resourceKey, valid, delayableTransient, protective);
    }

    private static RetryStormBackoffPolicy newPolicy() {
        return newPolicy(new RetryStormBackoffConfig(true, 1000L));
    }

    private static RetryStormBackoffPolicy newPolicy(RetryStormBackoffConfig config) {
        return new RetryStormBackoffPolicy(config, new RetryStormBackoffStateStore());
    }
}
