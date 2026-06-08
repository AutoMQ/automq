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
import kafka.server.ResourceErrorExtractor;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

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
        List<ResourceErrorExtractor.ResourceError> errors = leaderErrors("topic-0");

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1000L).action());

        config.update(Map.of(AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG, true));
        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1001L).action());
    }

    /** Given state is error-driven, skipped success or partial-success responses do not clear delaying state. */
    @Test
    public void testSkippedValidResultDoesNotClearState() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors = leaderErrors("topic-0");

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1000L).action());
        assertEquals(BackoffAction.DELAYED, evaluate(policy, ApiKeys.PRODUCE, errors, 1001L).action());
        BackoffDecision stillDelayed = evaluate(policy, ApiKeys.PRODUCE, errors, 1003L);
        assertEquals(BackoffAction.DELAYED, stillDelayed.action());
    }

    /** Given resource errors, policy derives retry storm classifications from api key and error code. */
    @Test
    public void testPolicyClassifiesResourceErrors() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors = leaderErrors("topic-0");

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1000L).action());
        BackoffDecision decision = evaluate(policy, ApiKeys.PRODUCE, errors, 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertTrue((decision.reasonMask() & RetryStormBackoffStateStore.REASON_DELAYABLE_TRANSIENT) != 0);
    }

    /** Given repeated delayable-transient errors for one resource, the second failure delays. */
    @Test
    public void testDelayableTransientSecondFailureDelays() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors = leaderErrors("topic-0");

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1000L).action());
        BackoffDecision decision = evaluate(policy, ApiKeys.PRODUCE, errors, 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(1000L, decision.delayMs());
        assertTrue((decision.reasonMask() & RetryStormBackoffStateStore.REASON_DELAYABLE_TRANSIENT) != 0);
    }

    /** Given protective-only errors for one resource, the sixth failure in the window delays. */
    @Test
    public void testProtectiveSixthFailureDelays() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors =
            List.of(error(Errors.UNKNOWN_TOPIC_OR_PARTITION, "topic-0"));

        for (int i = 0; i < 5; i++) {
            assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1000L + i).action());
        }
        BackoffDecision decision = evaluate(policy, ApiKeys.PRODUCE, errors, 1005L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertTrue((decision.reasonMask() & RetryStormBackoffStateStore.REASON_PROTECTIVE_ERROR) != 0);
    }

    /** Given a batch with one resource already delaying, response-level delay uses that resource decision. */
    @Test
    public void testBatchAggregatesDelayedResources() {
        RetryStormBackoffPolicy policy = newPolicy(new RetryStormBackoffConfig(true, 250L));
        evaluate(policy, ApiKeys.PRODUCE, leaderErrors("topic-0"), 1000L);

        BackoffDecision decision = evaluate(policy, ApiKeys.PRODUCE, List.of(
            error(Errors.NOT_LEADER_OR_FOLLOWER, "topic-0"),
            error(Errors.NOT_LEADER_OR_FOLLOWER, "topic-1")
        ), 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(250L, decision.delayMs());
        assertTrue((decision.reasonMask() & RetryStormBackoffStateStore.REASON_DELAYABLE_TRANSIENT) != 0);
    }

    /**
     * Given mixed transient and non-transient errors, each resource uses its own error class and
     * response delay aggregates the strictest resource decision.
     */
    @Test
    public void testMixedTransientAndNonTransientBatchAggregatesPerResourceDecisions() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors = List.of(
            error(Errors.NOT_LEADER_OR_FOLLOWER, "topic-0"),
            error(Errors.UNKNOWN_TOPIC_OR_PARTITION, "topic-1")
        );

        assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, errors, 1000L).action());
        BackoffDecision decision = evaluate(policy, ApiKeys.PRODUCE, errors, 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(RetryStormBackoffStateStore.REASON_DELAYABLE_TRANSIENT, decision.reasonMask());
    }

    /** Given separate protective-only resources reach threshold, response-level delay uses the delayed resource. */
    @Test
    public void testBatchAggregatesProtectiveResourceReasons() {
        RetryStormBackoffPolicy policy = newPolicy(new RetryStormBackoffConfig(true, 500L));
        List<ResourceErrorExtractor.ResourceError> seeded =
            List.of(error(Errors.UNKNOWN_TOPIC_OR_PARTITION, "topic-1"));
        for (int i = 0; i < 5; i++) {
            assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.PRODUCE, seeded, 1000L + i).action());
        }

        BackoffDecision decision = evaluate(policy, ApiKeys.PRODUCE, List.of(
            error(Errors.UNKNOWN_TOPIC_OR_PARTITION, "topic-0"),
            error(Errors.UNKNOWN_TOPIC_OR_PARTITION, "topic-1")
        ), 1006L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(500L, decision.delayMs());
        assertEquals(RetryStormBackoffStateStore.REASON_PROTECTIVE_ERROR, decision.reasonMask());
    }

    /** Given an extractor delay cap, response delay is bounded below the configured maximum. */
    @Test
    public void testResponseDelayCapLimitsDecisionDelay() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors = leaderErrors("topic-0");

        assertEquals(BackoffAction.IMMEDIATE, evaluateFetch(policy, errors, OptionalLong.of(25L), 1000L).action());
        BackoffDecision decision = evaluateFetch(policy, errors, OptionalLong.of(25L), 1001L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(25L, decision.delayMs());
    }

    /** Given a zero delay cap, policy updates state but returns immediate until an uncapped response arrives. */
    @Test
    public void testZeroResponseDelayCapUpdatesStateButReturnsImmediate() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors = leaderErrors("topic-0");

        assertEquals(BackoffAction.IMMEDIATE, evaluateFetch(policy, errors, OptionalLong.of(0L), 1000L).action());
        assertEquals(BackoffAction.IMMEDIATE, evaluateFetch(policy, errors, OptionalLong.of(0L), 1001L).action());
        BackoffDecision decision = evaluateFetch(policy, errors, OptionalLong.empty(), 1002L);
        assertEquals(BackoffAction.DELAYED, decision.action());
        assertEquals(1000L, decision.delayMs());
    }

    /** Given an unsupported API has resource errors, policy does not update state or delay. */
    @Test
    public void testUnsupportedApiReturnsImmediateWithoutStateUpdate() {
        RetryStormBackoffPolicy policy = newPolicy();
        List<ResourceErrorExtractor.ResourceError> errors =
            List.of(error(Errors.COORDINATOR_NOT_AVAILABLE, "group-a"));

        for (int i = 0; i < 10; i++) {
            assertEquals(BackoffAction.IMMEDIATE, evaluate(policy, ApiKeys.HEARTBEAT, errors, 1000L + i).action());
        }
    }

    private static BackoffDecision evaluate(RetryStormBackoffPolicy policy,
                                            ApiKeys apiKey,
                                            List<ResourceErrorExtractor.ResourceError> errors,
                                            long nowMs) {
        return policy.evaluate(apiKey, errors, new BackoffContext("connection-1", nowMs), OptionalLong.empty());
    }

    private static BackoffDecision evaluateFetch(RetryStormBackoffPolicy policy,
                                                 List<ResourceErrorExtractor.ResourceError> errors,
                                                 OptionalLong delayCapMs,
                                                 long nowMs) {
        return policy.evaluate(ApiKeys.FETCH, errors, new BackoffContext("connection-1", nowMs), delayCapMs);
    }

    private static List<ResourceErrorExtractor.ResourceError> leaderErrors(String resource) {
        return List.of(error(Errors.NOT_LEADER_OR_FOLLOWER, resource));
    }

    private static ResourceErrorExtractor.ResourceError error(Errors error, String resource) {
        return new ResourceErrorExtractor.ResourceError(error.code(), resource);
    }

    private static RetryStormBackoffPolicy newPolicy() {
        return newPolicy(new RetryStormBackoffConfig(true, 1000L));
    }

    private static RetryStormBackoffPolicy newPolicy(RetryStormBackoffConfig config) {
        return new RetryStormBackoffPolicy(config, new RetryStormBackoffStateStore());
    }
}
