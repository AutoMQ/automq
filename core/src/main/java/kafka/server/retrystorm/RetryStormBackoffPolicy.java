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

import java.util.EnumSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Evaluates all-error resource views and updates per-resource retry storm backoff state.
 *
 * <p>The policy owns strategy classification from API key and final response error code. The shared
 * extractor supplies only response facts, the state store decides each resource threshold, and the
 * policy returns the maximum response delay and combined reasons.</p>
 */
public class RetryStormBackoffPolicy {
    private static final Set<ApiKeys> SUPPORTED_API_KEYS = EnumSet.of(
        ApiKeys.PRODUCE,
        ApiKeys.FETCH,
        ApiKeys.LIST_OFFSETS,
        ApiKeys.OFFSET_FOR_LEADER_EPOCH,
        ApiKeys.METADATA,
        ApiKeys.DESCRIBE_TOPIC_PARTITIONS,
        ApiKeys.FIND_COORDINATOR,
        ApiKeys.JOIN_GROUP,
        ApiKeys.SYNC_GROUP,
        ApiKeys.OFFSET_COMMIT,
        ApiKeys.OFFSET_FETCH,
        ApiKeys.LEAVE_GROUP,
        ApiKeys.TXN_OFFSET_COMMIT,
        ApiKeys.ADD_OFFSETS_TO_TXN,
        ApiKeys.INIT_PRODUCER_ID,
        ApiKeys.END_TXN
    );
    private static final Set<Errors> DELAYABLE_LEADER_ERRORS = EnumSet.of(
        Errors.NOT_LEADER_OR_FOLLOWER,
        Errors.LEADER_NOT_AVAILABLE,
        Errors.FENCED_LEADER_EPOCH
    );
    private static final Set<Errors> DELAYABLE_COORDINATOR_ERRORS = EnumSet.of(
        Errors.COORDINATOR_LOAD_IN_PROGRESS,
        Errors.COORDINATOR_NOT_AVAILABLE,
        Errors.NOT_COORDINATOR
    );

    private final RetryStormBackoffConfig config;
    private final RetryStormBackoffStateStore stateStore;

    /**
     * Creates a policy over the dynamic retry storm config and per-resource state store.
     */
    public RetryStormBackoffPolicy(RetryStormBackoffConfig config, RetryStormBackoffStateStore stateStore) {
        this.config = config;
        this.stateStore = stateStore;
    }

    /**
     * Updates retry storm state from all-error resource errors and returns the response-level action.
     *
     * <p>The caller must only invoke this method for responses that have no valid result. Error
     * classification is derived from the API key and final response error code.</p>
     */
    public BackoffDecision evaluate(ApiKeys apiKey,
                                    List<ResourceErrorExtractor.ResourceError> errors,
                                    BackoffContext context,
                                    OptionalLong delayCapMs) {
        if (!config.enabled() || !supports(apiKey) || errors.isEmpty()) {
            return BackoffDecision.immediate();
        }

        long decisionDelayMs = delayCapMs.isPresent()
            ? Math.min(config.maxDelayMs(), Math.max(delayCapMs.getAsLong(), 0L))
            : config.maxDelayMs();
        long maxDelayMs = 0L;
        int reasonMask = 0;
        for (ResourceErrorExtractor.ResourceError error : errors) {
            boolean delayableTransient = isDelayableTransient(apiKey, Errors.forCode(error.errorCode()));
            RetryStormBackoffStateStore.StateDecision decision = stateStore.recordAndDecide(
                backoffKey(apiKey, error, context),
                new RetryStormBackoffStateStore.ErrorClassSet(delayableTransient, true),
                context.nowMs(),
                decisionDelayMs
            );
            if (decision.delayed()) {
                maxDelayMs = Math.max(maxDelayMs, decision.delayMs());
                reasonMask |= decision.reasonMask();
            }
        }

        if (reasonMask == 0) {
            return BackoffDecision.immediate();
        }
        return new BackoffDecision(BackoffAction.DELAYED, maxDelayMs, reasonMask);
    }

    /**
     * Returns whether retry storm backoff should evaluate all-error responses for this API.
     */
    public boolean supports(ApiKeys apiKey) {
        return SUPPORTED_API_KEYS.contains(apiKey);
    }

    private RetryStormBackoffStateStore.BackoffKey backoffKey(ApiKeys apiKey,
                                                              ResourceErrorExtractor.ResourceError error,
                                                              BackoffContext context) {
        return new RetryStormBackoffStateStore.BackoffKey(apiKey.id, error.resource(), context.clientScope());
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private boolean isDelayableTransient(ApiKeys apiKey, Errors error) {
        switch (apiKey) {
            case PRODUCE:
            case FETCH:
            case LIST_OFFSETS:
            case OFFSET_FOR_LEADER_EPOCH:
            case METADATA:
            case DESCRIBE_TOPIC_PARTITIONS:
                return DELAYABLE_LEADER_ERRORS.contains(error);
            case FIND_COORDINATOR:
            case JOIN_GROUP:
            case SYNC_GROUP:
            case OFFSET_COMMIT:
            case OFFSET_FETCH:
            case LEAVE_GROUP:
            case TXN_OFFSET_COMMIT:
            case ADD_OFFSETS_TO_TXN:
            case INIT_PRODUCER_ID:
            case END_TXN:
                return DELAYABLE_COORDINATOR_ERRORS.contains(error);
            default:
                return false;
        }
    }
}
