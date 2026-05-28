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

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Evaluates schema-independent retry storm summaries and updates per-resource backoff state.
 *
 * <p>The policy owns response-level aggregation only: extractors classify resources, the state store
 * decides each resource threshold, and the policy returns the maximum delay and combined reasons.</p>
 */
public class RetryStormBackoffPolicy {
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
     * Updates retry storm state for all relevant response resources and returns the response-level action.
     *
     * <p>Any valid resource makes the whole response immediate and clears that resource state. For all-error
     * responses, each resource is evaluated independently before the response-level delay and reason are
     * aggregated.</p>
     */
    public BackoffDecision evaluate(ApiKeys apiKey,
                                    RequestSummary requestSummary,
                                    ResponseSummary responseSummary,
                                    BackoffContext context) {
        if (!config.enabled()) {
            return new BackoffDecision(BackoffAction.IMMEDIATE);
        }

        boolean hasValidResource = false;
        for (ResourceResult resource : responseSummary.resources()) {
            if (resource.valid()) {
                hasValidResource = true;
                stateStore.clear(backoffKey(apiKey, resource, context));
            }
        }
        if (hasValidResource) {
            return new BackoffDecision(BackoffAction.IMMEDIATE);
        }

        long decisionDelayMs = responseSummary.delayCapMs().isPresent()
            ? Math.min(config.maxDelayMs(), Math.max(responseSummary.delayCapMs().getAsLong(), 0L))
            : config.maxDelayMs();
        boolean containsNonTransientError = responseSummary.resources().stream()
            .anyMatch(resource -> resource.protective() && !resource.delayableTransient());

        long maxDelayMs = 0L;
        Set<String> reasons = new TreeSet<>();
        for (ResourceResult resource : responseSummary.resources()) {
            if (!resource.delayableTransient() && !resource.protective()) {
                continue;
            }
            boolean delayableTransient = !containsNonTransientError && resource.delayableTransient();
            RetryStormBackoffStateStore.StateDecision decision = stateStore.recordAndDecide(
                backoffKey(apiKey, resource, context),
                new RetryStormBackoffStateStore.ErrorClassSet(delayableTransient, resource.protective()),
                context.nowMs(),
                decisionDelayMs
            );
            if (decision.delayed()) {
                maxDelayMs = Math.max(maxDelayMs, decision.delayMs());
                reasons.addAll(Arrays.stream(decision.reason().split(","))
                    .filter(reason -> !reason.isEmpty())
                    .collect(Collectors.toSet()));
            }
        }

        if (reasons.isEmpty()) {
            return new BackoffDecision(BackoffAction.IMMEDIATE);
        }
        return new BackoffDecision(BackoffAction.DELAYED, maxDelayMs, String.join(",", reasons));
    }

    private RetryStormBackoffStateStore.BackoffKey backoffKey(ApiKeys apiKey,
                                                              ResourceResult resource,
                                                              BackoffContext context) {
        return new RetryStormBackoffStateStore.BackoffKey(apiKey.id, resource.resourceKey(), context.clientScope());
    }
}
