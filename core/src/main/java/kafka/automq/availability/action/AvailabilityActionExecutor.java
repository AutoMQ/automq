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

package kafka.automq.availability.action;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dispatches Broker-executed actions with UUID idempotency, dry-run handling and fail-closed responses.
 */
public class AvailabilityActionExecutor {
    private final Map<AvailabilityActionType, AvailabilityActionAdapter> adapters;
    private final Time time;
    private final Map<Uuid, ActionResponse> responses = new ConcurrentHashMap<>();
    private final Map<Uuid, Long> responseDeadlines = new ConcurrentHashMap<>();
    private final Set<Uuid> inflight = ConcurrentHashMap.newKeySet();

    public AvailabilityActionExecutor(Map<AvailabilityActionType, AvailabilityActionAdapter> adapters, Time time) {
        this.adapters = new EnumMap<>(AvailabilityActionType.class);
        this.adapters.putAll(adapters);
        this.time = time;
    }

    public boolean supports(AvailabilityActionType actionType) {
        return adapters.containsKey(actionType);
    }

    public ActionResponse execute(RecoveryAction action) {
        cleanupExpiredResponses(time.milliseconds());
        ActionResponse existing = responses.get(action.getActionUuid());
        if (existing != null) {
            return existing;
        }
        if (!inflight.add(action.getActionUuid())) {
            return failed(action, time.milliseconds(), "action already inflight");
        }
        try {
            long nowMs = time.milliseconds();
            ActionResponse response = executeOnce(action, nowMs);
            responses.put(action.getActionUuid(), response);
            responseDeadlines.put(action.getActionUuid(), action.getDeadlineMs());
            return response;
        } finally {
            inflight.remove(action.getActionUuid());
        }
    }

    private ActionResponse executeOnce(RecoveryAction action, long nowMs) {
        if (nowMs > action.getDeadlineMs()) {
            return failed(action, nowMs, "action deadline exceeded");
        }
        if (action.isDryRun()) {
            return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
                ActionExecutionStatus.DRY_RUN, nowMs, nowMs, null);
        }
        AvailabilityActionAdapter adapter = adapters.get(action.getActionType());
        if (adapter == null) {
            return failed(action, nowMs, "unsupported action type " + action.getActionType());
        }
        try {
            return adapter.execute(action, nowMs);
        } catch (Exception e) {
            return failed(action, nowMs, e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private ActionResponse failed(RecoveryAction action, long nowMs, String reason) {
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.FAILED, nowMs, nowMs, reason);
    }

    private void cleanupExpiredResponses(long nowMs) {
        responseDeadlines.entrySet().removeIf(entry -> {
            if (nowMs <= entry.getValue()) {
                return false;
            }
            responses.remove(entry.getKey());
            return true;
        });
    }
}
