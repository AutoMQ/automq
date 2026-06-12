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

package kafka.automq.availability.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes active-Controller availability actions with UUID idempotency and response persistence.
 */
public class ControllerActionExecutor {
    private final Map<AvailabilityActionType, ControllerActionAdapter> adapters;
    private final ControllerActionStateStore stateStore;
    private final Time time;
    private final Set<Uuid> inflight = ConcurrentHashMap.newKeySet();

    public ControllerActionExecutor(Map<AvailabilityActionType, ControllerActionAdapter> adapters,
                                    ControllerActionStateStore stateStore,
                                    Time time) {
        this.adapters = adapters;
        this.stateStore = stateStore;
        this.time = time;
    }

    public CompletableFuture<ActionResponse> execute(RecoveryAction action) {
        ControllerActionState existingState = stateStore.get(action.getActionUuid());
        if (existingState != null && existingState.getResponse() != null) {
            return CompletableFuture.completedFuture(existingState.getResponse());
        }
        long nowMs = time.milliseconds();
        if (nowMs > action.getDeadlineMs()) {
            ActionResponse response = incomplete(action, "controller action deadline expired before execution");
            stateStore.put(new ControllerActionState(action, response));
            return CompletableFuture.completedFuture(response);
        }
        if (!inflight.add(action.getActionUuid())) {
            return CompletableFuture.completedFuture(failed(action, "action already inflight"));
        }
        stateStore.put(new ControllerActionState(action, null));
        ControllerActionAdapter adapter = adapters.get(action.getActionType());
        if (adapter == null) {
            ActionResponse response = failed(action, "unsupported controller action type " + action.getActionType());
            stateStore.put(new ControllerActionState(action, response));
            inflight.remove(action.getActionUuid());
            return CompletableFuture.completedFuture(response);
        }
        return adapter.execute(action).handle((ignored, exception) -> {
            ActionResponse response = exception == null ? succeeded(action) : failed(action,
                exception.getClass().getName() + ": " + exception.getMessage());
            stateStore.put(new ControllerActionState(action, response));
            inflight.remove(action.getActionUuid());
            return response;
        });
    }

    public void restore(ControllerActionState state) {
        if (state == null || state.getAction() == null) {
            return;
        }
        ControllerActionState existingState = stateStore.get(state.getAction().getActionUuid());
        if (existingState == null || existingState.getResponse() == null) {
            stateStore.put(state);
        }
        if (state.getResponse() == null) {
            execute(state.getAction());
        }
    }

    private ActionResponse succeeded(RecoveryAction action) {
        long nowMs = time.milliseconds();
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.SUCCEEDED, nowMs, nowMs, null);
    }

    private ActionResponse failed(RecoveryAction action, String reason) {
        long nowMs = time.milliseconds();
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.FAILED, nowMs, nowMs, reason);
    }

    private ActionResponse incomplete(RecoveryAction action, String reason) {
        long nowMs = time.milliseconds();
        return new ActionResponse(action.getActionUuid(), action.getActionType(), action.getTarget(),
            ActionExecutionStatus.INCOMPLETE, nowMs, nowMs, reason);
    }

    public interface ControllerActionAdapter {
        CompletableFuture<Void> execute(RecoveryAction action);
    }

    public interface ControllerActionStateStore {
        ControllerActionState get(Uuid actionUuid);

        void put(ControllerActionState state);
    }
}
