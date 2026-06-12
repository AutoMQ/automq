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

import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityConstants;
import org.apache.kafka.controller.availability.AvailabilityKvKeys;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.List;

/**
 * Classifies retained availability KV state for cleanup and reports deletion failures without blocking reconcile.
 */
public class AvailabilityKvCleanup {
    private final CleanupSink cleanupSink;
    private final CleanupFailureListener failureListener;
    private final long responseRetentionMs;
    private final long actionCleanupGraceMs;

    public AvailabilityKvCleanup(CleanupSink cleanupSink, CleanupFailureListener failureListener,
                                 long responseRetentionMs, long actionCleanupGraceMs) {
        this.cleanupSink = cleanupSink;
        this.failureListener = failureListener;
        this.responseRetentionMs = responseRetentionMs;
        this.actionCleanupGraceMs = actionCleanupGraceMs;
    }

    public int cleanup(long nowMs, List<RecoveryAction> actions, List<ActionResponse> responses,
                       List<ControllerActionState> controllerActionStates) {
        int deleted = 0;
        for (RecoveryAction action : actions) {
            if (deleteExpiredAction(nowMs, action)) {
                deleted++;
            }
        }
        for (ActionResponse response : responses) {
            if (deleteExpiredResponse(nowMs, response)) {
                deleted++;
            }
        }
        for (ControllerActionState state : controllerActionStates) {
            if (deleteExpiredControllerActionState(nowMs, state)) {
                deleted++;
            }
        }
        return deleted;
    }

    private boolean deleteExpiredAction(long nowMs, RecoveryAction action) {
        if (cleanupClass(AvailabilityConstants.ACTION_NAMESPACE, nowMs, action, null, null) !=
            AvailabilityKvKeys.CleanupClass.EXPIRED_ACTION) {
            return false;
        }
        return delete(AvailabilityConstants.ACTION_NAMESPACE, AvailabilityKvKeys.actionKey(action.getActionUuid()));
    }

    private boolean deleteExpiredResponse(long nowMs, ActionResponse response) {
        if (cleanupClass(AvailabilityConstants.RESPONSE_NAMESPACE, nowMs, null, response, null) !=
            AvailabilityKvKeys.CleanupClass.EXPIRED_RESPONSE) {
            return false;
        }
        return delete(AvailabilityConstants.RESPONSE_NAMESPACE, AvailabilityKvKeys.responseKey(response.getActionUuid()));
    }

    private boolean deleteExpiredControllerActionState(long nowMs, ControllerActionState state) {
        if (cleanupClass(AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE, nowMs, null, null, state) !=
            AvailabilityKvKeys.CleanupClass.EXPIRED_CONTROLLER_ACTION) {
            return false;
        }
        return delete(AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE,
            AvailabilityKvKeys.controllerActionKey(state.getAction().getActionUuid()));
    }

    private AvailabilityKvKeys.CleanupClass cleanupClass(String namespace, long nowMs, RecoveryAction action,
                                                        ActionResponse response, ControllerActionState state) {
        return AvailabilityKvKeys.cleanupClass(namespace, nowMs, action, response, state, responseRetentionMs,
            actionCleanupGraceMs);
    }

    private boolean delete(String namespace, String key) {
        try {
            cleanupSink.delete(namespace, key);
            return true;
        } catch (RuntimeException e) {
            failureListener.onCleanupFailure(namespace, key, e);
            return false;
        }
    }

    public interface CleanupSink {
        void delete(String namespace, String key);
    }

    public interface CleanupFailureListener {
        void onCleanupFailure(String namespace, String key, RuntimeException exception);
    }
}
