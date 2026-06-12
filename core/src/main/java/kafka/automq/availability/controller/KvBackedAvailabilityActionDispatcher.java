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

import kafka.automq.availability.transport.AvailabilityKvRequestSender;

import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.controller.availability.RecoveryDecision;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persists broker-executed actions to KV and executes controller-local actions through the controller executor.
 */
public class KvBackedAvailabilityActionDispatcher implements ControllerAvailabilityReconciler.ActionDispatcher {
    private final AvailabilityKvRequestSender sender;
    private final ControllerActionExecutor controllerActionExecutor;
    private final Set<AvailabilityActionType> controllerActionTypes;
    private final BrokerActionOwnerResolver brokerActionOwnerResolver;
    private final Set<String> dispatchedActionIds = ConcurrentHashMap.newKeySet();

    public KvBackedAvailabilityActionDispatcher(AvailabilityKvRequestSender sender,
                                                ControllerActionExecutor controllerActionExecutor,
                                                Set<AvailabilityActionType> controllerActionTypes) {
        this(sender, controllerActionExecutor, controllerActionTypes, action -> action.getExecutorBrokerId());
    }

    public KvBackedAvailabilityActionDispatcher(AvailabilityKvRequestSender sender,
                                                ControllerActionExecutor controllerActionExecutor,
                                                Set<AvailabilityActionType> controllerActionTypes,
                                                BrokerActionOwnerResolver brokerActionOwnerResolver) {
        this.sender = sender;
        this.controllerActionExecutor = controllerActionExecutor;
        this.controllerActionTypes = controllerActionTypes;
        this.brokerActionOwnerResolver = brokerActionOwnerResolver;
    }

    @Override
    public void dispatch(RecoveryDecision decision) {
        RecoveryAction action = decision.action();
        String actionId = action.getActionUuid().toString();
        if (!dispatchedActionIds.add(actionId)) {
            return;
        }
        if (action.isDryRun()) {
            ActionResponse response = new ActionResponse(action.getActionUuid(), action.getActionType(),
                action.getTarget(), ActionExecutionStatus.DRY_RUN, action.getCreatedAtMs(), action.getCreatedAtMs(),
                null);
            sender.put(AvailabilityKvTransport.putControllerActionStateRequest(new ControllerActionState(action, response)))
                .whenComplete((ignored, exception) -> clearDispatchOnFailure(actionId, exception));
            return;
        }
        if (controllerActionTypes.contains(action.getActionType())) {
            controllerActionExecutor.execute(action).thenAccept(response ->
                sender.put(AvailabilityKvTransport.putControllerActionStateRequest(new ControllerActionState(action, response)))
                    .whenComplete((ignored, exception) -> clearDispatchOnFailure(actionId, exception)));
        } else {
            sender.put(AvailabilityKvTransport.putBrokerActionRequest(resolveBrokerExecutor(action)))
                .whenComplete((ignored, exception) -> clearDispatchOnFailure(actionId, exception));
        }
    }

    private RecoveryAction resolveBrokerExecutor(RecoveryAction action) {
        if (action.getTarget().getKind() == AvailabilityTarget.Kind.BROKER) {
            return action;
        }
        int executorBrokerId = brokerActionOwnerResolver.executorBrokerId(action);
        if (executorBrokerId == action.getExecutorBrokerId()) {
            return action;
        }
        return new RecoveryAction(action.getActionUuid(), action.getActionType(), action.getTarget(),
            executorBrokerId, action.getCreatedAtMs(), action.getDeadlineMs(), action.getReason(), action.isDryRun());
    }

    private void clearDispatchOnFailure(String actionId, Throwable exception) {
        if (exception != null) {
            dispatchedActionIds.remove(actionId);
        }
    }

    public interface BrokerActionOwnerResolver {
        int executorBrokerId(RecoveryAction action);
    }
}
