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

import kafka.autobalancer.common.Action;
import kafka.autobalancer.common.ActionType;
import kafka.autobalancer.executor.ActionExecutorService;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.controller.availability.RecoveryAction;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Converts availability partition reassignment actions to the existing Controller auto-balancer executor path.
 */
public class AvailabilityPartitionReassignmentAdapter {
    private final ActionExecutorService actionExecutorService;
    private final PartitionReassignmentTargetSelector targetSelector;
    private final CandidateBrokerSupplier candidateBrokerSupplier;
    private final CurrentOwnerSupplier currentOwnerSupplier;

    public AvailabilityPartitionReassignmentAdapter(ActionExecutorService actionExecutorService,
                                                    PartitionReassignmentTargetSelector targetSelector,
                                                    CandidateBrokerSupplier candidateBrokerSupplier,
                                                    CurrentOwnerSupplier currentOwnerSupplier) {
        this.actionExecutorService = actionExecutorService;
        this.targetSelector = targetSelector;
        this.candidateBrokerSupplier = candidateBrokerSupplier;
        this.currentOwnerSupplier = currentOwnerSupplier;
    }

    public CompletableFuture<Void> execute(RecoveryAction action) {
        TopicPartition topicPartition = new TopicPartition(action.getTarget().getTopic(),
            action.getTarget().getPartition());
        int currentOwner = currentOwnerSupplier.currentOwner(topicPartition);
        int destination = targetSelector.select(currentOwner, candidateBrokerSupplier.candidateBrokers())
            .orElseThrow(() -> new IllegalStateException("no eligible partition reassignment destination"));
        return actionExecutorService.execute(List.of(new Action(ActionType.MOVE, topicPartition, currentOwner, destination)));
    }

    public interface CandidateBrokerSupplier {
        List<PartitionReassignmentTargetSelector.CandidateBroker> candidateBrokers();
    }

    public interface CurrentOwnerSupplier {
        int currentOwner(TopicPartition topicPartition);
    }
}
