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

package org.apache.kafka.controller;

import org.apache.kafka.common.es.ElasticStreamSwitch;
import org.apache.kafka.controller.errors.ControllerExceptions;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import com.automq.stream.utils.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Owns periodic scheduling and ephemeral pacing state for Elastic controlled-shutdown drains.
 * The scheduler only triggers work; all Controller state access and mutation runs through a
 * {@link QuorumController.ControllerWriteOperation} on the Controller event queue.
 * This manager cancels its scheduled task when closed but does not own the supplied scheduler.
 */
final class GentleControlledShutdownControlManager implements AutoCloseable {
    static final String EVENT_NAME = "gentleControlledShutdownDrain";

    private static final Logger LOGGER =
        LoggerFactory.getLogger(GentleControlledShutdownControlManager.class);
    private static final long INTERVAL_SECONDS = 1L;

    private final QuorumController quorumController;
    private final ClusterControlManager clusterControl;
    private final ReplicationControlManager replicationControl;
    private final ScheduledFuture<?> scheduledFuture;
    private final Map<Integer, BrokerDrainState> drainStates = new HashMap<>();

    GentleControlledShutdownControlManager(
        QuorumController quorumController,
        ClusterControlManager clusterControl,
        ReplicationControlManager replicationControl
    ) {
        this(quorumController, clusterControl, replicationControl,
            Threads.COMMON_SCHEDULER);
    }

    GentleControlledShutdownControlManager(
        QuorumController quorumController,
        ClusterControlManager clusterControl,
        ReplicationControlManager replicationControl,
        ScheduledExecutorService scheduler
    ) {
        this.quorumController = quorumController;
        this.clusterControl = clusterControl;
        this.replicationControl = replicationControl;
        this.scheduledFuture = scheduler.scheduleWithFixedDelay(
            this::runDrainTask, INTERVAL_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    void activate() {
        drainStates.clear();
    }

    void deactivate() {
        drainStates.clear();
    }

    private void runDrainTask() {
        if (!ElasticStreamSwitch.isEnabled() || !quorumController.isActive()) {
            return;
        }
        try {
            quorumController.appendWriteEvent(
                EVENT_NAME, OptionalLong.empty(), new DrainOperation()).whenComplete((ignored, e) -> {
                    if (e == null) {
                        return;
                    }
                    Throwable cause = e instanceof CompletionException && e.getCause() != null
                        ? e.getCause() : e;
                    if (ControllerExceptions.isNotControllerException(cause)) {
                        LOGGER.debug("Skipping gentle controlled-shutdown drain because this "
                            + "Controller is no longer active");
                    } else {
                        LOGGER.warn("Gentle controlled-shutdown drain task failed", cause);
                    }
                });
        } catch (Throwable e) {
            LOGGER.warn("Unable to submit gentle controlled-shutdown drain task", e);
        }
    }

    /**
     * Cancel future drain triggers without shutting down the shared scheduler.
     */
    @Override
    public void close() {
        scheduledFuture.cancel(false);
    }

    private final class DrainOperation implements QuorumController.ControllerWriteOperation<Void> {
        private final List<Integer> drainedBrokers = new ArrayList<>();

        @Override
        public ControllerResult<Void> generateRecordsAndResult() {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            if (!ElasticStreamSwitch.isEnabled()) {
                drainStates.clear();
                return ControllerResult.of(records, null);
            }
            Map<Integer, BrokerRegistration> registrations = clusterControl.brokerRegistrations();
            drainStates.keySet().removeIf(brokerId -> !registrations.containsKey(brokerId));
            for (BrokerRegistration broker : registrations.values()) {
                if (!broker.inControlledShutdown()
                        || !replicationControl.hasControlledShutdownLeaders(broker.id())) {
                    drainStates.remove(broker.id());
                    continue;
                }
                BrokerDrainState drainState = drainStates.get(broker.id());
                if (drainState == null || drainState.brokerEpoch != broker.epoch()) {
                    int leaderCount = replicationControl.controlledShutdownLeaderCount(broker.id());
                    drainState = new BrokerDrainState(
                        broker.epoch(), Math.max((leaderCount + 59) / 60, 100));
                    drainStates.put(broker.id(), drainState);
                }
                ControllerResult<Void> result = replicationControl.maybeDrainControlledShutdownBroker(
                    broker.id(), drainState.batchTarget);
                if (!result.records().isEmpty()) {
                    records.addAll(result.records());
                    drainedBrokers.add(broker.id());
                }
            }
            return ControllerResult.of(records, null);
        }

        @Override
        public void processBatchEndOffset(long offset) {
            for (int brokerId : drainedBrokers) {
                clusterControl.heartbeatManager().advanceControlledShutdownOffset(brokerId, offset);
            }
        }
    }

    private static final class BrokerDrainState {
        private final long brokerEpoch;
        private final int batchTarget;

        private BrokerDrainState(long brokerEpoch, int batchTarget) {
            this.brokerEpoch = brokerEpoch;
            this.batchTarget = batchTarget;
        }
    }
}
