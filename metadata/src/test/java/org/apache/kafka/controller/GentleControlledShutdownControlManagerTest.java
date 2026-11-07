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
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class GentleControlledShutdownControlManagerTest {
    /**
     * Given a periodic gentle-drain trigger with no drain work, when Elastic mode is enabled,
     * then the active Controller runs a record-free scan without a heartbeat wakeup.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testPeriodicTriggerRunsIdleScanWithoutHeartbeatWakeup() throws Exception {
        QuorumController controller = mock(QuorumController.class);
        ClusterControlManager clusterControl = mock(ClusterControlManager.class);
        ReplicationControlManager replicationControl = mock(ReplicationControlManager.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<Object> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(scheduler)
            .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        when(clusterControl.brokerRegistrations()).thenReturn(Map.of());
        when(controller.appendWriteEvent(
            any(), any(OptionalLong.class), any(QuorumController.ControllerWriteOperation.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        try (GentleControlledShutdownControlManager manager =
                 new GentleControlledShutdownControlManager(
                     controller, clusterControl, replicationControl, scheduler)) {
            var taskCaptor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).scheduleWithFixedDelay(
                taskCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
            ElasticStreamSwitch.setSwitch(true);
            manager.activate();
            when(controller.isActive()).thenReturn(true);
            taskCaptor.getValue().run();
            var operationCaptor = org.mockito.ArgumentCaptor
                .forClass(QuorumController.ControllerWriteOperation.class);
            verify(controller).appendWriteEvent(
                eq("gentleControlledShutdownDrain"), eq(OptionalLong.empty()),
                operationCaptor.capture());
            assertTrue(operationCaptor.getValue().generateRecordsAndResult().records().isEmpty());

            when(controller.isActive()).thenReturn(false);
            taskCaptor.getValue().run();
            verify(controller).appendWriteEvent(
                eq("gentleControlledShutdownDrain"), eq(OptionalLong.empty()),
                any(QuorumController.ControllerWriteOperation.class));
        } finally {
            ElasticStreamSwitch.setSwitch(false);
        }
    }

    /**
     * Given a scheduled drain whose Controller event is still pending, when the scheduler task
     * runs, then the scheduler does not block waiting for the Controller event.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testSchedulerDoesNotWaitForControllerEventCompletion() throws Exception {
        QuorumController controller = mock(QuorumController.class);
        ClusterControlManager clusterControl = mock(ClusterControlManager.class);
        ReplicationControlManager replicationControl = mock(ReplicationControlManager.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<Object> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(scheduler)
            .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        BrokerRegistration broker = controlledShutdownBroker(0, 1L);
        when(clusterControl.brokerRegistrations()).thenReturn(Map.of(0, broker));
        when(replicationControl.hasControlledShutdownLeaders(0)).thenReturn(true);
        when(controller.isActive()).thenReturn(true);
        CompletableFuture<Void> controllerEvent = new CompletableFuture<>();
        when(controller.appendWriteEvent(
            any(), any(OptionalLong.class), any(QuorumController.ControllerWriteOperation.class)))
            .thenReturn(controllerEvent);

        try (GentleControlledShutdownControlManager manager =
                 new GentleControlledShutdownControlManager(
                     controller, clusterControl, replicationControl, scheduler)) {
            var taskCaptor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).scheduleWithFixedDelay(
                taskCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
            ElasticStreamSwitch.setSwitch(true);
            manager.activate();

            CompletableFuture<Void> schedulerRun = CompletableFuture.runAsync(taskCaptor.getValue());
            schedulerRun.get(1, TimeUnit.SECONDS);
            assertFalse(controllerEvent.isDone());
            verify(controller).appendWriteEvent(eq("gentleControlledShutdownDrain"),
                eq(OptionalLong.empty()), any(QuorumController.ControllerWriteOperation.class));
        } finally {
            ElasticStreamSwitch.setSwitch(false);
        }
    }

    /**
     * Given two Brokers are draining concurrently, when one periodic operation runs, then each
     * Broker receives its independently computed batch and shares the real batch-end barrier.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testConcurrentBrokersReceiveIndependentBatches() throws Exception {
        QuorumController controller = mock(QuorumController.class);
        ClusterControlManager clusterControl = mock(ClusterControlManager.class);
        ReplicationControlManager replicationControl = mock(ReplicationControlManager.class);
        BrokerHeartbeatManager heartbeatManager = mock(BrokerHeartbeatManager.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<Object> scheduledFuture = mock(ScheduledFuture.class);
        BrokerRegistration firstBroker = controlledShutdownBroker(0, 10L);
        BrokerRegistration secondBroker = controlledShutdownBroker(1, 20L);
        ApiMessageAndVersion firstRecord = mock(ApiMessageAndVersion.class);
        ApiMessageAndVersion secondRecord = mock(ApiMessageAndVersion.class);
        doReturn(scheduledFuture).when(scheduler)
            .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        when(clusterControl.brokerRegistrations()).thenReturn(Map.of(0, firstBroker, 1, secondBroker));
        when(clusterControl.heartbeatManager()).thenReturn(heartbeatManager);
        when(replicationControl.hasControlledShutdownLeaders(0)).thenReturn(true);
        when(replicationControl.hasControlledShutdownLeaders(1)).thenReturn(true);
        when(replicationControl.controlledShutdownLeaderCount(0)).thenReturn(150);
        when(replicationControl.controlledShutdownLeaderCount(1)).thenReturn(6001);
        when(replicationControl.maybeDrainControlledShutdownBroker(0, 100))
            .thenReturn(ControllerResult.of(List.of(firstRecord), null));
        when(replicationControl.maybeDrainControlledShutdownBroker(1, 101))
            .thenReturn(ControllerResult.of(List.of(secondRecord), null));
        when(controller.isActive()).thenReturn(true);
        when(controller.appendWriteEvent(
            any(), any(OptionalLong.class), any(QuorumController.ControllerWriteOperation.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        try (GentleControlledShutdownControlManager manager =
                 new GentleControlledShutdownControlManager(
                     controller, clusterControl, replicationControl, scheduler)) {
            var taskCaptor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).scheduleWithFixedDelay(
                taskCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
            ElasticStreamSwitch.setSwitch(true);
            manager.activate();
            taskCaptor.getValue().run();

            var operationCaptor = org.mockito.ArgumentCaptor
                .forClass(QuorumController.ControllerWriteOperation.class);
            verify(controller).appendWriteEvent(eq("gentleControlledShutdownDrain"),
                eq(OptionalLong.empty()), operationCaptor.capture());
            ControllerResult<?> result = operationCaptor.getValue().generateRecordsAndResult();
            assertEquals(2, result.records().size());
            assertTrue(result.records().contains(firstRecord));
            assertTrue(result.records().contains(secondRecord));

            operationCaptor.getValue().processBatchEndOffset(42L);
            verify(heartbeatManager).advanceControlledShutdownOffset(0, 42L);
            verify(heartbeatManager).advanceControlledShutdownOffset(1, 42L);
        } finally {
            ElasticStreamSwitch.setSwitch(false);
        }
    }

    /**
     * Given a Broker retains leaders across a Controller lifecycle change, when the next tick
     * runs, then its fixed batch target is recomputed from the remaining leaders.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testLifecycleChangeRecomputesBatchTarget() throws Exception {
        QuorumController controller = mock(QuorumController.class);
        ClusterControlManager clusterControl = mock(ClusterControlManager.class);
        ReplicationControlManager replicationControl = mock(ReplicationControlManager.class);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<Object> scheduledFuture = mock(ScheduledFuture.class);
        BrokerRegistration broker = controlledShutdownBroker(0, 10L);
        doReturn(scheduledFuture).when(scheduler)
            .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
        when(clusterControl.brokerRegistrations()).thenReturn(Map.of(0, broker));
        when(replicationControl.hasControlledShutdownLeaders(0)).thenReturn(true);
        when(replicationControl.controlledShutdownLeaderCount(0)).thenReturn(6001, 5900);
        when(replicationControl.maybeDrainControlledShutdownBroker(anyInt(), anyInt()))
            .thenReturn(ControllerResult.of(List.of(), null));
        when(controller.isActive()).thenReturn(true);
        when(controller.appendWriteEvent(
            any(), any(OptionalLong.class), any(QuorumController.ControllerWriteOperation.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        try (GentleControlledShutdownControlManager manager =
                 new GentleControlledShutdownControlManager(
                     controller, clusterControl, replicationControl, scheduler)) {
            var taskCaptor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).scheduleWithFixedDelay(
                taskCaptor.capture(), eq(1L), eq(1L), eq(TimeUnit.SECONDS));
            ElasticStreamSwitch.setSwitch(true);
            manager.activate();

            taskCaptor.getValue().run();
            var operationCaptor = org.mockito.ArgumentCaptor
                .forClass(QuorumController.ControllerWriteOperation.class);
            verify(controller).appendWriteEvent(any(), any(OptionalLong.class), operationCaptor.capture());
            operationCaptor.getValue().generateRecordsAndResult();
            verify(replicationControl).maybeDrainControlledShutdownBroker(0, 101);

            manager.deactivate();
            manager.activate();
            clearInvocations(controller, replicationControl);
            taskCaptor.getValue().run();
            verify(controller).appendWriteEvent(any(), any(OptionalLong.class), operationCaptor.capture());
            operationCaptor.getValue().generateRecordsAndResult();
            verify(replicationControl).maybeDrainControlledShutdownBroker(0, 100);
        } finally {
            ElasticStreamSwitch.setSwitch(false);
        }
    }

    private static BrokerRegistration controlledShutdownBroker(int brokerId, long brokerEpoch) {
        BrokerRegistration broker = mock(BrokerRegistration.class);
        when(broker.id()).thenReturn(brokerId);
        when(broker.epoch()).thenReturn(brokerEpoch);
        when(broker.inControlledShutdown()).thenReturn(true);
        return broker;
    }
}
