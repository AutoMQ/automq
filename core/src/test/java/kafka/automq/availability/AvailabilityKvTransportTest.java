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

package kafka.automq.availability;

import kafka.automq.availability.controller.AvailabilityKvTransport;
import kafka.automq.availability.controller.AvailabilityKvCleanup;
import kafka.automq.availability.transport.AvailabilityBrokerKvCodec;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteKVsRequestData.DeleteKVRequest;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.controller.availability.ActionExecutionStatus;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityActionType;
import org.apache.kafka.controller.availability.AvailabilityCodecs;
import org.apache.kafka.controller.availability.AvailabilityConstants;
import org.apache.kafka.controller.availability.AvailabilityKvKeys;
import org.apache.kafka.controller.availability.AvailabilitySignal;
import org.apache.kafka.controller.availability.AvailabilitySignalType;
import org.apache.kafka.controller.availability.AvailabilityTarget;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.controller.stream.KVNamespace;
import org.apache.kafka.image.KVImage;
import org.apache.kafka.image.RegistryRef;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(40)
public class AvailabilityKvTransportTest {
    @Test
    public void testControllerTransportCollectsOnlyAvailabilityNamespaces() {
        BrokerAvailabilitySnapshot signal = new BrokerAvailabilitySnapshot(1, 10L, 100L, 70L, 100L,
            List.of(new AvailabilitySignal(AvailabilitySignalType.APPEND_STUCK, AvailabilityTarget.broker(1, 10L))));
        ActionResponse response = new ActionResponse(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(1, 10L), ActionExecutionStatus.SUCCEEDED, 100L, 120L, null);
        RecoveryAction controllerAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.PARTITION_REASSIGNMENT,
            AvailabilityTarget.topicPartition("topic", 0), -1, 100L, 200L, "test", false);
        ControllerActionState state = new ControllerActionState(controllerAction, null);

        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map = kvMap(registry);
        put(map, registry, AvailabilityConstants.SIGNAL_NAMESPACE, AvailabilityKvKeys.signalKey(1),
            ByteBuffer.wrap(AvailabilityCodecs.encodeSignal(signal)));
        put(map, registry, AvailabilityConstants.RESPONSE_NAMESPACE, AvailabilityKvKeys.responseKey(response.getActionUuid()),
            ByteBuffer.wrap(AvailabilityCodecs.encodeResponse(response)));
        put(map, registry, AvailabilityConstants.ACTION_NAMESPACE, AvailabilityKvKeys.actionKey(controllerAction.getActionUuid()),
            ByteBuffer.wrap(AvailabilityCodecs.encodeAction(controllerAction)));
        put(map, registry, AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE,
            AvailabilityKvKeys.controllerActionKey(controllerAction.getActionUuid()),
            ByteBuffer.wrap(AvailabilityCodecs.encodeControllerAction(state)));
        put(map, registry, "other", "ignored", ByteBuffer.wrap(new byte[] {1}));
        registry.getOrCreateSnapshot(0);

        AvailabilityKvTransport transport = transport(new KVImage(map, new RegistryRef(registry, 0, new ArrayList<>())));

        assertEquals(List.of(signal), transport.collectSignals());
        assertEquals(List.of(response), transport.collectActionResponses());
        assertEquals(List.of(controllerAction), transport.collectBrokerActions());
        assertEquals(List.of(state), transport.collectControllerActionStates());
    }

    @Test
    public void testBrokerTransportBuildsNamespaceAwarePutAndDeleteRequests() {
        AvailabilityBrokerKvCodec codec = new AvailabilityBrokerKvCodec();
        BrokerAvailabilitySnapshot signal = new BrokerAvailabilitySnapshot(2, 20L, 100L, 70L, 100L, List.of());
        ActionResponse response = new ActionResponse(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), ActionExecutionStatus.SUCCEEDED, 100L, 120L, null);

        PutKVRequest signalPut = codec.putSignalRequest(signal).putKVRequests().get(0);
        PutKVRequest responsePut = codec.putActionResponseRequest(response).putKVRequests().get(0);
        DeleteKVRequest actionDelete = codec.deleteBrokerActionRequest(response.getActionUuid().toString())
            .deleteKVRequests().get(0);

        assertEquals(AvailabilityConstants.SIGNAL_NAMESPACE, signalPut.namespace());
        assertEquals("2", signalPut.key());
        assertArrayEquals(AvailabilityCodecs.encodeSignal(signal), signalPut.value());
        assertEquals(AvailabilityConstants.RESPONSE_NAMESPACE, responsePut.namespace());
        assertEquals(response.getActionUuid().toString(), responsePut.key());
        assertArrayEquals(AvailabilityCodecs.encodeResponse(response), responsePut.value());
        assertEquals(AvailabilityConstants.ACTION_NAMESPACE, actionDelete.namespace());
        assertEquals(response.getActionUuid().toString(), actionDelete.key());
    }

    @Test
    public void testControllerTransportBuildsActionAndControllerStateRequests() {
        RecoveryAction brokerAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), 2, 100L, 200L, "node", false);
        RecoveryAction controllerAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.PARTITION_REASSIGNMENT,
            AvailabilityTarget.topicPartition("topic", 0), -1, 100L, 200L, "partition", false);
        ControllerActionState controllerState = new ControllerActionState(controllerAction, null);

        PutKVRequest brokerActionPut = AvailabilityKvTransport.putBrokerActionRequest(brokerAction).putKVRequests().get(0);
        PutKVRequest controllerActionPut = AvailabilityKvTransport.putControllerActionStateRequest(controllerState)
            .putKVRequests().get(0);

        assertEquals(AvailabilityConstants.ACTION_NAMESPACE, brokerActionPut.namespace());
        assertEquals(brokerAction.getActionUuid().toString(), brokerActionPut.key());
        assertEquals(brokerAction, AvailabilityCodecs.decodeAction(brokerActionPut.value()));
        assertEquals(AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE, controllerActionPut.namespace());
        assertEquals(controllerAction.getActionUuid().toString(), controllerActionPut.key());
        assertEquals(controllerState, AvailabilityCodecs.decodeControllerAction(controllerActionPut.value()));
    }

    @Test
    public void testControllerRuntimeFactorySignalSourceIncludesBrokerActions() {
        RecoveryAction brokerAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), 2, 100L, 200L, "node", false);
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map = kvMap(registry);
        put(map, registry, AvailabilityConstants.ACTION_NAMESPACE, AvailabilityKvKeys.actionKey(brokerAction.getActionUuid()),
            ByteBuffer.wrap(AvailabilityCodecs.encodeAction(brokerAction)));
        registry.getOrCreateSnapshot(0);
        AvailabilityKvTransport transport = transport(new KVImage(map, new RegistryRef(registry, 0, new ArrayList<>())));

        assertEquals(List.of(brokerAction), ControllerAvailabilityRuntimeFactory.signalSource(transport).collectBrokerActions());
    }

    @Test
    public void testAvailabilityKvReadersUseCurrentRetainedImageInsteadOfStaleSnapshot() {
        RecoveryAction staleAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(1, 10L), 1, 100L, 200L, "stale", false);
        RecoveryAction currentAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), 2, 100L, 200L, "current", false);
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map = kvMap(registry);
        put(map, registry, AvailabilityConstants.ACTION_NAMESPACE, AvailabilityKvKeys.actionKey(staleAction.getActionUuid()),
            ByteBuffer.wrap(AvailabilityCodecs.encodeAction(staleAction)));
        registry.getOrCreateSnapshot(0);
        KVImage staleImage = new KVImage(map, new RegistryRef(registry, 0, new ArrayList<>()));

        remove(map, AvailabilityConstants.ACTION_NAMESPACE, AvailabilityKvKeys.actionKey(staleAction.getActionUuid()));
        put(map, registry, AvailabilityConstants.ACTION_NAMESPACE, AvailabilityKvKeys.actionKey(currentAction.getActionUuid()),
            ByteBuffer.wrap(AvailabilityCodecs.encodeAction(currentAction)));
        registry.getOrCreateSnapshot(1);
        registry.deleteSnapshot(0);
        KVImage currentImage = new KVImage(map, new RegistryRef(registry, 1, new ArrayList<>()));

        assertThrows(RuntimeException.class, () -> new AvailabilityBrokerKvCodec().collectActions(staleImage));

        AtomicReference<KVImage> current = new AtomicReference<>(currentImage);
        AvailabilityKvTransport transport = new AvailabilityKvTransport(reader -> reader.accept(current.get()));
        assertEquals(List.of(currentAction), transport.collectBrokerActions());
        assertEquals(List.of(currentAction), new AvailabilityBrokerKvCodec().collectActions(current.get()));
    }

    @Test
    public void testCleanupDeletesExpiredStateAndReportsFailuresWithoutThrowing() {
        RecoveryAction expiredAction = new RecoveryAction(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), 2, 100L, 200L, "node", false);
        ActionResponse expiredResponse = new ActionResponse(Uuid.randomUuid(), AvailabilityActionType.NODE_EXIT,
            AvailabilityTarget.broker(2, 20L), ActionExecutionStatus.SUCCEEDED, 100L, 150L, null);
        RecoveryAction expiredControllerAction = new RecoveryAction(Uuid.randomUuid(),
            AvailabilityActionType.PARTITION_REASSIGNMENT, AvailabilityTarget.topicPartition("topic", 0), -1,
            100L, 200L, "partition", false);
        ControllerActionState expiredControllerState = new ControllerActionState(expiredControllerAction, null);
        List<String> deleted = new ArrayList<>();
        AtomicInteger failures = new AtomicInteger();
        AvailabilityKvCleanup cleanup = new AvailabilityKvCleanup((namespace, key) -> {
            if (AvailabilityConstants.RESPONSE_NAMESPACE.equals(namespace)) {
                throw new IllegalStateException("delete failed");
            }
            deleted.add(namespace + "/" + key);
        }, (namespace, key, exception) -> failures.incrementAndGet(), 600L, 600L);

        int deletedCount = cleanup.cleanup(1000L, List.of(expiredAction), List.of(expiredResponse),
            List.of(expiredControllerState));

        assertEquals(2, deletedCount);
        assertEquals(1, failures.get());
        assertEquals(List.of(
            AvailabilityConstants.ACTION_NAMESPACE + "/" + expiredAction.getActionUuid(),
            AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE + "/" + expiredControllerAction.getActionUuid()
        ), deleted);
    }

    private TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> kvMap(SnapshotRegistry registry) {
        return new TimelineHashMap<>(registry, 0);
    }

    private void put(
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map,
        SnapshotRegistry registry,
        String namespace,
        String key,
        ByteBuffer value
    ) {
        KVNamespace kvNamespace = KVNamespace.of(namespace);
        TimelineHashMap<String, ByteBuffer> namespaceKVs = map.get(kvNamespace);
        if (namespaceKVs == null) {
            namespaceKVs = new TimelineHashMap<>(registry, 0);
            map.put(kvNamespace, namespaceKVs);
        }
        namespaceKVs.put(key, value);
    }

    private void remove(
        TimelineHashMap<KVNamespace, TimelineHashMap<String, ByteBuffer>> map,
        String namespace,
        String key
    ) {
        TimelineHashMap<String, ByteBuffer> namespaceKVs = map.get(KVNamespace.of(namespace));
        if (namespaceKVs != null) {
            namespaceKVs.remove(key);
        }
    }

    private AvailabilityKvTransport transport(KVImage kvImage) {
        return new AvailabilityKvTransport(reader -> reader.accept(kvImage));
    }
}
