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
import org.apache.kafka.controller.availability.AvailabilityCodecs;
import org.apache.kafka.controller.availability.AvailabilityConstants;
import org.apache.kafka.controller.availability.AvailabilityKvKeys;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.ControllerActionState;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.image.KVImage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Reads availability KV namespaces from the Controller metadata image without making metadata depend on core runtime.
 */
public class AvailabilityKvTransport {
    private final KvImageReader kvImageReader;

    public AvailabilityKvTransport(KvImageReader kvImageReader) {
        this.kvImageReader = kvImageReader;
    }

    public List<BrokerAvailabilitySnapshot> collectSignals() {
        List<BrokerAvailabilitySnapshot> signals = new ArrayList<>();
        namespaceValues(AvailabilityConstants.SIGNAL_NAMESPACE)
            .forEach(value -> signals.add(AvailabilityCodecs.decodeSignal(toByteArray(value))));
        return signals;
    }

    public List<ActionResponse> collectActionResponses() {
        List<ActionResponse> responses = new ArrayList<>();
        namespaceValues(AvailabilityConstants.RESPONSE_NAMESPACE)
            .forEach(value -> responses.add(AvailabilityCodecs.decodeResponse(toByteArray(value))));
        return responses;
    }

    public List<ControllerActionState> collectControllerActionStates() {
        List<ControllerActionState> states = new ArrayList<>();
        namespaceValues(AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE)
            .forEach(value -> states.add(AvailabilityCodecs.decodeControllerAction(toByteArray(value))));
        return states;
    }

    public List<RecoveryAction> collectBrokerActions() {
        List<RecoveryAction> actions = new ArrayList<>();
        namespaceValues(AvailabilityConstants.ACTION_NAMESPACE)
            .forEach(value -> actions.add(AvailabilityCodecs.decodeAction(toByteArray(value))));
        return actions;
    }

    private List<ByteBuffer> namespaceValues(String namespace) {
        List<ByteBuffer> values = new ArrayList<>();
        kvImageReader.read(kvImage -> values.addAll(kvImage.namespaceKVs(namespace).values()));
        return values;
    }

    public interface KvImageReader {
        void read(Consumer<KVImage> reader);
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    public static PutKVsRequestData putBrokerActionRequest(RecoveryAction action) {
        return putRequest(AvailabilityConstants.ACTION_NAMESPACE,
            AvailabilityKvKeys.actionKey(action.getActionUuid()), AvailabilityCodecs.encodeAction(action));
    }

    public static PutKVsRequestData putActionResponseRequest(ActionResponse response) {
        return putRequest(AvailabilityConstants.RESPONSE_NAMESPACE,
            AvailabilityKvKeys.responseKey(response.getActionUuid()), AvailabilityCodecs.encodeResponse(response));
    }

    public static PutKVsRequestData putControllerActionStateRequest(ControllerActionState state) {
        return putRequest(AvailabilityConstants.CONTROLLER_ACTION_NAMESPACE,
            AvailabilityKvKeys.controllerActionKey(state.getAction().getActionUuid()),
            AvailabilityCodecs.encodeControllerAction(state));
    }

    private static PutKVsRequestData putRequest(String namespace, String key, byte[] value) {
        return new PutKVsRequestData().setPutKVRequests(Collections.singletonList(new PutKVRequest()
            .setNamespace(namespace)
            .setKey(key)
            .setValue(value)
            .setOverwrite(true)));
    }

    public static DeleteKVsRequestData deleteRequest(String namespace, String key) {
        return new DeleteKVsRequestData().setDeleteKVRequests(Collections.singletonList(new DeleteKVsRequestData.DeleteKVRequest()
            .setNamespace(namespace)
            .setKey(key)));
    }
}
