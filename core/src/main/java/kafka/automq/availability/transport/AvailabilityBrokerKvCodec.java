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

package kafka.automq.availability.transport;

import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData.PutKVRequest;
import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.AvailabilityCodecs;
import org.apache.kafka.controller.availability.AvailabilityConstants;
import org.apache.kafka.controller.availability.AvailabilityKvKeys;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.controller.availability.RecoveryAction;
import org.apache.kafka.image.KVImage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Owns Broker-side availability KV encoding: action collection plus signal/response request construction.
 */
public class AvailabilityBrokerKvCodec {
    public List<RecoveryAction> collectActions(KVImage kvImage) {
        List<RecoveryAction> actions = new ArrayList<>();
        kvImage.namespaceKVs(AvailabilityConstants.ACTION_NAMESPACE).values().forEach(value ->
            actions.add(AvailabilityCodecs.decodeAction(toByteArray(value))));
        return actions;
    }

    public static PutKVsRequestData putSignalRequest(BrokerAvailabilitySnapshot signal) {
        return putRequest(AvailabilityConstants.SIGNAL_NAMESPACE,
            AvailabilityKvKeys.signalKey(signal.getBrokerId()), AvailabilityCodecs.encodeSignal(signal));
    }

    public static PutKVsRequestData putActionResponseRequest(ActionResponse response) {
        return putRequest(AvailabilityConstants.RESPONSE_NAMESPACE,
            AvailabilityKvKeys.responseKey(response.getActionUuid()), AvailabilityCodecs.encodeResponse(response));
    }

    public static DeleteKVsRequestData deleteBrokerActionRequest(String actionUuid) {
        return deleteRequest(AvailabilityConstants.ACTION_NAMESPACE, actionUuid);
    }

    private static PutKVsRequestData putRequest(String namespace, String key, byte[] value) {
        return new PutKVsRequestData().setPutKVRequests(Collections.singletonList(new PutKVRequest()
                .setNamespace(namespace)
                .setKey(key)
                .setValue(value)
                .setOverwrite(true)));
    }

    private static DeleteKVsRequestData deleteRequest(String namespace, String key) {
        return new DeleteKVsRequestData().setDeleteKVRequests(Collections.singletonList(new DeleteKVsRequestData.DeleteKVRequest()
                .setNamespace(namespace)
                .setKey(key)));
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
