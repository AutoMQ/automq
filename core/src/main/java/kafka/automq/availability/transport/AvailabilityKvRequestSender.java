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

import org.apache.kafka.controller.availability.ActionResponse;
import org.apache.kafka.controller.availability.BrokerAvailabilitySnapshot;
import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;

import java.util.concurrent.CompletableFuture;

/**
 * Sends availability KV mutations to the Controller and completes only after the request result is known.
 */
public interface AvailabilityKvRequestSender {
    CompletableFuture<Void> put(PutKVsRequestData request);

    CompletableFuture<Void> delete(DeleteKVsRequestData request);

    default CompletableFuture<Void> putSignal(BrokerAvailabilitySnapshot signal) {
        return put(AvailabilityBrokerKvCodec.putSignalRequest(signal));
    }

    default CompletableFuture<Void> putActionResponse(ActionResponse response) {
        return put(AvailabilityBrokerKvCodec.putActionResponseRequest(response));
    }

    default CompletableFuture<Void> deleteBrokerAction(String actionUuid) {
        return delete(AvailabilityBrokerKvCodec.deleteBrokerActionRequest(actionUuid));
    }
}
