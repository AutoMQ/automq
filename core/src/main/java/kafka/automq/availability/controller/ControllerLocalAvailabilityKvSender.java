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

import org.apache.kafka.common.message.DeleteKVsRequestData;
import org.apache.kafka.common.message.PutKVsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Writes availability KV mutations directly through the active Controller.
 */
public class ControllerLocalAvailabilityKvSender implements AvailabilityKvRequestSender {
    private final Controller controller;

    public ControllerLocalAvailabilityKvSender(Controller controller) {
        this.controller = controller;
    }

    @Override
    public CompletableFuture<Void> put(PutKVsRequestData request) {
        return controller.putKVs(context(), request).thenAccept(response -> {
            Errors topLevel = Errors.forCode(response.errorCode());
            if (topLevel != Errors.NONE) {
                throw new RuntimeException("top-level error " + topLevel);
            }
            response.putKVResponses().stream()
                .map(item -> Errors.forCode(item.errorCode()))
                .filter(error -> error != Errors.NONE)
                .findFirst()
                .ifPresent(error -> {
                    throw new RuntimeException("sub-response error " + error);
                });
        });
    }

    @Override
    public CompletableFuture<Void> delete(DeleteKVsRequestData request) {
        return controller.deleteKVs(context(), request).thenAccept(response -> {
            Errors topLevel = Errors.forCode(response.errorCode());
            if (topLevel != Errors.NONE) {
                throw new RuntimeException("top-level error " + topLevel);
            }
            response.deleteKVResponses().stream()
                .map(item -> Errors.forCode(item.errorCode()))
                .filter(error -> error != Errors.NONE && error != Errors.KEY_NOT_EXIST)
                .findFirst()
                .ifPresent(error -> {
                    throw new RuntimeException("sub-response error " + error);
                });
        });
    }

    private ControllerRequestContext context() {
        return new ControllerRequestContext(null, null, OptionalLong.empty());
    }
}
