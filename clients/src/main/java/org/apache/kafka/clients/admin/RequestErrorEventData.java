/*
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
package org.apache.kafka.clients.admin;

import com.automq.events.RequestErrorEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * POJO wrapper for {@link RequestErrorEvent} protobuf message.
 * <p>
 * This class hides all protobuf references so that modules with conflicting
 * protobuf versions (shaded vs unshaded) can build and serialize
 * {@code RequestErrorEvent} without directly touching protobuf classes.
 */
public class RequestErrorEventData {

    public static final String TYPE = "com.automq.risk.request_error";
    public static final String DATA_SCHEMA = RequestErrorEvent.getDescriptor().getFullName();

    private int apiKey;
    private int errorCode;
    private String resource;
    private List<String> clientIps = Collections.emptyList();
    private List<String> clientIds = Collections.emptyList();
    private double rps;
    private String reason;

    public RequestErrorEventData setApiKey(int apiKey) {
        this.apiKey = apiKey;
        return this;
    }

    public RequestErrorEventData setErrorCode(int errorCode) {
        this.errorCode = errorCode;
        return this;
    }

    public RequestErrorEventData setResource(String resource) {
        this.resource = resource;
        return this;
    }

    public RequestErrorEventData setClientIps(List<String> clientIps) {
        this.clientIps = clientIps != null ? clientIps : Collections.emptyList();
        return this;
    }

    public RequestErrorEventData setClientIds(List<String> clientIds) {
        this.clientIds = clientIds != null ? clientIds : Collections.emptyList();
        return this;
    }

    public RequestErrorEventData setRps(double rps) {
        this.rps = rps;
        return this;
    }

    public RequestErrorEventData setReason(String reason) {
        this.reason = reason;
        return this;
    }

    public int apiKey() {
        return apiKey;
    }

    public int errorCode() {
        return errorCode;
    }

    public String resource() {
        return resource;
    }

    public List<String> clientIps() {
        return clientIps;
    }

    public List<String> clientIds() {
        return clientIds;
    }

    public double rps() {
        return rps;
    }

    public String reason() {
        return reason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RequestErrorEvent{");
        sb.append("apiKey=").append(apiKey);
        sb.append(", errorCode=").append(errorCode);
        if (resource != null) sb.append(", resource='").append(resource).append('\'');
        if (!clientIps.isEmpty()) sb.append(", clientIps=").append(clientIps);
        if (!clientIds.isEmpty()) sb.append(", clientIds=").append(clientIds);
        sb.append(", rps=").append(rps);
        if (reason != null) sb.append(", reason='").append(reason).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Serialize to protobuf byte array.
     */
    public byte[] toByteArray() {
        RequestErrorEvent.Builder b = RequestErrorEvent.newBuilder()
            .setApiKey(apiKey)
            .setErrorCode(errorCode)
            .addAllClientIps(clientIps)
            .addAllClientIds(clientIds)
            .setRps(rps);
        if (resource != null) {
            b.setResource(resource);
        }
        if (reason != null) {
            b.setReason(reason);
        }
        return b.build().toByteArray();
    }

    /**
     * Deserialize from protobuf byte array.
     */
    public static RequestErrorEventData fromByteArray(byte[] data) {
        try {
            RequestErrorEvent event = RequestErrorEvent.parseFrom(data);
            return new RequestErrorEventData()
                .setApiKey(event.getApiKey())
                .setErrorCode(event.getErrorCode())
                .setResource(event.hasResource() ? event.getResource() : null)
                .setClientIps(new ArrayList<>(event.getClientIpsList()))
                .setClientIds(new ArrayList<>(event.getClientIdsList()))
                .setRps(event.getRps())
                .setReason(event.hasReason() ? event.getReason() : null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse RequestErrorEvent", e);
        }
    }
}
