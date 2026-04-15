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

import com.automq.events.OffsetCommitFrequencyEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * POJO wrapper for {@link OffsetCommitFrequencyEvent} protobuf message.
 */
public class OffsetCommitFrequencyEventData {

    public static final String TYPE = "com.automq.risk.offset_commit_frequency";
    public static final String DATA_SCHEMA = OffsetCommitFrequencyEvent.getDescriptor().getFullName();

    private List<String> clientIps = Collections.emptyList();
    private List<String> clientIds = Collections.emptyList();
    private String groupId;
    private String topic;
    private double rps;

    public OffsetCommitFrequencyEventData setClientIps(List<String> clientIps) {
        this.clientIps = clientIps != null ? clientIps : Collections.emptyList();
        return this;
    }

    public OffsetCommitFrequencyEventData setClientIds(List<String> clientIds) {
        this.clientIds = clientIds != null ? clientIds : Collections.emptyList();
        return this;
    }

    public OffsetCommitFrequencyEventData setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public OffsetCommitFrequencyEventData setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public OffsetCommitFrequencyEventData setRps(double rps) {
        this.rps = rps;
        return this;
    }

    public List<String> clientIps() {
        return clientIps;
    }

    public List<String> clientIds() {
        return clientIds;
    }

    public String groupId() {
        return groupId;
    }

    public String topic() {
        return topic;
    }

    public double rps() {
        return rps;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OffsetCommitFrequencyEvent{");
        if (!clientIps.isEmpty()) sb.append("clientIps=").append(clientIps).append(", ");
        if (!clientIds.isEmpty()) sb.append("clientIds=").append(clientIds).append(", ");
        sb.append("groupId='").append(groupId).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", rps=").append(rps);
        sb.append('}');
        return sb.toString();
    }

    public byte[] toByteArray() {
        OffsetCommitFrequencyEvent.Builder b = OffsetCommitFrequencyEvent.newBuilder()
            .addAllClientIps(clientIps)
            .addAllClientIds(clientIds)
            .setRps(rps);
        if (groupId != null) b.setGroupId(groupId);
        if (topic != null) b.setTopic(topic);
        return b.build().toByteArray();
    }

    public static OffsetCommitFrequencyEventData fromByteArray(byte[] data) {
        try {
            OffsetCommitFrequencyEvent e = OffsetCommitFrequencyEvent.parseFrom(data);
            return new OffsetCommitFrequencyEventData()
                .setClientIps(new ArrayList<>(e.getClientIpsList()))
                .setClientIds(new ArrayList<>(e.getClientIdsList()))
                .setGroupId(e.getGroupId())
                .setTopic(e.getTopic())
                .setRps(e.getRps());
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse OffsetCommitFrequencyEvent", ex);
        }
    }
}
