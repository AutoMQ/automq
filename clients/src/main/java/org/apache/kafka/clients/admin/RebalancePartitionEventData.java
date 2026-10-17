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

import com.automq.events.RebalancePartitionEvent;

/**
 * POJO wrapper for {@link RebalancePartitionEvent} protobuf message.
 */
public class RebalancePartitionEventData {

    public static final String TYPE = "com.automq.ops.rebalance.partition";
    public static final String DATA_SCHEMA = RebalancePartitionEvent.getDescriptor().getFullName();

    private String rebalanceId;
    private String topicPartition;
    private int fromBroker;
    private int toBroker;
    private String reason;

    public RebalancePartitionEventData setRebalanceId(String rebalanceId) {
        this.rebalanceId = rebalanceId;
        return this;
    }

    public RebalancePartitionEventData setTopicPartition(String topicPartition) {
        this.topicPartition = topicPartition;
        return this;
    }

    public RebalancePartitionEventData setFromBroker(int fromBroker) {
        this.fromBroker = fromBroker;
        return this;
    }

    public RebalancePartitionEventData setToBroker(int toBroker) {
        this.toBroker = toBroker;
        return this;
    }

    public RebalancePartitionEventData setReason(String reason) {
        this.reason = reason;
        return this;
    }

    public String rebalanceId() {
        return rebalanceId;
    }

    public String topicPartition() {
        return topicPartition;
    }

    public int fromBroker() {
        return fromBroker;
    }

    public int toBroker() {
        return toBroker;
    }

    public String reason() {
        return reason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RebalancePartitionEvent{");
        sb.append("rebalanceId='").append(rebalanceId).append('\'');
        sb.append(", topicPartition='").append(topicPartition).append('\'');
        sb.append(", fromBroker=").append(fromBroker);
        sb.append(", toBroker=").append(toBroker);
        if (reason != null) sb.append(", reason='").append(reason).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public byte[] toByteArray() {
        RebalancePartitionEvent.Builder b = RebalancePartitionEvent.newBuilder();
        if (rebalanceId != null) b.setRebalanceId(rebalanceId);
        if (topicPartition != null) b.setTopicPartition(topicPartition);
        b.setFromBroker(fromBroker);
        b.setToBroker(toBroker);
        if (reason != null) b.setReason(reason);
        return b.build().toByteArray();
    }

    public static RebalancePartitionEventData fromByteArray(byte[] data) {
        try {
            RebalancePartitionEvent e = RebalancePartitionEvent.parseFrom(data);
            return new RebalancePartitionEventData()
                .setRebalanceId(e.getRebalanceId())
                .setTopicPartition(e.getTopicPartition())
                .setFromBroker(e.getFromBroker())
                .setToBroker(e.getToBroker())
                .setReason(e.getReason());
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse RebalancePartitionEvent", ex);
        }
    }
}
