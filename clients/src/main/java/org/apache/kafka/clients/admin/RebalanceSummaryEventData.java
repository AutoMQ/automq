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

import com.automq.events.RebalanceSummaryEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * POJO wrapper for {@link RebalanceSummaryEvent} protobuf message.
 */
public class RebalanceSummaryEventData {

    public static final String TYPE = "com.automq.ops.rebalance.summary";
    public static final String DATA_SCHEMA = RebalanceSummaryEvent.getDescriptor().getFullName();

    private String rebalanceId;
    private String triggerReason;
    private int partitionCount;
    private Map<Integer, Double> brokerLoadBefore = new HashMap<>();
    private Map<Integer, Double> brokerLoadAfter = new HashMap<>();

    public RebalanceSummaryEventData setRebalanceId(String rebalanceId) {
        this.rebalanceId = rebalanceId;
        return this;
    }

    public RebalanceSummaryEventData setTriggerReason(String triggerReason) {
        this.triggerReason = triggerReason;
        return this;
    }

    public RebalanceSummaryEventData setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
        return this;
    }

    public RebalanceSummaryEventData setBrokerLoadBefore(Map<Integer, Double> brokerLoadBefore) {
        this.brokerLoadBefore = brokerLoadBefore != null ? brokerLoadBefore : new HashMap<>();
        return this;
    }

    public RebalanceSummaryEventData setBrokerLoadAfter(Map<Integer, Double> brokerLoadAfter) {
        this.brokerLoadAfter = brokerLoadAfter != null ? brokerLoadAfter : new HashMap<>();
        return this;
    }

    public String rebalanceId() {
        return rebalanceId;
    }

    public String triggerReason() {
        return triggerReason;
    }

    public int partitionCount() {
        return partitionCount;
    }

    public Map<Integer, Double> brokerLoadBefore() {
        return brokerLoadBefore;
    }

    public Map<Integer, Double> brokerLoadAfter() {
        return brokerLoadAfter;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RebalanceSummaryEvent{");
        sb.append("rebalanceId='").append(rebalanceId).append('\'');
        sb.append(", triggerReason='").append(triggerReason).append('\'');
        sb.append(", partitionCount=").append(partitionCount);
        if (!brokerLoadBefore.isEmpty()) sb.append(", brokerLoadBefore=").append(brokerLoadBefore);
        if (!brokerLoadAfter.isEmpty()) sb.append(", brokerLoadAfter=").append(brokerLoadAfter);
        sb.append('}');
        return sb.toString();
    }

    public byte[] toByteArray() {
        RebalanceSummaryEvent.Builder b = RebalanceSummaryEvent.newBuilder();
        if (rebalanceId != null) b.setRebalanceId(rebalanceId);
        if (triggerReason != null) b.setTriggerReason(triggerReason);
        b.setPartitionCount(partitionCount);
        b.putAllBrokerLoadBefore(brokerLoadBefore);
        b.putAllBrokerLoadAfter(brokerLoadAfter);
        return b.build().toByteArray();
    }

    public static RebalanceSummaryEventData fromByteArray(byte[] data) {
        try {
            RebalanceSummaryEvent e = RebalanceSummaryEvent.parseFrom(data);
            return new RebalanceSummaryEventData()
                .setRebalanceId(e.getRebalanceId())
                .setTriggerReason(e.getTriggerReason())
                .setPartitionCount(e.getPartitionCount())
                .setBrokerLoadBefore(new HashMap<>(e.getBrokerLoadBeforeMap()))
                .setBrokerLoadAfter(new HashMap<>(e.getBrokerLoadAfterMap()));
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse RebalanceSummaryEvent", ex);
        }
    }
}
