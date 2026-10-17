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

import com.automq.events.FailoverEvent;

/**
 * POJO wrapper for {@link FailoverEvent} protobuf message.
 */
public class FailoverEventData {

    public static final String TYPE = "com.automq.ops.failover";
    public static final String DATA_SCHEMA = FailoverEvent.getDescriptor().getFullName();

    private int failedNodeId;
    private long detectedTimestamp;
    private long completedTimestamp;
    private String detail;

    public FailoverEventData setFailedNodeId(int failedNodeId) {
        this.failedNodeId = failedNodeId;
        return this;
    }

    public FailoverEventData setDetectedTimestamp(long detectedTimestamp) {
        this.detectedTimestamp = detectedTimestamp;
        return this;
    }

    public FailoverEventData setCompletedTimestamp(long completedTimestamp) {
        this.completedTimestamp = completedTimestamp;
        return this;
    }

    public FailoverEventData setDetail(String detail) {
        this.detail = detail;
        return this;
    }

    public int failedNodeId() {
        return failedNodeId;
    }

    public long detectedTimestamp() {
        return detectedTimestamp;
    }

    public long completedTimestamp() {
        return completedTimestamp;
    }

    public String detail() {
        return detail;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("FailoverEvent{");
        sb.append("failedNodeId=").append(failedNodeId);
        sb.append(", detectedTimestamp=").append(detectedTimestamp);
        sb.append(", completedTimestamp=").append(completedTimestamp);
        if (detail != null) sb.append(", detail='").append(detail).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public byte[] toByteArray() {
        FailoverEvent.Builder b = FailoverEvent.newBuilder()
            .setFailedNodeId(failedNodeId)
            .setDetectedTimestamp(detectedTimestamp)
            .setCompletedTimestamp(completedTimestamp);
        if (detail != null) b.setDetail(detail);
        return b.build().toByteArray();
    }

    public static FailoverEventData fromByteArray(byte[] data) {
        try {
            FailoverEvent e = FailoverEvent.parseFrom(data);
            return new FailoverEventData()
                .setFailedNodeId(e.getFailedNodeId())
                .setDetectedTimestamp(e.getDetectedTimestamp())
                .setCompletedTimestamp(e.getCompletedTimestamp())
                .setDetail(e.getDetail());
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse FailoverEvent", ex);
        }
    }
}
