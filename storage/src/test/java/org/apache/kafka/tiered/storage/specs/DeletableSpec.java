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
package org.apache.kafka.tiered.storage.specs;

import org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent;

import java.util.Objects;

public final class DeletableSpec {

    private final Integer sourceBrokerId;
    private final LocalTieredStorageEvent.EventType eventType;
    private final Integer eventCount;

    public DeletableSpec(Integer sourceBrokerId,
                         LocalTieredStorageEvent.EventType eventType,
                         Integer eventCount) {
        this.sourceBrokerId = sourceBrokerId;
        this.eventType = eventType;
        this.eventCount = eventCount;
    }

    public Integer getSourceBrokerId() {
        return sourceBrokerId;
    }

    public LocalTieredStorageEvent.EventType getEventType() {
        return eventType;
    }

    public Integer getEventCount() {
        return eventCount;
    }

    @Override
    public String toString() {
        return "DeletableSpec{" +
                "sourceBrokerId=" + sourceBrokerId +
                ", eventType=" + eventType +
                ", eventCount=" + eventCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletableSpec that = (DeletableSpec) o;
        return Objects.equals(sourceBrokerId, that.sourceBrokerId)
                && eventType == that.eventType
                && Objects.equals(eventCount, that.eventCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceBrokerId, eventType, eventCount);
    }
}
