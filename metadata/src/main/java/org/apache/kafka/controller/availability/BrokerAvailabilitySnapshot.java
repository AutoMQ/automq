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

package org.apache.kafka.controller.availability;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerAvailabilitySnapshot {
    private int schemaVersion = AvailabilityConstants.SCHEMA_VERSION;
    private int brokerId;
    private long brokerEpoch;
    private long timestampMs;
    private long windowStartMs;
    private long windowEndMs;
    private List<AvailabilitySignal> signals = new ArrayList<>();

    public BrokerAvailabilitySnapshot() {
    }

    public BrokerAvailabilitySnapshot(int brokerId, long brokerEpoch, long timestampMs, long windowStartMs,
                                      long windowEndMs, List<AvailabilitySignal> signals) {
        this.schemaVersion = AvailabilityConstants.SCHEMA_VERSION;
        this.brokerId = brokerId;
        this.brokerEpoch = brokerEpoch;
        this.timestampMs = timestampMs;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
        this.signals = signals;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public long getBrokerEpoch() {
        return brokerEpoch;
    }

    public void setBrokerEpoch(long brokerEpoch) {
        this.brokerEpoch = brokerEpoch;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public long getWindowStartMs() {
        return windowStartMs;
    }

    public void setWindowStartMs(long windowStartMs) {
        this.windowStartMs = windowStartMs;
    }

    public long getWindowEndMs() {
        return windowEndMs;
    }

    public void setWindowEndMs(long windowEndMs) {
        this.windowEndMs = windowEndMs;
    }

    public List<AvailabilitySignal> getSignals() {
        return signals;
    }

    public void setSignals(List<AvailabilitySignal> signals) {
        this.signals = signals;
    }

    public boolean hasSignal(AvailabilitySignalType type) {
        return signals.stream().anyMatch(signal -> signal.getType() == type);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BrokerAvailabilitySnapshot)) {
            return false;
        }
        BrokerAvailabilitySnapshot that = (BrokerAvailabilitySnapshot) o;
        return schemaVersion == that.schemaVersion &&
            brokerId == that.brokerId &&
            brokerEpoch == that.brokerEpoch &&
            timestampMs == that.timestampMs &&
            windowStartMs == that.windowStartMs &&
            windowEndMs == that.windowEndMs &&
            Objects.equals(signals, that.signals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, brokerId, brokerEpoch, timestampMs, windowStartMs, windowEndMs, signals);
    }
}
