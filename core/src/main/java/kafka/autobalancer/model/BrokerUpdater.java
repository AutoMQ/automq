/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.autobalancer.model;

import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.model.samples.Samples;
import kafka.autobalancer.model.samples.SingleValueSamples;
import kafka.autobalancer.model.samples.SnapshottableSamples;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BrokerUpdater extends AbstractInstanceUpdater {
    private final int brokerId;
    private String rack;
    private boolean active;

    public BrokerUpdater(int brokerId, String rack, boolean active) {
        this.brokerId = brokerId;
        this.rack = rack;
        this.active = active;
    }

    public int brokerId() {
        return this.brokerId;
    }

    public String rack() {
        return this.rack;
    }

    public void setRack(String rack) {
        this.rack = rack;
    }

    public boolean isActive() {
        lock.lock();
        try {
            return this.active;
        } finally {
            lock.unlock();
        }
    }

    public void setActive(boolean active) {
        lock.lock();
        try {
            this.active = active;
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected boolean processMetric(byte metricType, double value) {
        if (metricType == RawMetricTypes.BROKER_METRIC_VERSION) {
            this.metricVersion = MetricVersion.of((short) value);
        }
        return true;
    }

    @Override
    protected Samples createSample(byte metricType) {
        switch (metricType) {
            case RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS:
            case RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS:
            case RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS:
                return new SnapshottableSamples();
            default:
                return new SingleValueSamples();
        }
    }

    @Override
    protected String name() {
        return "broker-" + brokerId;
    }

    @Override
    protected AbstractInstance createInstance(boolean metricsOutOfDate) {
        if (!active) {
            return null;
        }
        return new Broker(brokerId, rack, lastUpdateTimestamp, getMetricsSnapshot(), metricVersion, metricsOutOfDate);
    }

    @Override
    protected Set<Byte> requiredMetrics() {
        return metricVersion.requiredBrokerMetrics();
    }

    protected Map<Byte, Snapshot> getMetricsSnapshot() {
        Map<Byte, Snapshot> snapshotMap = new HashMap<>();
        for (Map.Entry<Byte, Samples> entry : metricSampleMap.entrySet()) {
            byte metricType = entry.getKey();
            if (metricType == RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS
                || metricType == RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS
                || metricType == RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS) {
                SnapshottableSamples snapshottableSamples = (SnapshottableSamples) entry.getValue();
                snapshotMap.put(entry.getKey(), snapshottableSamples.snapshot());
            }
        }
        return snapshotMap;
    }

    public static class Broker extends AbstractInstance {
        private final int brokerId;
        private final String rack;
        private final Map<Byte, Snapshot> metricsSnapshot;
        private boolean isSlowBroker;

        public Broker(int brokerId, String rack, long timestamp, Map<Byte, Snapshot> metricsSnapshot,
            MetricVersion metricVersion, boolean metricsOutOfDate) {
            super(timestamp, metricVersion, metricsOutOfDate);
            this.brokerId = brokerId;
            this.rack = rack;
            this.metricsSnapshot = metricsSnapshot;
            this.isSlowBroker = false;
        }

        public int getBrokerId() {
            return this.brokerId;
        }

        public String getRack() {
            return this.rack;
        }

        public boolean isSlowBroker() {
            return isSlowBroker;
        }

        public void setSlowBroker(boolean isSlowBroker) {
            this.isSlowBroker = isSlowBroker;
        }

        public Map<Byte, Snapshot> getMetricsSnapshot() {
            return this.metricsSnapshot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Broker broker = (Broker) o;
            return brokerId == broker.brokerId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(brokerId);
        }

        public String shortString() {
            return "Broker{" +
                    "brokerId=" + brokerId +
                    ", outOfDate=" + metricsOutOfDate +
                    ", slow=" + isSlowBroker +
                    ", " + timeString() +
                    ", " + loadString() +
                    "}";
        }

        @Override
        public Broker copy() {
            Broker broker = new Broker(brokerId, rack, timestamp, null, metricVersion, metricsOutOfDate);
            broker.copyLoads(this);
            return broker;
        }

        @Override
        public String toString() {
            return "Broker{" +
                    "brokerId=" + brokerId +
                    ", outOfDate=" + metricsOutOfDate +
                    ", slow=" + isSlowBroker +
                    ", " + super.toString() +
                    "}";
        }
    }
}
