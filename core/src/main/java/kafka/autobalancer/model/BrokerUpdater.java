/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.model;

import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.model.samples.SnapshotSamples;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BrokerUpdater extends AbstractInstanceUpdater {
    private final int brokerId;
    private final String rack;
    private final Map<Byte, SnapshotSamples> metricSequanceMap = new HashMap<>();
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

    public boolean isActive() {
        return this.active;
    }

    public Map<Byte, SnapshotSamples> metricSequenceMap() {
        return this.metricSequanceMap;
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
    protected boolean validateMetrics(Map<Byte, Double> metricsMap) {
        return metricsMap.keySet().containsAll(RawMetricTypes.BROKER_METRICS);
    }

    @Override
    protected boolean isValidInstance() {
        return active;
    }

    @Override
    protected void update0(Map<Byte, Double> metricsMap, long timestamp) {
        super.update0(metricsMap, timestamp);
        for (Map.Entry<Byte, Double> entry : metricsMap.entrySet()) {
            if (!RawMetricTypes.BROKER_METRICS.contains(entry.getKey())) {
                continue;
            }
            SnapshotSamples metric = metricSequanceMap.computeIfAbsent(entry.getKey(), k -> new SnapshotSamples());
            metric.append(entry.getValue());
        }
    }

    @Override
    protected String name() {
        return "broker-" + brokerId;
    }

    @Override
    protected AbstractInstance createInstance() {
        return new Broker(brokerId, rack, timestamp, getMetricsSnapshot());
    }

    protected Map<Byte, Snapshot> getMetricsSnapshot() {
        Map<Byte, Snapshot> snapshotMap = new HashMap<>();
        for (Map.Entry<Byte, SnapshotSamples> entry : metricSequanceMap.entrySet()) {
            snapshotMap.put(entry.getKey(), entry.getValue().snapshot());
        }
        return snapshotMap;
    }

    public static class Broker extends AbstractInstance {
        private final int brokerId;
        private final String rack;
        private final Map<Byte, Snapshot> metricsSnapshot;
        private boolean isSlowBroker;

        public Broker(int brokerId, String rack, long timestamp, Map<Byte, Snapshot> metricsSnapshot) {
            super(timestamp);
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
                    ", slow=" + isSlowBroker +
                    ", " + timeString() +
                    ", " + loadString() +
                    "}";
        }

        @Override
        public Broker copy() {
            Broker broker = new Broker(brokerId, rack, timestamp, null);
            broker.copyLoads(this);
            return broker;
        }

        @Override
        public String toString() {
            return "Broker{" +
                    "brokerId=" + brokerId +
                    ", slow=" + isSlowBroker +
                    ", " + super.toString() +
                    "}";
        }
    }
}
