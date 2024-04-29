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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BrokerUpdater extends AbstractInstanceUpdater {
    private final Broker broker;

    public BrokerUpdater(int brokerId, String rack, boolean active) {
        this.broker = createBroker(brokerId, rack, active);
    }

    public void setActive(boolean active) {
        lock.lock();
        try {
            this.broker.setActive(active);
        } finally {
            lock.unlock();
        }
    }

    protected Broker createBroker(int brokerId, String rack, boolean active) {
        return new Broker(brokerId, rack, active);
    }

    @Override
    protected boolean validateMetrics(Map<Byte, Double> metricsMap) {
        return metricsMap.keySet().containsAll(RawMetricTypes.BROKER_METRICS);
    }

    @Override
    protected AbstractInstance instance() {
        return this.broker;
    }

    @Override
    protected boolean isValidInstance() {
        return broker.isActive();
    }

    public static class Broker extends AbstractInstance {
        private final int brokerId;
        private final String rack;
        private final Map<Byte, MetricValueSequence> metrics = new HashMap<>();
        private boolean active;
        private boolean isSlowBroker;

        public Broker(int brokerId, String rack, boolean active) {
            this.brokerId = brokerId;
            this.rack = rack;
            this.active = active;
            this.isSlowBroker = false;
        }

        public Broker(Broker other, boolean deepCopy) {
            super(other, deepCopy);
            this.brokerId = other.brokerId;
            this.rack = other.rack;
            this.active = other.active;
            this.isSlowBroker = other.isSlowBroker;
            if (deepCopy) {
                for (Map.Entry<Byte, MetricValueSequence> entry : other.metrics.entrySet()) {
                    this.metrics.put(entry.getKey(), entry.getValue().copy());
                }
            }
        }

        public int getBrokerId() {
            return this.brokerId;
        }

        public String getRack() {
            return this.rack;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public boolean isActive() {
            return this.active;
        }

        public boolean isSlowBroker() {
            return isSlowBroker;
        }

        public void setSlowBroker(boolean isSlowBroker) {
            this.isSlowBroker = isSlowBroker;
        }

        @Override
        public void update(Map<Byte, Double> metricsMap, long timestamp) {
            super.update(metricsMap, timestamp);
            for (Map.Entry<Byte, Double> entry : metricsMap.entrySet()) {
                if (!RawMetricTypes.BROKER_METRICS.contains(entry.getKey())) {
                    continue;
                }
                MetricValueSequence metric = metrics.computeIfAbsent(entry.getKey(), k -> new MetricValueSequence());
                metric.append(entry.getValue());
            }
        }

        public Map<Byte, MetricValueSequence> getMetrics() {
            return metrics;
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
                    ", active=" + active +
                    ", slow=" + isSlowBroker +
                    ", " + timeString() +
                    ", " + loadString() +
                    "}";
        }

        @Override
        public Broker copy(boolean deepCopy) {
            return new Broker(this, deepCopy);
        }

        @Override
        public void processMetric(byte metricType, double value) {
            // do nothing
        }

        @Override
        protected String name() {
            return "broker-" + brokerId;
        }

        @Override
        public String toString() {
            return "Broker{" +
                    "brokerId=" + brokerId +
                    ", active=" + active +
                    ", slow=" + isSlowBroker +
                    ", " + super.toString() +
                    "}";
        }
    }
}
