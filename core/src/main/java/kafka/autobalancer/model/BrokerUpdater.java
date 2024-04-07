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

import kafka.autobalancer.common.Resource;

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
        return true;
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
        private boolean active;

        public Broker(int brokerId, String rack, boolean active) {
            this.brokerId = brokerId;
            this.rack = rack;
            this.active = active;
        }

        public Broker(Broker other) {
            super(other);
            this.brokerId = other.brokerId;
            this.rack = other.rack;
            this.active = other.active;
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

        public void reduceLoad(Resource resource, double delta) {
            this.setLoad(resource, load(resource) - delta);
        }

        public void addLoad(Resource resource, double delta) {
            this.setLoad(resource, load(resource) + delta);
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
                    ", " + timeString() +
                    ", " + loadString() +
                    "}";
        }

        @Override
        public Broker copy() {
            return new Broker(this);
        }

        @Override
        public void processMetrics() {
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
                    ", " + super.toString() +
                    "}";
        }
    }
}
