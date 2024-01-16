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

package kafka.autobalancer.model;

import kafka.autobalancer.common.RawMetricType;
import kafka.autobalancer.common.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BrokerUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerUpdater.class);
    private final Lock lock = new ReentrantLock();
    private final Broker broker;

    public BrokerUpdater(int brokerId) {
        this.broker = new Broker(brokerId);
    }

    public static class Broker {
        private final int brokerId;
        private final double[] brokerCapacity = new double[Resource.cachedValues().size()];
        private final double[] brokerLoad = new double[Resource.cachedValues().size()];
        private final Set<Resource> resources = new HashSet<>();
        private final Map<RawMetricType, Double> metricsMap = new HashMap<>();
        private boolean active;
        private long timestamp;

        public Broker(int brokerId) {
            this.brokerId = brokerId;
            Arrays.fill(this.brokerCapacity, Resource.IGNORED_CAPACITY_VALUE);
        }

        public Broker(Broker other) {
            this.brokerId = other.brokerId;
            System.arraycopy(other.brokerCapacity, 0, this.brokerCapacity, 0, other.brokerCapacity.length);
            System.arraycopy(other.brokerLoad, 0, this.brokerLoad, 0, other.brokerLoad.length);
            this.resources.addAll(other.resources);
            this.active = other.active;
            this.timestamp = other.timestamp;
        }

        public int getBrokerId() {
            return this.brokerId;
        }

        public void setCapacity(Resource resource, double value) {
            this.brokerCapacity[resource.id()] = value;
        }

        public double capacity(Resource resource) {
            return this.brokerCapacity[resource.id()];
        }

        public Set<Resource> getResources() {
            return resources;
        }

        public void setLoad(Resource resource, double value) {
            resources.add(resource);
            this.brokerLoad[resource.id()] = value;
        }

        public void setMetricValue(RawMetricType metricType, double value) {
            this.metricsMap.put(metricType, value);
        }

        public double load(Resource resource) {
            if (!resources.contains(resource)) {
                return 0.0;
            }
            return this.brokerLoad[resource.id()];
        }

        public void reduceLoad(Resource resource, double delta) {
            this.brokerLoad[resource.id()] -= delta;
        }

        public void addLoad(Resource resource, double delta) {
            resources.add(resource);
            this.brokerLoad[resource.id()] += delta;
        }

        public double utilizationFor(Resource resource) {
            double capacity = capacity(resource);
            if (capacity == Resource.IGNORED_CAPACITY_VALUE) {
                return 0.0;
            }
            if (capacity == 0.0) {
                return Double.MAX_VALUE;
            }
            return load(resource) / capacity(resource);
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public boolean isActive() {
            return this.active;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return this.timestamp;
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

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{brokerId=")
                    .append(brokerId)
                    .append(", active=").append(active)
                    .append(", timestamp=").append(timestamp)
                    .append(", Capacities=[");
            for (int i = 0; i < brokerCapacity.length; i++) {
                builder.append(Resource.of(i).resourceString(brokerCapacity[i]));
                if (i != brokerCapacity.length - 1) {
                    builder.append(", ");
                }
            }
            builder.append("], Loads=[");
            for (int i = 0; i < brokerLoad.length; i++) {
                builder.append(Resource.of(i).resourceString(brokerLoad[i]));
                if (i != brokerLoad.length - 1) {
                    builder.append(", ");
                }
            }
            builder.append("]");
            int i = 0;
            for (Map.Entry<RawMetricType, Double> entry : metricsMap.entrySet()) {
                if (i == 0) {
                    builder.append(" Metrics={");
                }
                builder.append(entry.getKey())
                        .append("=")
                        .append(entry.getValue());
                if (i != metricsMap.size() - 1) {
                    builder.append(", ");
                }
                i++;
            }
            builder.append("}");
            return builder.toString();
        }
    }

    public boolean update(Map<RawMetricType, Double> metricsMap, long time) {
        if (!metricsMap.keySet().containsAll(RawMetricType.brokerMetricTypes())) {
            LOGGER.error("Broker metrics for broker={} sanity check failed, metrics is incomplete {}", broker.getBrokerId(), metricsMap.keySet());
            return false;
        }

        lock.lock();
        try {
            if (time < broker.getTimestamp()) {
                LOGGER.warn("Outdated broker metrics at time {} for broker={}, last updated time {}", time, broker.getBrokerId(), broker.getTimestamp());
                return false;
            }
            for (Map.Entry<RawMetricType, Double> entry : metricsMap.entrySet()) {
                if (entry.getKey().metricScope() != RawMetricType.MetricScope.BROKER) {
                    continue;
                }
                switch (entry.getKey()) {
                    case BROKER_CAPACITY_NW_IN:
                        broker.setCapacity(Resource.NW_IN, entry.getValue());
                        break;
                    case BROKER_CAPACITY_NW_OUT:
                        broker.setCapacity(Resource.NW_OUT, entry.getValue());
                        break;
                    case ALL_TOPIC_BYTES_IN:
                        broker.setLoad(Resource.NW_IN, entry.getValue());
                        break;
                    case ALL_TOPIC_BYTES_OUT:
                        broker.setLoad(Resource.NW_OUT, entry.getValue());
                        break;
                    case BROKER_CPU_UTIL:
                        broker.setLoad(Resource.CPU, entry.getValue());
                        break;
                    default:
                        broker.setMetricValue(entry.getKey(), entry.getValue());
                        break;
                }
            }
            broker.setTimestamp(time);
        } finally {
            lock.unlock();
        }

        LOGGER.debug("Successfully updated on broker {} at time {}", broker.getBrokerId(), broker.getTimestamp());
        return true;
    }

    public Broker get() {
        Broker broker;
        lock.lock();
        try {
            broker = new Broker(this.broker);
        } finally {
            lock.unlock();
        }
        return broker;
    }

    public Broker get(long timeSince) {
        Broker broker;
        lock.lock();
        try {
            // Broker is fenced or in shutdown.
            // For fenced or shutting-down brokers, the replicationControlManager will handle the movement of partitions.
            // So we do not need to consider them in auto-balancer.
            if (!this.broker.isActive()) {
                return null;
            }
            if (this.broker.timestamp < timeSince) {
                LOGGER.warn("Broker {} metrics is out of sync, expected earliest time: {}, actual: {}",
                        this.broker.getBrokerId(), timeSince, this.broker.timestamp);
                return null;
            }
            broker = new Broker(this.broker);
        } finally {
            lock.unlock();
        }
        return broker;
    }

    public void setActive(boolean active) {
        lock.lock();
        try {
            this.broker.setActive(active);
        } finally {
            lock.unlock();
        }
    }


}
