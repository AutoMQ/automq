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

import kafka.autobalancer.common.Resource;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricsUtils;
import kafka.autobalancer.metricsreporter.metric.RawMetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
        private boolean active;
        private long timestamp;

        public Broker(int brokerId) {
            this.brokerId = brokerId;
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
            if (capacity == 0) {
                return 0;
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
            return "Broker{" +
                    "brokerId=" + brokerId +
                    ", brokerCapacity=" + Arrays.toString(brokerCapacity) +
                    ", brokerLoad=" + Arrays.toString(brokerLoad) +
                    ", active=" + active +
                    '}';
        }
    }

    public boolean update(AutoBalancerMetrics metrics) {
        if (metrics.metricClassId() != AutoBalancerMetrics.MetricClassId.BROKER_METRIC) {
            LOGGER.error("Mismatched metrics type {} for broker", metrics.metricClassId());
            return false;
        }

        if (!MetricsUtils.sanityCheckBrokerMetricsCompleteness(metrics)) {
            LOGGER.error("Broker metrics sanity check failed, metrics is incomplete {}", metrics);
            return false;
        }

        lock.lock();
        try {
            if (metrics.time() < broker.getTimestamp()) {
                LOGGER.warn("Outdated metrics at time {}, last updated time {}", metrics.time(), broker.getTimestamp());
                return false;
            }
            for (Map.Entry<RawMetricType, Double> entry : metrics.getMetricTypeValueMap().entrySet()) {
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
                        LOGGER.error("Unsupported broker metrics type {}", entry.getKey());
                        break;
                }
            }
            broker.setTimestamp(metrics.time());
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

    public int id() {
        return this.broker.getBrokerId();
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
