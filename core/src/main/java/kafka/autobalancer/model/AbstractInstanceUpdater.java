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


import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.RawMetricType;
import kafka.autobalancer.common.Resource;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractInstanceUpdater {
    protected static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    protected final Lock lock = new ReentrantLock();

    protected abstract boolean validateMetrics(Map<RawMetricType, Double> metricsMap);

    protected abstract AbstractInstance instance();

    public boolean update(Map<RawMetricType, Double> metricsMap, long time) {
        if (!validateMetrics(metricsMap)) {
            LOGGER.error("Metrics validation failed for: {}, metrics: {}", this.instance().name(), metricsMap.keySet());
            return false;
        }

        lock.lock();
        try {
            if (time < this.instance().getTimestamp()) {
                LOGGER.warn("Metrics for {} is outdated at {}, last updated time {}", this.instance().name(), time,
                        this.instance().getTimestamp());
                return false;
            }
            this.instance().update(metricsMap, time);
        } finally {
            lock.unlock();
        }
        LOGGER.debug("Successfully updated on {} at time {}", this.instance().name(), this.instance().getTimestamp());
        return true;
    }

    public AbstractInstance get() {
        return get(-1);
    }

    public AbstractInstance get(long timeSince) {
        lock.lock();
        try {
            if (this.instance().getTimestamp() < timeSince) {
                LOGGER.debug("Metrics for {} is out of sync, expected earliest time: {}, actual: {}",
                        this.instance().name(), timeSince, this.instance().getTimestamp());
                return null;
            }
            if (!isValidInstance()) {
                LOGGER.debug("Metrics for {} is invalid", this.instance().name());
                return null;
            }
            return this.instance().copy();
        } finally {
            lock.unlock();
        }
    }

    protected abstract boolean isValidInstance();

    public static abstract class AbstractInstance {
        protected final double[] loads = new double[Resource.cachedValues().size()];
        protected final Set<Resource> resources = new HashSet<>();
        protected Map<RawMetricType, Double> metricsMap = new HashMap<>();
        protected long timestamp = 0L;

        public AbstractInstance() {

        }

        public AbstractInstance(AbstractInstance other) {
            System.arraycopy(other.loads, 0, this.loads, 0, loads.length);
            this.resources.addAll(other.resources);
            this.metricsMap.putAll(other.metricsMap);
            this.timestamp = other.timestamp;
        }

        public abstract AbstractInstance copy();

        public abstract void processMetrics();

        public void setLoad(Resource resource, double value) {
            this.resources.add(resource);
            this.loads[resource.id()] = value;
        }

        public double load(Resource resource) {
            if (!this.resources.contains(resource)) {
                return 0.0;
            }
            return this.loads[resource.id()];
        }

        public Set<Resource> getResources() {
            return this.resources;
        }

        public void update(Map<RawMetricType, Double> metricsMap, long timestamp) {
            this.metricsMap = metricsMap;
            this.timestamp = timestamp;
        }

        public Map<RawMetricType, Double> getMetricsMap() {
            return this.metricsMap;
        }

        public double ofValue(RawMetricType metricType) {
            return this.metricsMap.getOrDefault(metricType, 0.0);
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        protected abstract String name();

        protected String timeString() {
            return "timestamp=" + timestamp;
        }

        protected String loadString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Loads={");
            for (int i = 0; i < loads.length; i++) {
                builder.append(Resource.of(i).resourceString(loads[i]));
                if (i != loads.length - 1) {
                    builder.append(", ");
                }
            }
            builder.append("}");
            return builder.toString();
        }

        protected String metricsString() {
            StringBuilder builder = new StringBuilder();
            builder.append("Metrics={");
            int i = 0;
            for (Map.Entry<RawMetricType, Double> entry : metricsMap.entrySet()) {
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

        @Override
        public String toString() {
            return timeString() +
                    ", " +
                    loadString() +
                    ", " +
                    metricsString();
        }
    }
}
