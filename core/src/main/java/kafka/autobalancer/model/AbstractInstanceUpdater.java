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


import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.common.types.Resource;
import kafka.autobalancer.model.samples.Samples;

import com.automq.stream.utils.LogContext;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractInstanceUpdater {
    protected static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    protected final Lock lock = new ReentrantLock();
    protected Map<Byte, Samples> metricSampleMap = new HashMap<>();
    protected long lastUpdateTimestamp = 0L;
    protected MetricVersion metricVersion = defaultVersion();
    private final long createTimestamp;

    public AbstractInstanceUpdater() {
        this.createTimestamp = System.currentTimeMillis();
    }

    public long createTimestamp() {
        return createTimestamp;
    }

    public boolean update(Iterable<Map.Entry<Byte, Double>> metricsMap, long time) {
        lock.lock();
        try {
            if (time < lastUpdateTimestamp) {
                LOGGER.warn("Metrics for {} is outdated at {}, last updated time {}", name(), time, lastUpdateTimestamp);
                return false;
            }
            update0(metricsMap, time);
        } finally {
            lock.unlock();
        }
        return true;
    }

    protected MetricVersion defaultVersion() {
        return MetricVersion.V0;
    }

    public MetricVersion metricVersion() {
        return metricVersion;
    }

    protected void update0(Iterable<Map.Entry<Byte, Double>> metricsMap, long timestamp) {
        for (Map.Entry<Byte, Double> entry : metricsMap) {
            byte metricType = entry.getKey();
            double value = entry.getValue();
            if (!processMetric(metricType, value)) {
                continue;
            }
            metricSampleMap.computeIfAbsent(metricType, k -> createSample(metricType)).append(value, timestamp);
        }
        this.lastUpdateTimestamp = timestamp;
    }

    abstract boolean processMetric(byte metricType, double value);

    abstract Samples createSample(byte metricType);

    public long getLastUpdateTimestamp() {
        long timestamp;
        lock.lock();
        try {
            timestamp = this.lastUpdateTimestamp;
        } finally {
            lock.unlock();
        }
        return timestamp;
    }

    public AbstractInstance get() {
        return get(-1);
    }

    public AbstractInstance get(long timeSince) {
        lock.lock();
        try {
            return createInstance(lastUpdateTimestamp < timeSince || !isMetricsComplete());
        } finally {
            lock.unlock();
        }
    }

    protected abstract Set<Byte> requiredMetrics();

    protected boolean isMetricsComplete() {
        Set<Byte> requiredMetrics = requiredMetrics();
        if (!metricSampleMap.keySet().containsAll(requiredMetrics)) {
            LOGGER.warn("Metrics for {} of version {} is incomplete, expected: {}, actual: {}", name(),
                metricVersion(), requiredMetrics, metricSampleMap.keySet());
            return false;
        }
        return true;
    }

    protected abstract String name();

    protected abstract AbstractInstance createInstance(boolean metricsOutOfDate);

    public abstract static class AbstractInstance {
        protected final Map<Byte, Load> loads = new HashMap<>();
        protected final long timestamp;
        protected final MetricVersion metricVersion;
        protected boolean metricsOutOfDate;

        public AbstractInstance(long timestamp, MetricVersion metricVersion, boolean metricsOutOfDate) {
            this.timestamp = timestamp;
            this.metricVersion = metricVersion;
            this.metricsOutOfDate = metricsOutOfDate;
        }

        public boolean isMetricsOutOfDate() {
            return metricsOutOfDate;
        }

        public void setMetricsOutOfDate(boolean metricsOutOfDate) {
            this.metricsOutOfDate = metricsOutOfDate;
        }

        public abstract AbstractInstance copy();

        public void addLoad(byte resource, Load load) {
            this.loads.compute(resource, (k, v) -> {
                if (v == null) {
                    return new Load(load);
                }
                v.add(load);
                return v;
            });
        }

        public void reduceLoad(byte resource, Load load) {
            this.loads.compute(resource, (k, v) -> {
                if (v == null) {
                    return new Load(load);
                }
                v.reduceValue(load);
                return v;
            });
        }

        public void setLoad(byte resource, Load load) {
            this.loads.put(resource, load);
        }

        public void setLoad(byte resource, double value) {
            setLoad(resource, value, true);
        }

        public void setLoad(byte resource, double value, boolean trusted) {
            this.loads.put(resource, new Load(trusted, value));
        }

        public Load load(byte resource) {
            return loads.getOrDefault(resource, new Load(true, 0));
        }

        public double loadValue(byte resource) {
            Load load = loads.get(resource);
            return load == null ? 0 : load.getValue();
        }

        public Map<Byte, Load> getLoads() {
            return this.loads;
        }

        protected void copyLoads(AbstractInstance other) {
            for (Map.Entry<Byte, Load> entry : other.loads.entrySet()) {
                this.loads.put(entry.getKey(), new Load(entry.getValue()));
            }
        }

        public MetricVersion getMetricVersion() {
            return metricVersion;
        }

        protected String timeString() {
            return "timestamp=" + timestamp;
        }

        protected String loadString() {
            return "Loads={" +
                    buildLoadString() +
                    "}";
        }

        protected String buildLoadString() {
            StringBuilder builder = new StringBuilder();
            int index = 0;
            for (Map.Entry<Byte, Load> entry : loads.entrySet()) {
                String resourceStr = formatResourceStr(entry.getKey(), entry.getValue().getValue());
                builder.append(resourceStr);
                builder.append(" (");
                builder.append(entry.getValue().isTrusted() ? "trusted" : "untrusted");
                builder.append(")");
                if (index++ != loads.size() - 1) {
                    builder.append(", ");
                }
            }
            return builder.toString();
        }

        public String deltaLoadString(AbstractInstance other) {
            StringBuilder builder = new StringBuilder();
            int index = 0;
            for (Map.Entry<Byte, Load> entry : loads.entrySet()) {
                byte resource = entry.getKey();
                double loadValue = entry.getValue().getValue();
                double delta = loadValue;
                Load otherLoad = other.loads.get(resource);
                if (otherLoad != null) {
                    delta -= otherLoad.getValue();
                }
                String resourceStr = formatResourceStr(resource, loadValue);
                builder.append(resourceStr);
                builder.append(" (");
                builder.append(delta > 0 ? "+" : "-");
                builder.append(formatValueStr(resource, Math.abs(delta)));
                builder.append(")");
                if (index++ != loads.size() - 1) {
                    builder.append(", ");
                }
            }
            return builder.toString();
        }

        protected String formatResourceStr(byte resource, double value) {
            return Resource.resourceString(resource, value);
        }

        protected String formatValueStr(byte resource, double value) {
            return Resource.valueString(resource, value);
        }

        @Override
        public String toString() {
            return timeString() + ", " + loadString();
        }
    }

    public static class Load {
        private boolean trusted;
        private double value;

        public Load(boolean trusted, double value) {
            this.trusted = trusted;
            this.value = value;
        }

        public Load(Load other) {
            this.trusted = other.trusted;
            this.value = other.value;
        }

        public boolean isTrusted() {
            return trusted;
        }

        public void setTrusted(boolean isTrusted) {
            this.trusted = isTrusted;
        }

        public double getValue() {
            return value;
        }

        public void add(Load load) {
            this.value += load.value;
            this.trusted &= load.trusted;
        }

        public void reduceValue(Load load) {
            this.value -= load.value;
        }
    }
}
