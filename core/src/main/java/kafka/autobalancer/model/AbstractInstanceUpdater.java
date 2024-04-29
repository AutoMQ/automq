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


import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.types.Resource;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractInstanceUpdater {
    protected static final Logger LOGGER = new LogContext().logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
    protected final Lock lock = new ReentrantLock();

    protected abstract boolean validateMetrics(Map<Byte, Double> metricsMap);

    protected abstract AbstractInstance instance();

    public boolean update(Map<Byte, Double> metricsMap, long time) {
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
        return true;
    }

    public AbstractInstance get() {
        return get(-1);
    }

    public AbstractInstance get(long timeSince) {
        lock.lock();
        try {
            if (this.instance().getTimestamp() < timeSince) {
                return null;
            }
            if (!isValidInstance()) {
                return null;
            }
            return this.instance().copy();
        } finally {
            lock.unlock();
        }
    }

    protected abstract boolean isValidInstance();

    public static abstract class AbstractInstance {
        protected final Map<Byte, Double> loads = new HashMap<>();
        protected Map<Byte, Double> metricsMap = new HashMap<>();
        protected long timestamp = 0L;

        public AbstractInstance() {

        }

        public AbstractInstance(AbstractInstance other, boolean deepCopy) {
            this.loads.putAll(other.loads);
            this.timestamp = other.timestamp;
            if (deepCopy) {
                this.metricsMap.putAll(other.metricsMap);
            }
        }

        public abstract AbstractInstance copy(boolean deepCopy);

        public AbstractInstance copy() {
            return copy(true);
        }

        public void processMetrics() {
            for (Map.Entry<Byte, Double> entry : metricsMap.entrySet()) {
                processMetric(entry.getKey(), entry.getValue());
            }
        }

        public abstract void processMetric(byte metricType, double value);

        public void setLoad(byte resource, double value) {
            this.loads.put(resource, value);
        }

        public double load(byte resource) {
            return this.loads.getOrDefault(resource, 0.0);
        }

        public Map<Byte, Double> getLoads() {
            return this.loads;
        }

        public void update(Map<Byte, Double> metricsMap, long timestamp) {
            this.metricsMap = metricsMap;
            this.timestamp = timestamp;
        }

        public Map<Byte, Double> getMetricsMap() {
            return this.metricsMap;
        }

        public double ofValue(Byte metricType) {
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
            int index = 0;
            for (Map.Entry<Byte, Double> entry : loads.entrySet()) {
                builder.append(Resource.resourceString(entry.getKey(), entry.getValue()));
                if (index++ != loads.size() - 1) {
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
            for (Map.Entry<Byte, Double> entry : metricsMap.entrySet()) {
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
