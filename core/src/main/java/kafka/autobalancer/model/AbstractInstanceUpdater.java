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
    protected Map<Byte, Double> metricsMap = new HashMap<>();
    protected long timestamp = 0L;

    public boolean update(Map<Byte, Double> metricsMap, long time) {
        if (!validateMetrics(metricsMap)) {
            LOGGER.error("Metrics validation failed for: {}, metrics: {}", name(), metricsMap.keySet());
            return false;
        }

        lock.lock();
        try {
            if (time < timestamp) {
                LOGGER.warn("Metrics for {} is outdated at {}, last updated time {}", name(), time, timestamp);
                return false;
            }
            update0(metricsMap, time);
        } finally {
            lock.unlock();
        }
        return true;
    }

    protected void update0(Map<Byte, Double> metricsMap, long timestamp) {
        lock.lock();
        try {
            this.metricsMap = metricsMap;
            this.timestamp = timestamp;
        } finally {
            lock.unlock();
        }
    }

    public long getTimestamp() {
        long timestamp;
        lock.lock();
        try {
            timestamp = this.timestamp;
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
            if (timestamp < timeSince) {
                return null;
            }
            if (!isValidInstance()) {
                return null;
            }
            return createInstance();
        } finally {
            lock.unlock();
        }
    }

    protected abstract String name();

    protected abstract boolean validateMetrics(Map<Byte, Double> metricsMap);

    protected abstract AbstractInstance createInstance();

    protected abstract boolean isValidInstance();

    public static abstract class AbstractInstance {
        protected final Map<Byte, Double> loads = new HashMap<>();
        protected final long timestamp;

        public AbstractInstance(long timestamp) {
            this.timestamp = timestamp;
        }

        public abstract AbstractInstance copy();

        public void setLoad(byte resource, double value) {
            this.loads.put(resource, value);
        }

        public double load(byte resource) {
            return this.loads.getOrDefault(resource, 0.0);
        }

        public Map<Byte, Double> getLoads() {
            return this.loads;
        }

        protected void copyLoads(AbstractInstance other) {
            this.loads.putAll(other.loads);
        }

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

        @Override
        public String toString() {
            return timeString() + ", " + loadString();
        }
    }
}
