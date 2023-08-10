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
/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter.metric;

import kafka.metrics.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;

import java.nio.ByteBuffer;


/**
 * An interface for all the raw metrics reported by {@link CruiseControlMetricsReporter}.
 */
public abstract class CruiseControlMetric {
    static final byte METRIC_VERSION = 0;
    private final RawMetricType rawMetricType;
    private final long time;
    private final int brokerId;
    private final double value;

    public CruiseControlMetric(RawMetricType rawMetricType, long time, int brokerId, double value) {
        this.rawMetricType = rawMetricType;
        this.time = time;
        this.brokerId = brokerId;
        this.value = value;
    }

    /**
     * @return the metric class id for this metric. The metric class id will be stored in the serialized metrics
     * so that the deserializer will know which class should be used to deserialize the data.
     */
    public abstract MetricClassId metricClassId();

    /**
     * @return the {@link RawMetricType} of this metric.
     */
    public RawMetricType rawMetricType() {
        return rawMetricType;
    }

    /**
     * @return the timestamp for this metric.
     */
    public long time() {
        return time;
    }

    /**
     * @return the broker id who reported this metric.
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * @return the metric value.
     */
    public double value() {
        return value;
    }

    /**
     * Serialize the metric to a byte buffer with the header size reserved.
     *
     * @param headerSize the header size to reserve.
     * @return A ByteBuffer with header size reserved at the beginning.
     */
    abstract ByteBuffer toBuffer(int headerSize);

    @Override
    public String toString() {
        return String.format("[RawMetricType=%s,Time=%d,BrokerId=%d,Value=%.4f]", rawMetricType, time, brokerId, value);
    }

    /**
     * An enum that list all the implementations of the interface. This id will be store in the serialized
     * metrics to help the metric sampler to decide using which class to deserialize the metric bytes.
     */
    public enum MetricClassId {
        BROKER_METRIC((byte) 0), TOPIC_METRIC((byte) 1), PARTITION_METRIC((byte) 2);

        private final byte id;

        MetricClassId(byte id) {
            this.id = id;
        }

        static MetricClassId forId(byte id) {
            if (id < values().length) {
                return values()[id];
            } else {
                throw new IllegalArgumentException("MetricClassId " + id + " does not exist.");
            }
        }

        byte id() {
            return id;
        }
    }
}
