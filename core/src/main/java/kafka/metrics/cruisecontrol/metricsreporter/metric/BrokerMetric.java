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

import kafka.metrics.cruisecontrol.metricsreporter.exception.UnknownVersionException;

import java.nio.ByteBuffer;


/**
 * A container class to hold broker metric.
 */
public class BrokerMetric extends CruiseControlMetric {

    public BrokerMetric(RawMetricType rawMetricType, long time, int brokerId, double value) {
        super(rawMetricType, time, brokerId, value);
        if (rawMetricType.metricScope() != RawMetricType.MetricScope.BROKER) {
            throw new IllegalArgumentException(String.format("Cannot construct a BrokerMetric for %s whose scope is %s",
                    rawMetricType, rawMetricType.metricScope()));
        }
    }

    static BrokerMetric fromBuffer(ByteBuffer buffer) throws UnknownVersionException {
        byte version = buffer.get();
        if (version > METRIC_VERSION) {
            throw new UnknownVersionException("Cannot deserialize the topic metrics for version " + version + ". "
                    + "Current version is " + METRIC_VERSION);
        }
        RawMetricType rawMetricType = RawMetricType.forId(buffer.get());
        long time = buffer.getLong();
        int brokerId = buffer.getInt();
        double value = buffer.getDouble();
        return new BrokerMetric(rawMetricType, time, brokerId, value);
    }

    @Override
    public MetricClassId metricClassId() {
        return MetricClassId.BROKER_METRIC;
    }

    /**
     * The buffer capacity is calculated as follows:
     * <ul>
     *   <li>(headerPos + {@link Byte#BYTES}) - version</li>
     *   <li>{@link Byte#BYTES} - raw metric type</li>
     *   <li>{@link Long#BYTES} - time</li>
     *   <li>{@link Integer#BYTES} - broker id</li>
     *   <li>{@link Double#BYTES} - value</li>
     * </ul>
     *
     * @param headerPos Header position
     * @return Byte buffer of the partition metric.
     */
    @Override
    public ByteBuffer toBuffer(int headerPos) {
        ByteBuffer buffer = ByteBuffer.allocate(headerPos + Byte.BYTES
                + Byte.BYTES
                + Long.BYTES
                + Integer.BYTES
                + Double.BYTES);
        buffer.position(headerPos);
        buffer.put(METRIC_VERSION);
        buffer.put(rawMetricType().id());
        buffer.putLong(time());
        buffer.putInt(brokerId());
        buffer.putDouble(value());
        return buffer;
    }

    @Override
    public String toString() {
        return String.format("[%s,%s,time=%d,brokerId=%d,value=%.3f]",
                MetricClassId.BROKER_METRIC, rawMetricType(), time(), brokerId(), value());
    }
}

