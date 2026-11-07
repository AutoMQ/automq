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

package kafka.autobalancer.common.types;

import kafka.autobalancer.common.types.metrics.AbnormalLatency;
import kafka.autobalancer.common.types.metrics.AbnormalMetric;

import java.util.Map;

public class RawMetricTypes {
    public static final byte PARTITION_BYTES_IN = (byte) 0;
    public static final byte PARTITION_BYTES_OUT = (byte) 1;
    public static final byte PARTITION_SIZE = (byte) 2;
    public static final byte BROKER_APPEND_LATENCY_AVG_MS = (byte) 3;
    public static final byte BROKER_MAX_PENDING_APPEND_LATENCY_MS = (byte) 4;
    public static final byte BROKER_MAX_PENDING_FETCH_LATENCY_MS = (byte) 5;
    public static final byte BROKER_METRIC_VERSION = (byte) 6;
    public static final Map<Byte, AbnormalMetric> ABNORMAL_METRICS = Map.of(
//            BROKER_APPEND_LATENCY_AVG_MS, new AbnormalLatency(100), // 100ms
            BROKER_MAX_PENDING_APPEND_LATENCY_MS, new AbnormalLatency(10000), // 10s
            BROKER_MAX_PENDING_FETCH_LATENCY_MS, new AbnormalLatency(10000)  // 10s
    );

    public static AbnormalMetric ofAbnormalType(byte metricType) {
        return ABNORMAL_METRICS.get(metricType);
    }
}
