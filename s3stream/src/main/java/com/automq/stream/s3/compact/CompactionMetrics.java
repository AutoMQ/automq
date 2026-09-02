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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;

import io.opentelemetry.api.common.Attributes;

/**
 * Metrics owned by the compaction subsystem.
 */
public final class CompactionMetrics {
    public static final Metrics.LongGaugeBundle.LongGauge COMPACTION_DELAY_TIME = Metrics.instance()
        .longGauge("kafka_stream_compaction_delay_time", "Compaction delay time", "milliseconds")
        .register(MetricsLevel.INFO, Attributes.empty());
    public static final Metrics.LongCounterBundle.LongCounter COMPACTION_READ_SIZE = Metrics.instance()
        .longCounter("kafka_stream_compaction_read_size", "Compaction read size", "bytes")
        .register(MetricsLevel.INFO, Attributes.empty());
    public static final Metrics.LongCounterBundle.LongCounter COMPACTION_WRITE_SIZE = Metrics.instance()
        .longCounter("kafka_stream_compaction_write_size", "Compaction write size", "bytes")
        .register(MetricsLevel.INFO, Attributes.empty());

    static {
        COMPACTION_READ_SIZE.add(0);
        COMPACTION_WRITE_SIZE.add(0);
    }

    private CompactionMetrics() {
    }
}
