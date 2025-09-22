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

package kafka.automq.table.metric;

import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

public final class TableTopicMetricsManager {
    private static final Cache<String, Attributes> TOPIC_ATTRIBUTE_CACHE = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(1)).build();
    private static final Metrics.LongGaugeBundle DELAY_GAUGES = Metrics.instance()
        .longGauge("kafka_tabletopic_delay", "Table topic commit delay", "ms");
    private static final Metrics.DoubleGaugeBundle FIELDS_PER_SECOND_GAUGES = Metrics.instance()
        .doubleGauge("kafka_tabletopic_fps", "Table topic fields per second", "fields/s");

    private TableTopicMetricsManager() {
    }

    public static void initMetrics(Meter meter) {
        // Metrics instruments are registered via Metrics.instance(); no additional setup required.
    }

    public static Metrics.LongGaugeBundle.LongGauge registerDelay(String topic) {
        return DELAY_GAUGES.register(MetricsLevel.INFO, getTopicAttribute(topic));
    }

    public static Metrics.DoubleGaugeBundle.DoubleGauge registerFieldsPerSecond(String topic) {
        return FIELDS_PER_SECOND_GAUGES.register(MetricsLevel.INFO, getTopicAttribute(topic));
    }

    private static Attributes getTopicAttribute(String topic) {
        try {
            return TOPIC_ATTRIBUTE_CACHE.get(topic, () -> Attributes.of(AttributeKey.stringKey("topic"), topic));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
