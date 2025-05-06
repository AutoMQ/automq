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

package kafka.automq.table;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;

public class TableTopicMetricsManager {
    private static final Cache<String, Attributes> TOPIC_ATTRIBUTE_CACHE = CacheBuilder.newBuilder()
        .expireAfterAccess(Duration.ofMinutes(1)).build();
    private static Supplier<Map<String, Long>> delaySupplier = Collections::emptyMap;
    private static Supplier<Map<String, Double>> fieldsPerSecondSupplier = Collections::emptyMap;
    private static ObservableLongGauge delay;
    private static ObservableDoubleGauge fieldsPerSecond;

    public static void initMetrics(Meter meter) {
        String prefix = "kafka_tabletopic_";
        delay = meter.gaugeBuilder(prefix + "delay").ofLongs().setUnit("ms")
            .buildWithCallback(recorder ->
                delaySupplier.get().forEach((topic, delay) -> {
                    if (delay >= 0) {
                        recorder.record(delay, getTopicAttribute(topic));
                    }
                }));
        fieldsPerSecond = meter.gaugeBuilder(prefix + "fps")
            .buildWithCallback(recorder ->
                fieldsPerSecondSupplier.get().forEach((topic, fps) -> recorder.record(fps, getTopicAttribute(topic))));
    }

    public static void setDelaySupplier(Supplier<Map<String, Long>> supplier) {
        delaySupplier = supplier;
    }

    public static void setFieldsPerSecondSupplier(Supplier<Map<String, Double>> supplier) {
        fieldsPerSecondSupplier = supplier;
    }

    private static Attributes getTopicAttribute(String topic) {
        try {
            return TOPIC_ATTRIBUTE_CACHE.get(topic, () -> Attributes.of(AttributeKey.stringKey("topic"), topic));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
