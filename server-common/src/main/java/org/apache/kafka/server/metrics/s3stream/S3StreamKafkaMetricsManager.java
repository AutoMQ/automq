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

package org.apache.kafka.server.metrics.s3stream;

import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.NoopObservableLongGauge;
import com.automq.stream.s3.metrics.wrapper.ConfigListener;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class S3StreamKafkaMetricsManager {

    private static final List<ConfigListener> BASE_ATTRIBUTES_LISTENERS = new ArrayList<>();
    private static final MultiAttributes<String> BROKER_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_NODE_ID);
    private static final MultiAttributes<String> S3_OBJECT_ATTRIBUTES = new MultiAttributes<>(Attributes.empty(),
            S3StreamKafkaMetricsConstants.LABEL_OBJECT_STATE);

    static {
        BASE_ATTRIBUTES_LISTENERS.add(BROKER_ATTRIBUTES);
        BASE_ATTRIBUTES_LISTENERS.add(S3_OBJECT_ATTRIBUTES);
    }

    private static ObservableLongGauge autoBalancerMetricsTimeDelay = new NoopObservableLongGauge();
    private static Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier = Collections::emptyMap;
    private static ObservableLongGauge s3ObjectCountMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> s3ObjectCountMapSupplier = Collections::emptyMap;
    private static ObservableLongGauge s3ObjectSizeMetrics = new NoopObservableLongGauge();
    private static Supplier<Long> s3ObjectSizeSupplier = () -> 0L;
    private static ObservableLongGauge streamSetObjectNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Map<String, Integer>> streamSetObjectNumSupplier = Collections::emptyMap;
    private static ObservableLongGauge streamObjectNumMetrics = new NoopObservableLongGauge();
    private static Supplier<Integer> streamObjectNumSupplier = () -> 0;
    private static MetricsConfig metricsConfig = new MetricsConfig(MetricsLevel.INFO, Attributes.empty());

    public static void configure(MetricsConfig metricsConfig) {
        synchronized (BASE_ATTRIBUTES_LISTENERS) {
            S3StreamKafkaMetricsManager.metricsConfig = metricsConfig;
            for (ConfigListener listener : BASE_ATTRIBUTES_LISTENERS) {
                listener.onConfigChange(metricsConfig);
            }
        }
    }

    public static void initMetrics(Meter meter, String prefix) {
        autoBalancerMetricsTimeDelay = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.AUTO_BALANCER_METRICS_TIME_DELAY_METRIC_NAME)
                .setDescription("The time delay of auto balancer metrics per broker")
                .setUnit("ms")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<Integer, Long> metricsTimeDelayMap = autoBalancerMetricsTimeMapSupplier.get();
                        for (Map.Entry<Integer, Long> entry : metricsTimeDelayMap.entrySet()) {
                            long timestamp = entry.getValue();
                            long delay = timestamp == 0 ? -1 : System.currentTimeMillis() - timestamp;
                            result.record(delay, BROKER_ATTRIBUTES.get(String.valueOf(entry.getKey())));
                        }
                    }
                });
        s3ObjectCountMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.S3_OBJECT_COUNT_BY_STATE)
                .setDescription("The total count of s3 objects in different states")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<String, Integer> s3ObjectCountMap = s3ObjectCountMapSupplier.get();
                        for (Map.Entry<String, Integer> entry : s3ObjectCountMap.entrySet()) {
                            result.record(entry.getValue(), S3_OBJECT_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });
        s3ObjectSizeMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.S3_OBJECT_SIZE)
                .setDescription("The total size of s3 objects in bytes")
                .setUnit("bytes")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        result.record(s3ObjectSizeSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
        streamSetObjectNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.STREAM_SET_OBJECT_NUM)
                .setDescription("The total number of stream set objects")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        Map<String, Integer> streamSetObjectNumMap = streamSetObjectNumSupplier.get();
                        for (Map.Entry<String, Integer> entry : streamSetObjectNumMap.entrySet()) {
                            result.record(entry.getValue(), BROKER_ATTRIBUTES.get(entry.getKey()));
                        }
                    }
                });
        streamObjectNumMetrics = meter.gaugeBuilder(prefix + S3StreamKafkaMetricsConstants.STREAM_OBJECT_NUM)
                .setDescription("The total number of stream objects")
                .ofLongs()
                .buildWithCallback(result -> {
                    if (MetricsLevel.INFO.isWithin(metricsConfig.getMetricsLevel())) {
                        result.record(streamObjectNumSupplier.get(), metricsConfig.getBaseAttributes());
                    }
                });
    }

    public static void setAutoBalancerMetricsTimeMapSupplier(Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier) {
        S3StreamKafkaMetricsManager.autoBalancerMetricsTimeMapSupplier = autoBalancerMetricsTimeMapSupplier;
    }

    public static void setS3ObjectCountMapSupplier(Supplier<Map<String, Integer>> s3ObjectCountMapSupplier) {
        S3StreamKafkaMetricsManager.s3ObjectCountMapSupplier = s3ObjectCountMapSupplier;
    }

    public static void setS3ObjectSizeSupplier(Supplier<Long> s3ObjectSizeSupplier) {
        S3StreamKafkaMetricsManager.s3ObjectSizeSupplier = s3ObjectSizeSupplier;
    }

    public static void setStreamSetObjectNumSupplier(Supplier<Map<String, Integer>> streamSetObjectNumSupplier) {
        S3StreamKafkaMetricsManager.streamSetObjectNumSupplier = streamSetObjectNumSupplier;
    }

    public static void setStreamObjectNumSupplier(Supplier<Integer> streamObjectNumSupplier) {
        S3StreamKafkaMetricsManager.streamObjectNumSupplier = streamObjectNumSupplier;
    }
}
