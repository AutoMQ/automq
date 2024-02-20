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

package kafka.log.stream.s3.telemetry.metrics;

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

    static {
        BASE_ATTRIBUTES_LISTENERS.add(BROKER_ATTRIBUTES);
    }

    private static ObservableLongGauge autoBalancerMetricsTimeDelay = new NoopObservableLongGauge();
    private static Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier = Collections::emptyMap;
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
    }

    public static void setAutoBalancerMetricsTimeMapSupplier(Supplier<Map<Integer, Long>> autoBalancerMetricsTimeMapSupplier) {
        S3StreamKafkaMetricsManager.autoBalancerMetricsTimeMapSupplier = autoBalancerMetricsTimeMapSupplier;
    }
}
