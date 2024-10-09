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

package org.apache.kafka.server.metrics.s3stream;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaS3MetricsLoggerReporter implements MetricsRegistryListener, MetricsReporter {
    private static final Logger LOGGER = S3StreamMetricsLogger.logger("[KafkaS3MetricsLoggerReporter] ");
    private static final String S3_METRICS_LOGGER_INTERVAL_MS = "s3.metrics.logger.interval.ms";
    private final Map<String, Map<MetricName, Metric>> interestedMetrics = new ConcurrentHashMap<>();
    private final MetricsRegistry metricsRegistry = KafkaYammerMetrics.defaultRegistry();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("s3-metrics-logger-%d", true));
    private final S3MetricsLoggerProcessor processor = new S3MetricsLoggerProcessor();
    private final S3MetricsLoggerProcessor.Context context = new S3MetricsLoggerProcessor.Context();
    private long loggerIntervalMs = 60000;

    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        if (isS3Metrics(name)) {
            interestedMetrics.computeIfAbsent(name.getType(), k -> new ConcurrentHashMap<>());
            interestedMetrics.get(name.getType()).put(name, metric);
            LOGGER.info("S3Metrics {} registered", name);
        }
    }

    private boolean isS3Metrics(MetricName name) {
        return "org.apache.kafka.server.metrics.s3stream".equals(name.getGroup());
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        if (interestedMetrics.containsKey(name.getType())) {
            interestedMetrics.get(name.getType()).remove(name);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            if (configs.containsKey(S3_METRICS_LOGGER_INTERVAL_MS)) {
                loggerIntervalMs = Long.parseLong(String.valueOf(configs.get(S3_METRICS_LOGGER_INTERVAL_MS)));
            }
        } catch (Exception ignored) {

        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        executorService.scheduleAtFixedRate(this::logMetrics, loggerIntervalMs, loggerIntervalMs, TimeUnit.MILLISECONDS);
        metricsRegistry.addListener(this);
        LOGGER.info("KafkaS3MetricsLoggerReporter init successful, log interval: {}ms", loggerIntervalMs);
    }

    private void logMetrics() {
        for (Map.Entry<String, Map<MetricName, Metric>> entry : interestedMetrics.entrySet()) {
            for (Map.Entry<MetricName, Metric> metricEntry : entry.getValue().entrySet()) {
                try {
                    metricEntry.getValue().processWith(processor, metricEntry.getKey(), context);
                } catch (Exception e) {
                    LOGGER.error("Error when processing metric: {}", metricEntry.getKey(), e);
                }
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {

    }

    @Override
    public void metricRemoval(KafkaMetric metric) {

    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
