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

package com.automq.opentelemetry.yammer;

import com.yammer.metrics.core.MetricName;

import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;

public class OTelMetricUtils {
    public static final String REQUEST_TAG_KEY = "request";
    public static final String TYPE_TAG_KEY = "type";
    // metric groups
    private static final String KAFKA_NETWORK_GROUP = "kafka.network";
    private static final String KAFKA_LOG_GROUP = "kafka.log";
    private static final String KAFKA_CONTROLLER_GROUP = "kafka.controller";

    // metric types
    private static final String REQUEST_METRICS_TYPE = "RequestMetrics";
    private static final String LOG_FLUSH_STATS_TYPE = "LogFlushStats";
    private static final String CONTROLLER_EVENT_MANAGER_TYPE = "ControllerEventManager";

    // metric names
    private static final String REQUEST_BYTES = "RequestBytes";
    private static final String TOTAL_TIME_MS = "TotalTimeMs";
    private static final String REQUEST_QUEUE_TIME = "RequestQueueTimeMs";
    private static final String RESPONSE_QUEUE_TIME = "ResponseQueueTimeMs";
    private static final String LOG_FLUSH_RATE_AND_TIME_MS = "LogFlushRateAndTimeMs";
    private static final String EVENT_QUEUE_TIME_MS = "EventQueueTimeMs";
    private static final String EVENT_QUEUE_PROCESSING_TIME_MS = "EventQueueProcessingTimeMs";

    public static boolean isInterestedMetric(MetricName metricName) {
        if (metricName == null) {
            return false;
        }
        switch (metricName.getGroup()) {
            case KAFKA_NETWORK_GROUP:
                return isInterestedNetworkMetric(metricName);
            case KAFKA_LOG_GROUP:
                return isInterestedLogMetric(metricName);
            case KAFKA_CONTROLLER_GROUP:
                return isInterestedControllerMetric(metricName);
            default:
                return false;
        }
    }

    public static boolean isInterestedNetworkMetric(MetricName metricName) {
        if (metricName == null) {
            return false;
        }
        if (REQUEST_METRICS_TYPE.equals(metricName.getType())) {
            switch (metricName.getName()) {
                case REQUEST_BYTES:
                case TOTAL_TIME_MS:
                case REQUEST_QUEUE_TIME:
                case RESPONSE_QUEUE_TIME:
                    return true;
            }
        }
        return false;
    }

    public static boolean isInterestedLogMetric(MetricName metricName) {
        if (metricName == null) {
            return false;
        }
        if (LOG_FLUSH_STATS_TYPE.equals(metricName.getType())) {
            switch (metricName.getName()) {
                case LOG_FLUSH_RATE_AND_TIME_MS:
                    return true;
            }
        }
        return false;
    }

    public static boolean isInterestedControllerMetric(MetricName metricName) {
        if (metricName == null) {
            return false;
        }
        if (CONTROLLER_EVENT_MANAGER_TYPE.equals(metricName.getType())) {
            switch (metricName.getName()) {
                case EVENT_QUEUE_TIME_MS:
                case EVENT_QUEUE_PROCESSING_TIME_MS:
                    return true;
            }
        }
        return false;
    }

    public static String toOTelMetricNamePrefix(MetricName metricName) {
        if (metricName == null) {
            throw new IllegalArgumentException("Metric name must not be null");
        }
        switch (metricName.getName()) {
            case REQUEST_BYTES:
                return "kafka.request.size";
            case TOTAL_TIME_MS:
                return "kafka.request.time";
            case REQUEST_QUEUE_TIME:
                return  "kafka.request.queue.time";
            case RESPONSE_QUEUE_TIME:
                return  "kafka.response.queue.time";
            case LOG_FLUSH_RATE_AND_TIME_MS:
                return  "kafka.logs.flush.time";
            case EVENT_QUEUE_TIME_MS:
                return  "kafka.event.queue.time";
            case EVENT_QUEUE_PROCESSING_TIME_MS:
                return  "kafka.event.queue.processing.time";
            default:
                throw new IllegalArgumentException("Unsupported metric name: " + metricName.getName());
        }
    }

    public static String toMeanMetricName(MetricName metricName) {
        return toOTelMetricNamePrefix(metricName) + ".mean";
    }

    public static DoubleGaugeBuilder toMeanGaugeBuilder(Meter meter, MetricName metricName) {
        if (meter == null || metricName == null) {
            throw new IllegalArgumentException("Meter and metric name must not be null");
        }
        switch (metricName.getName()) {
            case REQUEST_BYTES:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("The mean request size in bytes")
                    .setUnit("bytes");
            case TOTAL_TIME_MS:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("The mean time the broker has taken to service requests")
                    .setUnit("milliseconds");
            case REQUEST_QUEUE_TIME:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("The mean time the broker has taken to dequeue requests")
                    .setUnit("milliseconds");
            case RESPONSE_QUEUE_TIME:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("The mean time the broker has taken to dequeue responses")
                    .setUnit("milliseconds");
            case LOG_FLUSH_RATE_AND_TIME_MS:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("Log flush time - mean")
                    .setUnit("milliseconds");
            case EVENT_QUEUE_TIME_MS:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("The mean time the event waits in the queue")
                    .setUnit("milliseconds");
            case EVENT_QUEUE_PROCESSING_TIME_MS:
                return meter.gaugeBuilder(toMeanMetricName(metricName))
                    .setDescription("The mean time used to process the event in the event queue")
                    .setUnit("milliseconds");
            default:
                throw new IllegalArgumentException("Unsupported metric name: " + metricName.getName());
        }
    }

}
