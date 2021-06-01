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
package org.apache.kafka.streams.processor.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_AVG_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_MAX_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RECORD_E2E_LATENCY_MIN_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMinAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;

public class ProcessorNodeMetrics {
    private ProcessorNodeMetrics() {}

    private static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    private static final String RATE_DESCRIPTION_SUFFIX = " per second";

    private static final String SUPPRESSION_EMIT = "suppression-emit";
    private static final String SUPPRESSION_EMIT_DESCRIPTION = "emitted records from the suppression buffer";
    private static final String SUPPRESSION_EMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SUPPRESSION_EMIT_DESCRIPTION;
    private static final String SUPPRESSION_EMIT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + SUPPRESSION_EMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String IDEMPOTENT_UPDATE_SKIP = "idempotent-update-skip";
    private static final String IDEMPOTENT_UPDATE_SKIP_DESCRIPTION = "skipped idempotent updates";
    private static final String IDEMPOTENT_UPDATE_SKIP_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + IDEMPOTENT_UPDATE_SKIP_DESCRIPTION;
    private static final String IDEMPOTENT_UPDATE_SKIP_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + IDEMPOTENT_UPDATE_SKIP_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String PROCESS = "process";
    private static final String PROCESS_DESCRIPTION = "calls to process";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String FORWARD = "forward";
    private static final String FORWARD_DESCRIPTION = "calls to forward";
    private static final String FORWARD_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + FORWARD_DESCRIPTION;
    private static final String FORWARD_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + FORWARD_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String LATE_RECORD_DROP = "late-record-drop";
    private static final String LATE_RECORD_DROP_DESCRIPTION = "dropped late records";
    private static final String LATE_RECORD_DROP_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + LATE_RECORD_DROP_DESCRIPTION;
    private static final String LATE_RECORD_DROP_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + LATE_RECORD_DROP_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    public static Sensor suppressionEmitSensor(final String threadId,
                                               final String taskId,
                                               final String processorNodeId,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            SUPPRESSION_EMIT,
            SUPPRESSION_EMIT_RATE_DESCRIPTION,
            SUPPRESSION_EMIT_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor skippedIdempotentUpdatesSensor(final String threadId,
            final String taskId,
            final String processorNodeId,
            final StreamsMetricsImpl streamsMetrics) {
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            IDEMPOTENT_UPDATE_SKIP,
            IDEMPOTENT_UPDATE_SKIP_RATE_DESCRIPTION,
            IDEMPOTENT_UPDATE_SKIP_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
    }

    public static Sensor processAtSourceSensor(final String threadId,
                                               final String taskId,
                                               final String processorNodeId,
                                               final StreamsMetricsImpl streamsMetrics) {
        final Sensor parentSensor = streamsMetrics.taskLevelSensor(threadId, taskId, PROCESS, RecordingLevel.DEBUG);
        addInvocationRateAndCountToSensor(
            parentSensor,
            TASK_LEVEL_GROUP,
            streamsMetrics.taskLevelTagMap(threadId, taskId),
            PROCESS,
            PROCESS_RATE_DESCRIPTION,
            PROCESS_TOTAL_DESCRIPTION
        );
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            PROCESS,
            PROCESS_RATE_DESCRIPTION,
            PROCESS_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensor
        );
    }

    public static Sensor forwardSensor(final String threadId,
                                       final String taskId,
                                       final String processorNodeId,
                                       final StreamsMetricsImpl streamsMetrics) {
        final Sensor parentSensor = throughputParentSensor(
            threadId,
            taskId,
            FORWARD,
            FORWARD_RATE_DESCRIPTION,
            FORWARD_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            FORWARD,
            FORWARD_RATE_DESCRIPTION,
            FORWARD_TOTAL_DESCRIPTION,
            RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensor
        );
    }

    public static Sensor lateRecordDropSensor(final String threadId,
                                              final String taskId,
                                              final String processorNodeId,
                                              final StreamsMetricsImpl streamsMetrics) {
        return throughputSensor(
            threadId,
            taskId,
            processorNodeId,
            LATE_RECORD_DROP,
            LATE_RECORD_DROP_RATE_DESCRIPTION,
            LATE_RECORD_DROP_TOTAL_DESCRIPTION,
            RecordingLevel.INFO,
            streamsMetrics);
    }

    public static Sensor e2ELatencySensor(final String threadId,
                                          final String taskId,
                                          final String processorNodeId,
                                          final StreamsMetricsImpl streamsMetrics) {
        final String sensorName = processorNodeId + "-" + RECORD_E2E_LATENCY;
        final Sensor sensor = streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, sensorName, RecordingLevel.INFO);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addAvgAndMinAndMaxToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            RECORD_E2E_LATENCY,
            RECORD_E2E_LATENCY_AVG_DESCRIPTION,
            RECORD_E2E_LATENCY_MIN_DESCRIPTION,
            RECORD_E2E_LATENCY_MAX_DESCRIPTION
        );
        return sensor;
    }

    private static Sensor throughputParentSensor(final String threadId,
                                                 final String taskId,
                                                 final String metricNamePrefix,
                                                 final String descriptionOfRate,
                                                 final String descriptionOfCount,
                                                 final RecordingLevel recordingLevel,
                                                 final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, metricNamePrefix, recordingLevel);
        final Map<String, String> parentTagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, ROLLUP_VALUE);
        addInvocationRateAndCountToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            parentTagMap,
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }

    private static Sensor throughputSensor(final String threadId,
                                           final String taskId,
                                           final String processorNodeId,
                                           final String metricNamePrefix,
                                           final String descriptionOfRate,
                                           final String descriptionOfCount,
                                           final RecordingLevel recordingLevel,
                                           final StreamsMetricsImpl streamsMetrics,
                                           final Sensor... parentSensors) {
        final Sensor sensor =
            streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, metricNamePrefix, recordingLevel, parentSensors);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addInvocationRateAndCountToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount
        );
        return sensor;
    }
}
