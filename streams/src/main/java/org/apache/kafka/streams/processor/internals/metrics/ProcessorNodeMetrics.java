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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_DESCRIPTION;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMaxToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCountToSensor;

public class ProcessorNodeMetrics {
    private ProcessorNodeMetrics() {}

    private static final String AVG_DESCRIPTION_PREFIX = "The average ";
    private static final String MAX_DESCRIPTION_PREFIX = "The maximum ";
    private static final String RATE_DESCRIPTION_PREFIX = "The average number of ";
    private static final String RATE_DESCRIPTION_SUFFIX = " per second";
    private static final String LATENCY_DESCRIPTION = "latency of ";
    private static final String AVG_LATENCY_DESCRIPTION_PREFIX = AVG_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
    private static final String MAX_LATENCY_DESCRIPTION_PREFIX = MAX_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;

    private static final String SUPPRESSION_EMIT = "suppression-emit";
    private static final String SUPPRESSION_EMIT_DESCRIPTION = "emitted records from the suppression buffer";
    private static final String SUPPRESSION_EMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SUPPRESSION_EMIT_DESCRIPTION;
    private static final String SUPPRESSION_EMIT_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + SUPPRESSION_EMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

    private static final String PROCESS = "process";
    private static final String PROCESS_DESCRIPTION = "calls to process";
    private static final String PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static final String PROCESS_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PROCESS_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION;
    private static final String PROCESS_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION;

    private static final String PUNCTUATE = "punctuate";
    private static final String PUNCTUATE_DESCRIPTION = "calls to punctuate";
    private static final String PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + PUNCTUATE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String PUNCTUATE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUNCTUATE_DESCRIPTION;
    private static final String PUNCTUATE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUNCTUATE_DESCRIPTION;

    private static final String CREATE = "create";
    private static final String CREATE_DESCRIPTION1 = "processor nodes created";
    private static final String CREATE_DESCRIPTION2 = "creations of processor nodes";
    private static final String CREATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CREATE_DESCRIPTION1;
    private static final String CREATE_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + CREATE_DESCRIPTION1 + RATE_DESCRIPTION_SUFFIX;
    private static final String CREATE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + CREATE_DESCRIPTION2;
    private static final String CREATE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + CREATE_DESCRIPTION2;

    private static final String DESTROY = "destroy";
    private static final String DESTROY_DESCRIPTION = "destructions of processor nodes";
    private static final String DESTROY_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + DESTROY_DESCRIPTION;
    private static final String DESTROY_RATE_DESCRIPTION =
        RATE_DESCRIPTION_PREFIX + DESTROY_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String DESTROY_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + DESTROY_DESCRIPTION;
    private static final String DESTROY_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + DESTROY_DESCRIPTION;

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

    public static Sensor processSensor(final String threadId,
                                       final String taskId,
                                       final String processorNodeId,
                                       final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            return throughputAndLatencySensorWithParent(
                threadId,
                taskId,
                processorNodeId,
                PROCESS,
                PROCESS_RATE_DESCRIPTION,
                PROCESS_TOTAL_DESCRIPTION,
                PROCESS_AVG_LATENCY_DESCRIPTION,
                PROCESS_MAX_LATENCY_DESCRIPTION,
                RecordingLevel.DEBUG,
                streamsMetrics
            );
        }
        return emptySensor(threadId, taskId, processorNodeId, PROCESS, RecordingLevel.DEBUG, streamsMetrics);
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

    public static Sensor punctuateSensor(final String threadId,
                                         final String taskId,
                                         final String processorNodeId,
                                         final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            return throughputAndLatencySensorWithParent(
                threadId,
                taskId,
                processorNodeId,
                PUNCTUATE,
                PUNCTUATE_RATE_DESCRIPTION,
                PUNCTUATE_TOTAL_DESCRIPTION,
                PUNCTUATE_AVG_LATENCY_DESCRIPTION,
                PUNCTUATE_MAX_LATENCY_DESCRIPTION,
                RecordingLevel.DEBUG,
                streamsMetrics
            );
        }
        return emptySensor(threadId, taskId, processorNodeId, PUNCTUATE, RecordingLevel.DEBUG, streamsMetrics);
    }

    public static Sensor createSensor(final String threadId,
                                      final String taskId,
                                      final String processorNodeId,
                                      final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            return throughputAndLatencySensorWithParent(
                threadId,
                taskId,
                processorNodeId,
                CREATE,
                CREATE_RATE_DESCRIPTION,
                CREATE_TOTAL_DESCRIPTION,
                CREATE_AVG_LATENCY_DESCRIPTION,
                CREATE_MAX_LATENCY_DESCRIPTION,
                RecordingLevel.DEBUG,
                streamsMetrics);
        }
        return emptySensor(threadId, taskId, processorNodeId, CREATE, RecordingLevel.DEBUG, streamsMetrics);
    }

    public static Sensor destroySensor(final String threadId,
                                       final String taskId,
                                       final String processorNodeId,
                                       final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            return throughputAndLatencySensorWithParent(
                threadId,
                taskId,
                processorNodeId,
                DESTROY,
                DESTROY_RATE_DESCRIPTION,
                DESTROY_TOTAL_DESCRIPTION,
                DESTROY_AVG_LATENCY_DESCRIPTION,
                DESTROY_MAX_LATENCY_DESCRIPTION,
                RecordingLevel.DEBUG,
                streamsMetrics);
        }
        return emptySensor(threadId, taskId, processorNodeId, DESTROY, RecordingLevel.DEBUG, streamsMetrics);
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

    public static Sensor processorAtSourceSensorOrForwardSensor(final String threadId,
                                                                final String taskId,
                                                                final String processorNodeId,
                                                                final StreamsMetricsImpl streamsMetrics) {
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            return forwardSensor(threadId, taskId, processorNodeId, streamsMetrics);
        }
        return processAtSourceSensor(threadId, taskId, processorNodeId, streamsMetrics);
    }

    private static Sensor throughputAndLatencySensorWithParent(final String threadId,
                                                               final String taskId,
                                                               final String processorNodeId,
                                                               final String metricNamePrefix,
                                                               final String descriptionOfRate,
                                                               final String descriptionOfCount,
                                                               final String descriptionOfAvgLatency,
                                                               final String descriptionOfMaxLatency,
                                                               final RecordingLevel recordingLevel,
                                                               final StreamsMetricsImpl streamsMetrics) {
        final Sensor parentSensor = throughputAndLatencyParentSensor(
            threadId,
            taskId,
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency,
            RecordingLevel.DEBUG,
            streamsMetrics
        );
        return throughputAndLatencySensor(
            threadId,
            taskId,
            processorNodeId,
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfCount,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency,
            RecordingLevel.DEBUG,
            streamsMetrics,
            parentSensor
        );
    }

    private static Sensor throughputAndLatencyParentSensor(final String threadId,
                                                           final String taskId,
                                                           final String metricNamePrefix,
                                                           final String descriptionOfRate,
                                                           final String descriptionOfCount,
                                                           final String descriptionOfAvgLatency,
                                                           final String descriptionOfMaxLatency,
                                                           final RecordingLevel recordingLevel,
                                                           final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor = streamsMetrics.taskLevelSensor(threadId, taskId, metricNamePrefix, recordingLevel);
        final Map<String, String> parentTagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, ROLLUP_VALUE);
        addAvgAndMaxToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            parentTagMap,
            metricNamePrefix + LATENCY_SUFFIX,
            descriptionOfAvgLatency,
            descriptionOfMaxLatency
        );
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

    private static Sensor throughputAndLatencySensor(final String threadId,
                                                     final String taskId,
                                                     final String processorNodeId,
                                                     final String metricNamePrefix,
                                                     final String descriptionOfRate,
                                                     final String descriptionOfCount,
                                                     final String descriptionOfAvg,
                                                     final String descriptionOfMax,
                                                     final RecordingLevel recordingLevel,
                                                     final StreamsMetricsImpl streamsMetrics,
                                                     final Sensor... parentSensors) {
        final Sensor sensor =
            streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, metricNamePrefix, recordingLevel, parentSensors);
        final Map<String, String> tagMap = streamsMetrics.nodeLevelTagMap(threadId, taskId, processorNodeId);
        addAvgAndMaxToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            tagMap,
            metricNamePrefix + LATENCY_SUFFIX,
            descriptionOfAvg,
            descriptionOfMax
        );
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

    private static Sensor emptySensor(final String threadId,
                                      final String taskId,
                                      final String processorNodeId,
                                      final String metricNamePrefix,
                                      final RecordingLevel recordingLevel,
                                      final StreamsMetricsImpl streamsMetrics) {
        return streamsMetrics.nodeLevelSensor(threadId, taskId, processorNodeId, metricNamePrefix, recordingLevel);
    }
}
