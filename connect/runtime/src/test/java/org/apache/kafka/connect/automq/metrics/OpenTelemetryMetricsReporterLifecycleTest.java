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

package org.apache.kafka.connect.automq.metrics;

import com.automq.opentelemetry.AutoMQTelemetryManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Properties;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests worker ownership of telemetry independently of task and Kafka client reporters. */
@Tag("S3Unit")
public class OpenTelemetryMetricsReporterLifecycleTest {
    @AfterEach
    void cleanup() {
        OpenTelemetryMetricsReporter.shutdownTelemetry();
    }

    /** Given repeated initialization and task closure, only worker shutdown closes shared telemetry. */
    @Test
    void testSharedTelemetrySurvivesReporterClosure() {
        try (MockedConstruction<AutoMQTelemetryManager> construction = mockConstruction(AutoMQTelemetryManager.class,
            (manager, context) -> when(manager.getMeter()).thenReturn(mock(Meter.class)))) {
            Properties props = properties();
            OpenTelemetryMetricsReporter.initializeTelemetry(props);
            OpenTelemetryMetricsReporter.initializeTelemetry(props);
            assertEquals(1, construction.constructed().size());
            AutoMQTelemetryManager manager = construction.constructed().get(0);
            InOrder initialization = inOrder(manager);
            initialization.verify(manager).setMetricCardinalityLimit(37);
            initialization.verify(manager).init();
            ArgumentCaptor<Attributes> attributes = ArgumentCaptor.forClass(Attributes.class);
            initialization.verify(manager).startAutomaticJmxMetrics(attributes.capture());
            assertEquals("cluster-1", attributes.getValue().get(AttributeKey.stringKey("connect_cluster_id")));
            String workerId = attributes.getValue().get(AttributeKey.stringKey("worker_id"));
            assertTrue(workerId != null && !workerId.isEmpty());

            OpenTelemetryMetricsReporter taskReporter = new OpenTelemetryMetricsReporter();
            taskReporter.init(List.of());
            taskReporter.close();
            verify(manager, never()).shutdown();
            OpenTelemetryMetricsReporter.shutdownTelemetry();
            OpenTelemetryMetricsReporter.shutdownTelemetry();
            verify(manager).shutdown();
        }
    }

    /** Given JMX startup failure, partially initialized resources are closed and initialization can retry. */
    @Test
    void testInitializationFailureCleansUpSharedManager() {
        try (MockedConstruction<AutoMQTelemetryManager> construction = mockConstruction(AutoMQTelemetryManager.class,
            (manager, context) -> doThrow(new IllegalStateException("JMX unavailable"))
                .when(manager).startAutomaticJmxMetrics(any(Attributes.class)))) {
            assertThrows(IllegalStateException.class, () -> OpenTelemetryMetricsReporter.initializeTelemetry(properties()));
            verify(construction.constructed().get(0)).shutdown();
            assertThrows(IllegalStateException.class, () -> OpenTelemetryMetricsReporter.initializeTelemetry(properties()));
            assertEquals(2, construction.constructed().size());
            verify(construction.constructed().get(1)).shutdown();
        }
    }

    private static Properties properties() {
        Properties props = new Properties();
        props.setProperty(MetricsConfigConstants.EXPORTER_URI_KEY, "prometheus://localhost:9090");
        props.setProperty(MetricsConfigConstants.METRIC_CARDINALITY_LIMIT_KEY, "37");
        props.setProperty(MetricsConfigConstants.TELEMETRY_METRICS_BASE_LABELS_CONFIG, "connect_cluster_id=cluster-1");
        return props;
    }
}
