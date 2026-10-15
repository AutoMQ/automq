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

package com.automq.opentelemetry;

import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.opentelemetry.exporter.MetricsExporter;
import com.automq.opentelemetry.exporter.MetricsExporterURI;
import com.automq.opentelemetry.jmx.AutomaticJmxMetricsCollector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.List;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests one worker-owned JMX source with idempotent startup and shutdown. */
@Tag("S3Unit")
public class AutoMQTelemetryManagerLifecycleTest {
    @AfterEach
    void resetGlobalTelemetry() {
        GlobalOpenTelemetry.resetForTest();
    }

    /** Given repeated source startup, one collector shares the SDK and closes once with its owner. */
    @Test
    void testAutomaticJmxSourceLifecycle() {
        AutoMQTelemetryManager manager = manager();
        try (MockedConstruction<AutomaticJmxMetricsCollector> construction = mockConstruction(AutomaticJmxMetricsCollector.class)) {
            manager.setMetricCardinalityLimit(37);
            manager.init();
            manager.init();
            assertThrows(IllegalStateException.class, () -> manager.setMetricCardinalityLimit(42));
            manager.startAutomaticJmxMetrics(Attributes.empty());
            manager.startAutomaticJmxMetrics(Attributes.empty());
            assertEquals(1, construction.constructed().size());
            AutomaticJmxMetricsCollector collector = construction.constructed().get(0);
            verify(collector).start();
            manager.shutdown();
            manager.shutdown();
            verify(collector).close();
            assertThrows(IllegalStateException.class, manager::getMeter);
            assertThrows(IllegalStateException.class, () -> manager.startAutomaticJmxMetrics(Attributes.empty()));
        } finally {
            manager.shutdown();
        }
    }

    /** Given a source startup failure, owner shutdown still releases the partially started collector. */
    @Test
    void testPartialJmxSourceStartupIsClosed() {
        AutoMQTelemetryManager manager = manager();
        try (MockedConstruction<AutomaticJmxMetricsCollector> construction = mockConstruction(AutomaticJmxMetricsCollector.class,
            (collector, context) -> doThrow(new IllegalStateException("JMX start failed")).when(collector).start())) {
            manager.init();
            assertThrows(IllegalStateException.class, () -> manager.startAutomaticJmxMetrics(Attributes.empty()));
            manager.shutdown();
            verify(construction.constructed().get(0)).close();
        } finally {
            manager.shutdown();
        }
    }

    /** Given one reader flush failure, later readers and the SDK still shut down. */
    @Test
    void testReaderFlushFailureDoesNotInterruptShutdown() throws Exception {
        InMemoryMetricReader failingReader = spy(InMemoryMetricReader.create());
        InMemoryMetricReader remainingReader = spy(InMemoryMetricReader.create());
        doThrow(new IllegalStateException("Flush failed")).when(failingReader).forceFlush();
        AutoMQTelemetryManager manager = manager(failingReader, remainingReader);
        try {
            manager.init();
            manager.shutdown();
            verify(failingReader).close();
            verify(remainingReader).forceFlush();
            verify(remainingReader).close();
            assertThrows(IllegalStateException.class, manager::getMeter);
        } finally {
            manager.shutdown();
        }
    }

    /** Given a closed owner without a previous source, JMX startup must not create resources. */
    @Test
    void testCannotStartAutomaticJmxAfterShutdown() {
        AutoMQTelemetryManager manager = manager();
        manager.shutdown();
        try (MockedConstruction<AutomaticJmxMetricsCollector> construction = mockConstruction(AutomaticJmxMetricsCollector.class)) {
            assertThrows(IllegalStateException.class, () -> manager.startAutomaticJmxMetrics(Attributes.empty()));
            assertEquals(0, construction.constructed().size());
        }
    }

    private static AutoMQTelemetryManager manager(MetricReader... readers) {
        MetricsExportConfig config = mock(MetricsExportConfig.class);
        when(config.baseLabels()).thenReturn(List.of());
        when(config.intervalMs()).thenReturn(1000);
        MetricsExporterURI exporters = mock(MetricsExporterURI.class);
        List<MetricsExporter> metricExporters = new ArrayList<>();
        for (MetricReader reader : readers) {
            metricExporters.add(() -> reader);
        }
        when(exporters.getMetricsExporters()).thenReturn(metricExporters);
        return new AutoMQTelemetryManager("", "worker-test", "0", config) {
            @Override
            protected MetricsExporterURI buildMetricsExporterURI(String exporterUri, MetricsExportConfig metricsExportConfig) {
                return exporters;
            }
        };
    }
}
