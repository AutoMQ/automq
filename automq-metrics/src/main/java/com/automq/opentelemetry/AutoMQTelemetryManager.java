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

import com.automq.opentelemetry.exporter.MetricsExporter;
import com.automq.opentelemetry.exporter.MetricsExporterURI;
import com.automq.opentelemetry.yammer.YammerMetricsReporter;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.jmx.engine.JmxMetricInsight;
import io.opentelemetry.instrumentation.jmx.engine.MetricConfiguration;
import io.opentelemetry.instrumentation.jmx.yaml.RuleParser;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Cpu;
import io.opentelemetry.instrumentation.runtimemetrics.java8.GarbageCollector;
import io.opentelemetry.instrumentation.runtimemetrics.java8.MemoryPools;
import io.opentelemetry.instrumentation.runtimemetrics.java8.Threads;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.resources.Resource;

/**
 * The main manager for AutoMQ telemetry.
 * This class is responsible for initializing, configuring, and managing the lifecycle of all
 * telemetry components, including the OpenTelemetry SDK, metric exporters, and various metric sources.
 */
public class AutoMQTelemetryManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoMQTelemetryManager.class);
    
    // Singleton instance support
    private static volatile AutoMQTelemetryManager instance;
    private static final Object LOCK = new Object();

    private final TelemetryConfig config;
    private final List<MetricReader> metricReaders = new ArrayList<>();
    private final List<AutoCloseable> autoCloseableList;
    private OpenTelemetrySdk openTelemetrySdk;
    private YammerMetricsReporter yammerReporter;

    /**
     * Constructs a new Telemetry Manager with the given configuration.
     *
     * @param props Configuration properties.
     */
    public AutoMQTelemetryManager(Properties props) {
        this.config = new TelemetryConfig(props);
        this.autoCloseableList = new ArrayList<>();
        // Redirect JUL from OpenTelemetry SDK to SLF4J for unified logging
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    /**
     * Gets the singleton instance of AutoMQTelemetryManager.
     * Returns null if no instance has been initialized.
     *
     * @return the singleton instance, or null if not initialized
     */
    public static AutoMQTelemetryManager getInstance() {
        return instance;
    }

    /**
     * Initializes the singleton instance with the given configuration.
     * This method should be called before any other components try to access the instance.
     *
     * @param props Configuration properties
     * @return the initialized singleton instance
     */
    public static AutoMQTelemetryManager initializeInstance(Properties props) {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    AutoMQTelemetryManager newInstance = new AutoMQTelemetryManager(props);
                    newInstance.init();
                    instance = newInstance;
                    LOGGER.info("AutoMQTelemetryManager singleton instance initialized");
                }
            }
        }
        return instance;
    }

    /**
     * Shuts down the singleton instance and releases all resources.
     */
    public static void shutdownInstance() {
        if (instance != null) {
            synchronized (LOCK) {
                if (instance != null) {
                    instance.shutdown();
                    instance = null;
                    LOGGER.info("AutoMQTelemetryManager singleton instance shutdown");
                }
            }
        }
    }

    /**
     * Initializes the telemetry system. This method sets up the OpenTelemetry SDK,
     * configures exporters, and registers JVM and JMX metrics.
     */
    public void init() {
        SdkMeterProvider meterProvider = buildMeterProvider();

        this.openTelemetrySdk = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setPropagators(ContextPropagators.create(TextMapPropagator.composite(
                        W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
                .buildAndRegisterGlobal();

        // Register JVM and JMX metrics
        registerJvmMetrics(openTelemetrySdk);
        registerJmxMetrics(openTelemetrySdk);

        LOGGER.info("AutoMQ Telemetry Manager initialized successfully.");
    }

    private SdkMeterProvider buildMeterProvider() {
        AttributesBuilder attrsBuilder = Attributes.builder()
                .put(TelemetryConstants.SERVICE_NAME_KEY, config.getServiceName())
                .put(TelemetryConstants.SERVICE_INSTANCE_ID_KEY, config.getInstanceId())
                .put(TelemetryConstants.HOST_NAME_KEY, config.getHostName())
                // Add attributes for Prometheus compatibility
                .put(TelemetryConstants.PROMETHEUS_JOB_KEY, config.getServiceName())
                .put(TelemetryConstants.PROMETHEUS_INSTANCE_KEY, config.getInstanceId());

        for (Pair<String, String> label : config.getBaseLabels()) {
            attrsBuilder.put(label.getKey(), label.getValue());
        }

        Resource resource = Resource.getDefault().merge(Resource.create(attrsBuilder.build()));
        SdkMeterProviderBuilder meterProviderBuilder = SdkMeterProvider.builder().setResource(resource);

        // Configure exporters from URI
        MetricsExporterURI exporterURI = buildMetricsExporterURI(config);
        for (MetricsExporter exporter : exporterURI.getMetricsExporters()) {
            MetricReader reader = exporter.asMetricReader();
            metricReaders.add(reader);
            SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(meterProviderBuilder, reader,
                    instrumentType -> config.getMetricCardinalityLimit());
        }

        return meterProviderBuilder.build();
    }

    protected MetricsExporterURI buildMetricsExporterURI(TelemetryConfig config) {
        return MetricsExporterURI.parse(config);
    }

    private void registerJvmMetrics(OpenTelemetry openTelemetry) {
        autoCloseableList.addAll(MemoryPools.registerObservers(openTelemetry));
        autoCloseableList.addAll(Cpu.registerObservers(openTelemetry));
        autoCloseableList.addAll(GarbageCollector.registerObservers(openTelemetry));
        autoCloseableList.addAll(Threads.registerObservers(openTelemetry));
        LOGGER.info("JVM metrics registered.");
    }

    @SuppressWarnings({"NP_LOAD_OF_KNOWN_NULL_VALUE", "RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE"})
    private void registerJmxMetrics(OpenTelemetry openTelemetry) {
        List<String> jmxConfigPaths = config.getJmxConfigPaths();
        if (jmxConfigPaths.isEmpty()) {
            LOGGER.info("No JMX metric config paths provided, skipping JMX metrics registration.");
            return;
        }

        JmxMetricInsight jmxMetricInsight = JmxMetricInsight.createService(openTelemetry, config.getExporterIntervalMs());
        MetricConfiguration metricConfig = new MetricConfiguration();

        for (String path : jmxConfigPaths) {
            try (InputStream ins = this.getClass().getResourceAsStream(path)) {
                if (ins == null) {
                    LOGGER.error("JMX config file not found in classpath: {}", path);
                    continue;
                }
                RuleParser parser = RuleParser.get();
                parser.addMetricDefsTo(metricConfig, ins, path);
            } catch (Exception e) {
                LOGGER.error("Failed to parse JMX config file: {}", path, e);
            }
        }

        jmxMetricInsight.start(metricConfig);
        // JmxMetricInsight doesn't implement Closeable, but we can create a wrapper

        LOGGER.info("JMX metrics registered with config paths: {}", jmxConfigPaths);
    }

    /**
     * Starts reporting metrics from a given Yammer MetricsRegistry.
     *
     * @param registry The Yammer registry to bridge metrics from.
     */
    public void startYammerMetricsReporter(MetricsRegistry registry) {
        if (this.openTelemetrySdk == null) {
            throw new IllegalStateException("TelemetryManager is not initialized. Call init() first.");
        }
        if (registry == null) {
            LOGGER.warn("Yammer MetricsRegistry is null, skipping reporter start.");
            return;
        }
        this.yammerReporter = new YammerMetricsReporter(registry);
        this.yammerReporter.start(getMeter());
    }

    public void shutdown() {
        autoCloseableList.forEach(autoCloseable -> {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close auto closeable", e);
            }
        });
        metricReaders.forEach(metricReader -> {
            metricReader.forceFlush();
            try {
                metricReader.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close metric reader", e);
            }
        });
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
        }
    }

    /**
     *  get YammerMetricsReporter instance.
     * @return The YammerMetricsReporter instance.
     */
    public YammerMetricsReporter getYammerReporter() {
        return this.yammerReporter;
    }

    /**
     * Gets the default meter from the initialized OpenTelemetry SDK.
     *
     * @return The meter instance.
     */
    public Meter getMeter() {
        if (this.openTelemetrySdk == null) {
            throw new IllegalStateException("TelemetryManager is not initialized. Call init() first.");
        }
        return this.openTelemetrySdk.getMeter(TelemetryConstants.TELEMETRY_SCOPE_NAME);
    }
}
