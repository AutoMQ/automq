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

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import com.automq.opentelemetry.exporter.DelegatingMetricReader;
import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.opentelemetry.exporter.MetricsExporter;
import com.automq.opentelemetry.exporter.MetricsExporterURI;
import com.automq.opentelemetry.yammer.YammerMetricsReporter;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class AutoMQTelemetryManager implements Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoMQTelemetryManager.class);

    // Reconfigurable config keys
    public static final String EXPORTER_URI_CONFIG = "s3.telemetry.metrics.exporter.uri";
    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(EXPORTER_URI_CONFIG);

    // Singleton instance support
    private static volatile AutoMQTelemetryManager instance;
    private static final Object LOCK = new Object();

    private volatile String exporterUri;
    private final String serviceName;
    private final String instanceId;
    private final MetricsExportConfig metricsExportConfig;
    private DelegatingMetricReader delegatingReader;
    private final List<AutoCloseable> autoCloseableList;
    private final Object reconfigureLock = new Object();
    private OpenTelemetrySdk openTelemetrySdk;
    private YammerMetricsReporter yammerReporter;

    private int metricCardinalityLimit = TelemetryConstants.DEFAULT_METRIC_CARDINALITY_LIMIT;
    private String jmxConfigPath;

    /**
     * Constructs a new Telemetry Manager with the given configuration.
     *
     * @param exporterUri   The metrics exporter URI.
     * @param serviceName   The service name to be used in telemetry data.
     * @param instanceId    The unique instance ID for this service instance.
     * @param metricsExportConfig The metrics configuration.
     */
    public AutoMQTelemetryManager(String exporterUri, String serviceName, String instanceId, MetricsExportConfig metricsExportConfig) {
        this.exporterUri = exporterUri;
        this.serviceName = serviceName;
        this.instanceId = instanceId;
        this.metricsExportConfig = metricsExportConfig;
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
     * @param exporterUri   The metrics exporter URI.
     * @param serviceName   The service name to be used in telemetry data.
     * @param instanceId    The unique instance ID for this service instance.
     * @param metricsExportConfig The metrics configuration.
     * @return the initialized singleton instance
     */
    public static AutoMQTelemetryManager initializeInstance(String exporterUri, String serviceName, String instanceId, MetricsExportConfig metricsExportConfig) {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    AutoMQTelemetryManager newInstance = new AutoMQTelemetryManager(exporterUri, serviceName, instanceId, metricsExportConfig);
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
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostName = "unknown-host";
        }
        AttributesBuilder attrsBuilder = Attributes.builder()
            .put(TelemetryConstants.SERVICE_NAME_KEY, serviceName)
            .put(TelemetryConstants.SERVICE_INSTANCE_ID_KEY, instanceId)
            .put(TelemetryConstants.HOST_NAME_KEY, hostName)
            // Add attributes for Prometheus compatibility
            .put(TelemetryConstants.PROMETHEUS_JOB_KEY, serviceName)
            .put(TelemetryConstants.PROMETHEUS_INSTANCE_KEY, instanceId);

        for (Pair<String, String> label : metricsExportConfig.baseLabels()) {
            attrsBuilder.put(label.getKey(), label.getValue());
        }

        Resource resource = Resource.getDefault().merge(Resource.create(attrsBuilder.build()));
        SdkMeterProviderBuilder meterProviderBuilder = SdkMeterProvider.builder().setResource(resource);

        // Configure exporters from URI
        MetricsExporterURI exporterURI = buildMetricsExporterURI(exporterUri, metricsExportConfig);
        List<MetricReader> readers = new ArrayList<>();
        for (MetricsExporter exporter : exporterURI.getMetricsExporters()) {
            readers.add(exporter.asMetricReader());
        }
        this.delegatingReader = new DelegatingMetricReader(readers);
        SdkMeterProviderUtil.registerMetricReaderWithCardinalitySelector(meterProviderBuilder, delegatingReader,
                instrumentType -> metricCardinalityLimit);

        return meterProviderBuilder.build();
    }

    protected MetricsExporterURI buildMetricsExporterURI(String exporterUri, MetricsExportConfig metricsExportConfig) {
        return MetricsExporterURI.parse(exporterUri, metricsExportConfig);
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
        List<String> jmxConfigPaths = getJmxConfigPaths();
        if (jmxConfigPaths.isEmpty()) {
            LOGGER.info("No JMX metric config paths provided, skipping JMX metrics registration.");
            return;
        }

        JmxMetricInsight jmxMetricInsight = JmxMetricInsight.createService(openTelemetry, metricsExportConfig.intervalMs());
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

    public List<String> getJmxConfigPaths() {
        if (StringUtils.isEmpty(jmxConfigPath)) {
            return Collections.emptyList();
        }
        return Stream.of(jmxConfigPath.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
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
        if (openTelemetrySdk != null) {
            openTelemetrySdk.close();
        }
    }

    public void reconfigure(String newExporterUri) {
        synchronized (reconfigureLock) {
            if (newExporterUri == null || newExporterUri.equals(this.exporterUri)) {
                LOGGER.debug("Exporter URI unchanged, skipping reconfiguration");
                return;
            }
            LOGGER.info("Reconfiguring metrics exporter from {} to {}", this.exporterUri, newExporterUri);
            this.exporterUri = newExporterUri;
            try {
                MetricsExporterURI exporterURIParsed = buildMetricsExporterURI(newExporterUri, metricsExportConfig);
                List<MetricsExporter> newExporters = exporterURIParsed.getMetricsExporters();
                List<MetricReader> newReaders = new ArrayList<>();
                for (MetricsExporter exporter : newExporters) {
                    newReaders.add(exporter.asMetricReader());
                }
                if (this.delegatingReader != null) {
                    this.delegatingReader.setDelegates(newReaders);
                }
                LOGGER.info("Metrics exporter reconfiguration completed successfully");
            } catch (Exception e) {
                LOGGER.error("Failed to reconfigure metrics exporter with URI: {}", newExporterUri, e);
            }
        }
    }

    public String getExporterUri() {
        return this.exporterUri;
    }

    /**
     * get YammerMetricsReporter instance.
     *
     * @return The YammerMetricsReporter instance.
     */
    public YammerMetricsReporter getYammerReporter() {
        return this.yammerReporter;
    }

    public void setMetricCardinalityLimit(int limit) {
        this.metricCardinalityLimit = limit;
    }

    public void setJmxConfigPaths(String jmxConfigPaths) {
        this.jmxConfigPath = jmxConfigPaths;
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

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        if (configs.containsKey(EXPORTER_URI_CONFIG)) {
            Object value = configs.get(EXPORTER_URI_CONFIG);
            String uri = value instanceof Password ? ((Password) value).value()
                    : (value != null ? value.toString() : null);
            if (uri != null && !uri.isBlank()) {
                try {
                    MetricsExporterURI.parse(uri, metricsExportConfig);
                } catch (Exception e) {
                    throw new ConfigException(EXPORTER_URI_CONFIG, uri, "Invalid exporter URI: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        if (configs.containsKey(EXPORTER_URI_CONFIG)) {
            Object value = configs.get(EXPORTER_URI_CONFIG);
            String newUri;
            if (value instanceof Password) {
                newUri = ((Password) value).value();
            } else {
                newUri = value != null ? value.toString() : null;
            }
            reconfigure(newUri);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
