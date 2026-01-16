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
package org.apache.kafka.connect.cli;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.automq.az.AzMetadataProviderHolder;
import org.apache.kafka.connect.automq.log.ConnectLogUploader;
import org.apache.kafka.connect.automq.metrics.MetricsConfigConstants;
import org.apache.kafka.connect.automq.metrics.OpenTelemetryMetricsReporter;
import org.apache.kafka.connect.automq.s3.S3PermissionProbe;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Common initialization logic for Kafka Connect, intended for use by command line utilities
 *
 * @param <H> the type of {@link Herder} to be used
 * @param <T> the type of {@link WorkerConfig} to be used
 */
public abstract class AbstractConnectCli<H extends Herder, T extends WorkerConfig> {

    private static Logger getLogger() {
        return LoggerFactory.getLogger(AbstractConnectCli.class);
    }
    private final String[] args;
    private final Time time = Time.SYSTEM;

    /**
     *
     * @param args the CLI arguments to be processed. Note that if one or more arguments are passed, the first argument is
     *             assumed to be the Connect worker properties file and is processed in {@link #run()}. The remaining arguments
     *             can be handled in {@link #processExtraArgs(Connect, String[])}
     */
    protected AbstractConnectCli(String... args) {
        this.args = args;
    }

    protected abstract String usage();

    /**
     * The first CLI argument is assumed to be the Connect worker properties file and is processed by default. This method
     * can be overridden if there are more arguments that need to be processed.
     *
     * @param connect the {@link Connect} instance that can be stopped (via {@link Connect#stop()}) if there's an error
     *                encountered while processing the additional CLI arguments.
     * @param extraArgs the extra CLI arguments that need to be processed
     */
    public void processExtraArgs(Connect<H> connect, String[] extraArgs) {
    }

    protected abstract H createHerder(T config, String workerId, Plugins plugins,
                                           ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                           RestServer restServer, RestClient restClient);

    protected abstract T createConfig(Map<String, String> workerProps);

    /**
     *  Validate {@link #args}, process worker properties from the first CLI argument, and start {@link Connect}
     */
    public void run() {
        if (args.length < 1 || Arrays.asList(args).contains("--help")) {
            Exit.exit(1);
        }

        try {
            String workerPropsFile = args[0];
            Map<String, String> workerProps = !workerPropsFile.isEmpty() ?
                    Utils.propsToStringMap(Utils.loadProps(workerPropsFile)) : Collections.emptyMap();
            String[] extraArgs = Arrays.copyOfRange(args, 1, args.length);

            // AutoMQ inject start
            // Initialize S3 log uploader if the target bucket is ready
            S3PermissionProbe.ProbeResult logProbe = S3PermissionProbe.probeLogUploader(workerProps);
            if (logProbe.shouldInitialize()) {
                ConnectLogUploader.initialize(workerProps);
            } else if (logProbe.isRequired()) {
                getLogger().warn("Skip starting S3 log uploader: {}", logProbe.reason());
            }
            AzMetadataProviderHolder.initialize(workerProps);

            initializeTelemetry(workerProps);
            // AutoMQ inject end

            Connect<H> connect = startConnect(workerProps);
            processExtraArgs(connect, extraArgs);

            // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
            connect.awaitStop();

        } catch (Throwable t) {
            getLogger().error("Stopping due to error", t);
            Exit.exit(2);
        }
    }

    /**
     * Initialize and start an instance of {@link Connect}
     *
     * @param workerProps the worker properties map used to initialize the {@link WorkerConfig}
     * @return a started instance of {@link Connect}
     */
    public Connect<H> startConnect(Map<String, String> workerProps) {
        getLogger().info("Kafka Connect worker initializing ...");
        long initStart = time.hiResClockMs();

        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();

        getLogger().info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        T config = createConfig(workerProps);
        getLogger().debug("Kafka cluster ID: {}", config.kafkaClusterId());

        RestClient restClient = new RestClient(config);

        ConnectRestServer restServer = new ConnectRestServer(config.rebalanceTimeout(), restClient, config.originals());
        restServer.initializeServer();

        URI advertisedUrl = restServer.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        H herder = createHerder(config, workerId, plugins, connectorClientConfigOverridePolicy, restServer, restClient);

        final Connect<H> connect = new Connect<>(herder, restServer);
        getLogger().info("Kafka Connect worker initialization took {}ms", time.hiResClockMs() - initStart);
        try {
            connect.start();
        } catch (Exception e) {
            getLogger().error("Failed to start Connect", e);
            connect.stop();
            Exit.exit(3);
        }

        return connect;
    }

    private void initializeTelemetry(Map<String, String> workerProps) {
        List<String> exporterUris = parseTelemetryExporterUris(workerProps.get(MetricsConfigConstants.EXPORTER_URI_KEY));
        if (exporterUris.isEmpty()) {
            getLogger().info("AutoMQ telemetry exporter URI is not configured; skipping telemetry initialization.");
            return;
        }

        S3PermissionProbe.ProbeResult metricsProbe = S3PermissionProbe.probeMetrics(workerProps);
        if (!metricsProbe.isRequired()) {
            startTelemetry(workerProps, exporterUris, false);
            return;
        }

        if (metricsProbe.shouldInitialize()) {
            startTelemetry(workerProps, exporterUris, false);
            return;
        }

        List<String> filteredExporters = filterOutOpsExporters(exporterUris);
        if (filteredExporters.isEmpty()) {
            getLogger().warn("Skip starting AutoMQ telemetry exporter: {}. S3 exporter unavailable and no alternative exporters configured.", metricsProbe.reason());
            return;
        }

        getLogger().warn("AutoMQ S3 telemetry exporter disabled ({}). Continuing with exporters: {}",
            metricsProbe.reason(), joinExporterUris(filteredExporters));
        startTelemetry(workerProps, filteredExporters, true);
    }

    private static List<String> parseTelemetryExporterUris(String exporterConfig) {
        List<String> exporters = new ArrayList<>();
        if (exporterConfig == null) {
            return exporters;
        }
        String[] entries = exporterConfig.split(",");
        for (String entry : entries) {
            String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                exporters.add(trimmed);
            }
        }
        return exporters;
    }

    private static List<String> filterOutOpsExporters(List<String> exporters) {
        List<String> filtered = new ArrayList<>(exporters.size());
        for (String exporter : exporters) {
            if (!isOpsExporter(exporter)) {
                filtered.add(exporter);
            }
        }
        return filtered;
    }

    private static boolean isOpsExporter(String exporter) {
        try {
            URI uri = URI.create(exporter);
            String scheme = uri.getScheme();
            return scheme != null && scheme.equalsIgnoreCase("ops");
        } catch (Exception e) {
            getLogger().warn("Invalid AutoMQ telemetry exporter URI '{}', ignoring for S3 readiness gating", exporter, e);
            return false;
        }
    }

    private static String joinExporterUris(List<String> exporters) {
        return String.join(",", exporters);
    }

    private void startTelemetry(Map<String, String> workerProps, List<String> exporters, boolean removeS3TelemetryConfig) {
        Properties telemetryProps = new Properties();
        telemetryProps.putAll(workerProps);
        telemetryProps.put(MetricsConfigConstants.EXPORTER_URI_KEY, joinExporterUris(exporters));
        if (removeS3TelemetryConfig) {
            telemetryProps.remove(MetricsConfigConstants.S3_BUCKET);
        }
        OpenTelemetryMetricsReporter.initializeTelemetry(telemetryProps);
    }
}
