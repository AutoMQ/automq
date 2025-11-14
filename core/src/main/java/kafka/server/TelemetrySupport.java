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
package kafka.server;

import kafka.automq.table.metric.TableTopicMetricsManager;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager;

import com.automq.opentelemetry.AutoMQTelemetryManager;
import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.shell.AutoMQApplication;
import com.automq.stream.s3.metrics.Metrics;
import com.automq.stream.s3.metrics.MetricsConfig;
import com.automq.stream.s3.metrics.MetricsLevel;
import com.automq.stream.s3.metrics.S3StreamMetricsManager;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import scala.collection.immutable.Set;

/**
 * Helper used by the core module to bootstrap AutoMQ telemetry using the AutoMQTelemetryManager implement.
 */
public final class TelemetrySupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(TelemetrySupport.class);
    private static final String COMMON_JMX_PATH = "/jmx/rules/common.yaml";
    private static final String BROKER_JMX_PATH = "/jmx/rules/broker.yaml";
    private static final String CONTROLLER_JMX_PATH = "/jmx/rules/controller.yaml";
    private static final String KAFKA_METRICS_PREFIX = "kafka_stream_";

    private TelemetrySupport() {
        // Utility class
    }

    public static AutoMQTelemetryManager start(KafkaConfig config, String clusterId) {
        AutoMQTelemetryManager telemetryManager = new AutoMQTelemetryManager(
            config.automq().metricsExporterURI(),
            clusterId,
            String.valueOf(config.nodeId()),
            AutoMQApplication.getBean(MetricsExportConfig.class)
        );
        
        telemetryManager.setJmxConfigPaths(buildJmxConfigPaths(config));
        telemetryManager.init();
        telemetryManager.startYammerMetricsReporter(KafkaYammerMetrics.defaultRegistry());
        initializeMetrics(telemetryManager, config);
        
        return telemetryManager;
    }

    private static void initializeMetrics(AutoMQTelemetryManager manager, KafkaConfig config) {
        S3StreamKafkaMetricsManager.setTruststoreCertsSupplier(() -> {
            try {
                Password password = config.getPassword("ssl.truststore.certificates");
                return password != null ? password.value() : null;
            } catch (Exception e) {
                LOGGER.error("Failed to obtain truststore certificates", e);
                return null;
            }
        });

        S3StreamKafkaMetricsManager.setCertChainSupplier(() -> {
            try {
                Password password = config.getPassword("ssl.keystore.certificate.chain");
                return password != null ? password.value() : null;
            } catch (Exception e) {
                LOGGER.error("Failed to obtain certificate chain", e);
                return null;
            }
        });

        Meter meter = manager.getMeter();
        MetricsLevel metricsLevel = parseMetricsLevel(config.s3MetricsLevel());
        long metricsIntervalMs = (long) config.s3ExporterReportIntervalMs();
        MetricsConfig metricsConfig = new MetricsConfig(metricsLevel, Attributes.empty(), metricsIntervalMs);

        Metrics.instance().setup(meter, metricsConfig);
        S3StreamMetricsManager.configure(new MetricsConfig(metricsLevel, Attributes.empty(), metricsIntervalMs));
        S3StreamMetricsManager.initMetrics(meter, KAFKA_METRICS_PREFIX);

        S3StreamKafkaMetricsManager.configure(new MetricsConfig(metricsLevel, Attributes.empty(), metricsIntervalMs));
        S3StreamKafkaMetricsManager.initMetrics(meter, KAFKA_METRICS_PREFIX);

        TableTopicMetricsManager.initMetrics(meter);
    }

    private static MetricsLevel parseMetricsLevel(String rawLevel) {
        if (StringUtils.isBlank(rawLevel)) {
            return MetricsLevel.INFO;
        }
        
        try {
            return MetricsLevel.valueOf(rawLevel.trim().toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Illegal metrics level '{}', defaulting to INFO", rawLevel);
            return MetricsLevel.INFO;
        }
    }

    private static String buildJmxConfigPaths(KafkaConfig config) {
        List<String> paths = new ArrayList<>();
        paths.add(COMMON_JMX_PATH);
        
        Set<ProcessRole> roles = config.processRoles();
        if (roles.contains(ProcessRole.BrokerRole)) {
            paths.add(BROKER_JMX_PATH);
        }
        if (roles.contains(ProcessRole.ControllerRole)) {
            paths.add(CONTROLLER_JMX_PATH);
        }
        
        return String.join(",", paths);
    }
}
