/*
 * Copyright 2026, AutoMQ HK Limited.
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
package kafka.server;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;

import com.automq.opentelemetry.AutoMQTelemetryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class DynamicTelemetryAdapter implements Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicTelemetryAdapter.class);

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            AutoMQTelemetryManager.EXPORTER_URI_CONFIG,
            AutoMQTelemetryManager.EXPORTER_LABEL_CONFIG
    );

    private final AutoMQTelemetryManager telemetryManager;
    private final KafkaConfig config;

    public DynamicTelemetryAdapter(AutoMQTelemetryManager telemetryManager, KafkaConfig config) {
        this.telemetryManager = telemetryManager;
        this.config = config;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        if (configs.containsKey(AutoMQTelemetryManager.EXPORTER_URI_CONFIG)) {
            telemetryManager.validateExporterUri(configs.get(AutoMQTelemetryManager.EXPORTER_URI_CONFIG));
        }
        if (configs.containsKey(AutoMQTelemetryManager.EXPORTER_LABEL_CONFIG)) {
            Object value = configs.get(AutoMQTelemetryManager.EXPORTER_LABEL_CONFIG);
            String labelStr = value != null ? value.toString() : null;
            telemetryManager.parseLabels(labelStr);
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        boolean requiresReinitialize = telemetryManager.mayApplyConfigChanges(configs);
        if (requiresReinitialize) {
            TelemetrySupport.reinitialize(telemetryManager, config);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
