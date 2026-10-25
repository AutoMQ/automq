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

package kafka.automq;

import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static kafka.automq.AutoMQConfig.S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoMQConfigTest {

    /**
     * Given an OTLP endpoint containing "ops", only an exporter with the OPS scheme should suppress the default.
     */
    @Test
    @Tag("S3Unit")
    public void testMetricsExporterDetectionUsesScheme() {
        KafkaConfig config = mock(KafkaConfig.class);
        String otlpUri = "otlp://?endpoint=https://telemetry.ops.example.com";
        when(config.getPassword(S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn(new Password(otlpUri));

        assertEquals(otlpUri + ",ops://?", AutoMQConfig.genMetricsExporterURI(config));

        String uriWithOpsExporter = otlpUri + ",OPS://?";
        when(config.getPassword(S3_TELEMETRY_METRICS_EXPORTER_URI_CONFIG)).thenReturn(new Password(uriWithOpsExporter));
        assertEquals(uriWithOpsExporter, AutoMQConfig.genMetricsExporterURI(config));
    }
}
