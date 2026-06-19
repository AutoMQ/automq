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

package com.automq.opentelemetry.exporter;

import com.automq.opentelemetry.common.OTLPCompressionType;
import com.automq.opentelemetry.common.OTLPProtocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.opentelemetry.sdk.metrics.export.MetricReader;

@Tag("S3Unit")
public class OTLPMetricsExporterTest {

    /**
     * Given a gRPC OTLP exporter configuration, when the metric reader is built,
     * then the OpenTelemetry gRPC sender provider must be available on the runtime classpath.
     */
    @Test
    public void testBuildGrpcMetricReaderWithSenderProvider() {
        OTLPMetricsExporter exporter = new OTLPMetricsExporter(
            1000,
            "http://localhost:4317",
            OTLPProtocol.GRPC.getProtocol(),
            OTLPCompressionType.NONE.getType()
        );

        MetricReader reader = exporter.asMetricReader();
        Assertions.assertNotNull(reader);
        reader.shutdown();
    }
}
