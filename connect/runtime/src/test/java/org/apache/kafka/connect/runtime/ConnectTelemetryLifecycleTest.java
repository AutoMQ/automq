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

package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.automq.az.AzMetadataProviderHolder;
import org.apache.kafka.connect.automq.metrics.OpenTelemetryMetricsReporter;
import org.apache.kafka.connect.cli.ConnectStandalone;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests cleanup through process stop and command-line startup failure. */
@Tag("S3Unit")
public class ConnectTelemetryLifecycleTest {
    /** Given repeated process stops, shared telemetry is released exactly once after the herder. */
    @Test
    void testStopReleasesTelemetryOnce() {
        Herder herder = mock(Herder.class);
        ConnectRestServer rest = mock(ConnectRestServer.class);
        try (MockedStatic<OpenTelemetryMetricsReporter> telemetry = mockStatic(OpenTelemetryMetricsReporter.class)) {
            Connect<Herder> connect = new Connect<>(herder, rest);
            connect.stop();
            connect.stop();
            verify(herder).stop();
            telemetry.verify(OpenTelemetryMetricsReporter::shutdownTelemetry);
        }
    }

    /** Given a REST stop failure, shared telemetry is still released. */
    @Test
    void testStopFailureStillReleasesTelemetry() {
        ConnectRestServer rest = mock(ConnectRestServer.class);
        doThrow(new IllegalStateException("REST stop failed")).when(rest).stop();
        try (MockedStatic<OpenTelemetryMetricsReporter> telemetry = mockStatic(OpenTelemetryMetricsReporter.class)) {
            Connect<Herder> connect = new Connect<>(mock(Herder.class), rest);
            assertThrows(IllegalStateException.class, connect::stop);
            telemetry.verify(OpenTelemetryMetricsReporter::shutdownTelemetry);
        }
    }

    /** Given failure after telemetry initialization, the CLI closes it before requesting process exit. */
    @Test
    void testStartupFailureReleasesTelemetry(@TempDir Path directory) throws Exception {
        Path properties = directory.resolve("worker.properties");
        Files.writeString(properties, "automq.telemetry.exporter.uri=prometheus://localhost:9090\n");
        ConnectStandalone cli = spy(new ConnectStandalone(properties.toString()));
        doThrow(new IllegalStateException("Worker start failed")).when(cli).startConnect(anyMap());
        AtomicInteger exitStatus = new AtomicInteger();
        Exit.setExitProcedure((status, message) -> exitStatus.set(status));
        try (MockedStatic<OpenTelemetryMetricsReporter> telemetry = mockStatic(OpenTelemetryMetricsReporter.class);
             MockedStatic<AzMetadataProviderHolder> az = mockStatic(AzMetadataProviderHolder.class)) {
            cli.run();
            telemetry.verify(() -> OpenTelemetryMetricsReporter.initializeTelemetry(any(Properties.class)));
            telemetry.verify(OpenTelemetryMetricsReporter::shutdownTelemetry);
            assertEquals(2, exitStatus.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
