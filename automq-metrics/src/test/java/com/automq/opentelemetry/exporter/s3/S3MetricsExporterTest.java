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

package com.automq.opentelemetry.exporter.s3;

import com.automq.opentelemetry.exporter.MetricsExportConfig;
import com.automq.stream.s3.operator.ObjectStorage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class S3MetricsExporterTest {

    /**
     * An unresponsive upload must release the buffer lock so the final export
     * and exporter shutdown can finish during broker shutdown.
     */
    @Test
    void testPendingUploadDoesNotBlockFinalExportAndShutdown() throws Exception {
        ObjectStorage storage = mock(ObjectStorage.class);
        CompletableFuture<ObjectStorage.WriteResult> upload = new CompletableFuture<>();
        CountDownLatch uploading = new CountDownLatch(1);
        when(storage.write(any(), anyString(), any())).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(2);
            buffer.release();
            uploading.countDown();
            return upload;
        });
        S3MetricsExporter exporter = new S3MetricsExporter(config(storage));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            assertTrue(exporter.export(List.of(metric())).isSuccess());
            Future<CompletableResultCode> flush = executor.submit(exporter::flush);
            assertTrue(uploading.await(10, TimeUnit.SECONDS));
            Future<?> shutdown = executor.submit(() -> {
                assertTrue(exporter.export(List.of(metric())).isSuccess());
                assertTrue(exporter.shutdown().isSuccess());
            });

            shutdown.get(10, TimeUnit.SECONDS);
            assertFalse(flush.get(10, TimeUnit.SECONDS).isSuccess());
            verify(storage).close();
        } finally {
            upload.complete(new ObjectStorage.WriteResult((short) 0));
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /**
     * A completed upload remains successful and an empty flush does not resend it.
     */
    @Test
    void testSuccessfulUploadClearsBuffer() {
        ObjectStorage storage = mock(ObjectStorage.class);
        when(storage.write(any(), anyString(), any())).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(2);
            buffer.release();
            return CompletableFuture.completedFuture(new ObjectStorage.WriteResult((short) 0));
        });
        S3MetricsExporter exporter = new S3MetricsExporter(config(storage));
        try {
            assertTrue(exporter.export(List.of(metric())).isSuccess());
            assertTrue(exporter.flush().isSuccess());
            assertTrue(exporter.flush().isSuccess());
            verify(storage).write(any(), anyString(), any());
        } finally {
            exporter.shutdown();
        }
    }

    private MetricsExportConfig config(ObjectStorage storage) {
        MetricsExportConfig config = mock(MetricsExportConfig.class);
        when(config.objectStorage()).thenReturn(storage);
        when(config.clusterId()).thenReturn("test-cluster");
        when(config.baseLabels()).thenReturn(List.of());
        return config;
    }

    private MetricData metric() {
        return ImmutableMetricData.createLongGauge(Resource.empty(), InstrumentationScopeInfo.empty(),
            "test_gauge", "", "", ImmutableGaugeData.create(List.of(
                ImmutableLongPointData.create(0, 1, Attributes.empty(), 1))));
    }
}
