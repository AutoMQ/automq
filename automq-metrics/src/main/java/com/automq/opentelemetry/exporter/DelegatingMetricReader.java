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

package com.automq.opentelemetry.exporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;

public class DelegatingMetricReader implements MetricReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingMetricReader.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private volatile List<MetricReader> delegates;
    private volatile CollectionRegistration collectionRegistration;
    private final Object lock = new Object();

    public DelegatingMetricReader(List<MetricReader> initialDelegates) {
        this.delegates = new ArrayList<>(initialDelegates);
    }

    public void setDelegates(List<MetricReader> newDelegates) {
        synchronized (lock) {
            List<MetricReader> oldDelegates = this.delegates;
            if (collectionRegistration != null && newDelegates != null) {
                for (MetricReader reader : newDelegates) {
                    reader.register(collectionRegistration);
                }
            }
            this.delegates = new ArrayList<>(newDelegates);
            if (oldDelegates != null && !oldDelegates.isEmpty()) {
                List<CompletableResultCode> shutdownResults = new ArrayList<>();
                for (MetricReader oldDelegate : oldDelegates) {
                    try {
                        shutdownResults.add(oldDelegate.shutdown());
                    } catch (Exception e) {
                        LOGGER.warn("Error shutting down old delegate", e);
                    }
                }
                CompletableResultCode.ofAll(shutdownResults)
                        .join(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    public List<MetricReader> getDelegates() {
        return Collections.unmodifiableList(delegates);
    }

    @Override
    public void register(CollectionRegistration registration) {
        synchronized (lock) {
            this.collectionRegistration = registration;
            for (MetricReader delegate : delegates) {
                delegate.register(registration);
            }
        }
    }

    @Override
    public CompletableResultCode forceFlush() {
        List<MetricReader> current = delegates;
        if (current.isEmpty()) {
            return CompletableResultCode.ofSuccess();
        }
        List<CompletableResultCode> codes = new ArrayList<>();
        for (MetricReader reader : current) {
            codes.add(reader.forceFlush());
        }
        return CompletableResultCode.ofAll(codes);
    }

    @Override
    public CompletableResultCode shutdown() {
        synchronized (lock) {
            List<MetricReader> current = delegates;
            if (current.isEmpty()) {
                return CompletableResultCode.ofSuccess();
            }
            List<CompletableResultCode> codes = new ArrayList<>();
            for (MetricReader reader : current) {
                codes.add(reader.shutdown());
            }
            return CompletableResultCode.ofAll(codes);
        }
    }

    @Override
    public void close() throws IOException {
        shutdown().join(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        List<MetricReader> current = delegates;
        if (!current.isEmpty()) {
            // should be consistent for all the MetricReader
            return current.get(0).getAggregationTemporality(instrumentType);
        }
        return AggregationTemporality.CUMULATIVE;
    }
}
