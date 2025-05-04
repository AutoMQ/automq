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

package kafka.log.stream.s3.telemetry.exporter;

import kafka.server.KafkaRaftServer;

import com.automq.shell.AutoMQApplication;
import com.automq.shell.metrics.S3MetricsConfig;
import com.automq.shell.metrics.S3MetricsExporter;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;

public class OpsMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpsMetricsExporter.class);
    private final String clusterId;
    private final int nodeId;
    private final int intervalMs;
    private final List<BucketURI> opsBuckets;
    private final List<Pair<String, String>> baseLabels;

    public OpsMetricsExporter(String clusterId, int nodeId, int intervalMs, List<BucketURI> opsBuckets, List<Pair<String, String>> baseLabels) {
        if (opsBuckets == null || opsBuckets.isEmpty()) {
            throw new IllegalArgumentException("At least one bucket URI must be provided for ops metrics exporter");
        }
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.intervalMs = intervalMs;
        this.opsBuckets = opsBuckets;
        this.baseLabels = baseLabels;
        LOGGER.info("OpsMetricsExporter initialized with clusterId: {}, nodeId: {}, intervalMs: {}, opsBuckets: {}",
            clusterId, nodeId, intervalMs, opsBuckets);
    }

    public String clusterId() {
        return clusterId;
    }

    public int nodeId() {
        return nodeId;
    }

    public int intervalMs() {
        return intervalMs;
    }

    public List<BucketURI> opsBuckets() {
        return opsBuckets;
    }

    @Override
    public MetricReader asMetricReader() {
        BucketURI bucket = opsBuckets.get(0);
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(bucket).threadPrefix("ops-metric").build();
        S3MetricsConfig metricsConfig = new S3MetricsConfig() {
            @Override
            public String clusterId() {
                return clusterId;
            }

            @Override
            public boolean isActiveController() {
                KafkaRaftServer raftServer = AutoMQApplication.getBean(KafkaRaftServer.class);
                return raftServer != null && raftServer.controller().exists(controller -> controller.controller() != null
                    && controller.controller().isActive());
            }

            @Override
            public int nodeId() {
                return nodeId;
            }

            @Override
            public ObjectStorage objectStorage() {
                return objectStorage;
            }

            @Override
            public List<Pair<String, String>> baseLabels() {
                return baseLabels;
            }
        };
        S3MetricsExporter s3MetricsExporter = new S3MetricsExporter(metricsConfig);
        s3MetricsExporter.start();
        return PeriodicMetricReader.builder(s3MetricsExporter).setInterval(Duration.ofMillis(intervalMs)).build();
    }
}
