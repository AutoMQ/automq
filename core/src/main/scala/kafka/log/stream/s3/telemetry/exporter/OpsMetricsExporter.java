/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.stream.s3.telemetry.exporter;

import com.automq.shell.AutoMQApplication;
import com.automq.shell.metrics.S3MetricsConfig;
import com.automq.shell.metrics.S3MetricsExporter;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;
import java.util.List;
import kafka.server.KafkaRaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpsMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpsMetricsExporter.class);
    private final String clusterId;
    private final int nodeId;
    private final int intervalMs;
    private final List<BucketURI> opsBuckets;

    public OpsMetricsExporter(String clusterId, int nodeId, int intervalMs, List<BucketURI> opsBuckets) {
        if (opsBuckets == null || opsBuckets.isEmpty()) {
            throw new IllegalArgumentException("At least one bucket URI must be provided for ops metrics exporter");
        }
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.intervalMs = intervalMs;
        this.opsBuckets = opsBuckets;
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
        ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(bucket).build();
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
        };
        S3MetricsExporter s3MetricsExporter = new S3MetricsExporter(metricsConfig);
        s3MetricsExporter.start();
        return PeriodicMetricReader.builder(s3MetricsExporter).setInterval(Duration.ofMillis(intervalMs)).build();
    }
}
