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

import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.utils.Threads;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

/**
 * An S3 metrics exporter that uploads metrics data to S3 buckets.
 */
public class S3MetricsExporter implements MetricExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetricsExporter.class);

    public static final int UPLOAD_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL") != null ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL")) : 60 * 1000;
    public static final int CLEANUP_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL") != null ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL")) : 2 * 60 * 1000;
    public static final int MAX_JITTER_INTERVAL = 60 * 1000;
    public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

    private final S3MetricsConfig config;
    private final Map<String, String> defaultTagMap = new HashMap<>();

    private final ByteBuf uploadBuffer = Unpooled.directBuffer(DEFAULT_BUFFER_SIZE);
    private final Random random = new Random();
    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private volatile long nextUploadInterval = UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);

    private final ObjectStorage objectStorage;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile boolean closed;
    private final Thread uploadThread;
    private final Thread cleanupThread;

    /**
     * Creates a new S3MetricsExporter.
     * 
     * @param config The configuration for the S3 metrics exporter.
     */
    public S3MetricsExporter(S3MetricsConfig config) {
        this.config = config;
        this.objectStorage = config.objectStorage();

        defaultTagMap.put("host_name", getHostName());
        defaultTagMap.put("job", config.clusterId());
        defaultTagMap.put("instance", String.valueOf(config.nodeId()));
        config.baseLabels().forEach(pair -> defaultTagMap.put(PrometheusUtils.mapLabelName(pair.getKey()), pair.getValue()));

        uploadThread = new Thread(new UploadTask());
        uploadThread.setName("s3-metrics-exporter-upload-thread");
        uploadThread.setDaemon(true);

        cleanupThread = new Thread(new CleanupTask());
        cleanupThread.setName("s3-metrics-exporter-cleanup-thread");
        cleanupThread.setDaemon(true);
    }

    /**
     * Starts the exporter threads.
     */
    public void start() {
        uploadThread.start();
        cleanupThread.start();
        LOGGER.info("S3MetricsExporter is started");
    }

    @Override
    public void close() {
        MetricExporter.super.close();
        closed = true;
        cleanupThread.interrupt();
        uploadThread.interrupt();
        LOGGER.info("S3MetricsExporter is closed");
    }

    private class UploadTask implements Runnable {

        @Override
        public void run() {
            while (!closed && !uploadThread.isInterrupted()) {
                try {
                    if (uploadBuffer.readableBytes() > 0 && System.currentTimeMillis() - lastUploadTimestamp > nextUploadInterval) {
                        flush();
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class CleanupTask implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (closed || !config.isPrimaryUploader()) {
                        Thread.sleep(Duration.ofMinutes(1).toMillis());
                        continue;
                    }
                    long expiredTime = System.currentTimeMillis() - CLEANUP_INTERVAL;

                    List<ObjectInfo> objects = objectStorage.list(String.format("automq/metrics/%s", config.clusterId())).join();

                    if (!objects.isEmpty()) {
                        List<ObjectPath> keyList = objects.stream()
                            .filter(object -> object.timestamp() < expiredTime)
                            .map(object -> new ObjectPath(object.bucketId(), object.key()))
                            .collect(Collectors.toList());

                        if (!keyList.isEmpty()) {
                            // Some of s3 implements allow only 1000 keys per request.
                            CompletableFuture<?>[] deleteFutures = Lists.partition(keyList, 1000)
                                .stream()
                                .map(objectStorage::delete)
                                .toArray(CompletableFuture[]::new);
                            CompletableFuture.allOf(deleteFutures).join();
                        }
                    }
                    if (Threads.sleep(Duration.ofMinutes(1).toMillis())) {
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    LOGGER.error("Cleanup s3 metrics failed", e);
                    if (Threads.sleep(Duration.ofMinutes(1).toMillis())) {
                        break;
                    }
                }
            }
        }
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            LOGGER.error("Failed to get host name", e);
            return "unknown";
        }
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        if (closed) {
            return CompletableResultCode.ofFailure();
        }

        try {
            List<String> lineList = new ArrayList<>();
            for (MetricData metric : metrics) {
                switch (metric.getType()) {
                    case LONG_SUM:
                        metric.getLongSumData().getPoints().forEach(point ->
                            lineList.add(serializeCounter(
                                PrometheusUtils.mapMetricsName(metric.getName(), metric.getUnit(), metric.getLongSumData().isMonotonic(), false),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case DOUBLE_SUM:
                        metric.getDoubleSumData().getPoints().forEach(point ->
                            lineList.add(serializeCounter(
                                PrometheusUtils.mapMetricsName(metric.getName(), metric.getUnit(), metric.getDoubleSumData().isMonotonic(), false),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case LONG_GAUGE:
                        metric.getLongGaugeData().getPoints().forEach(point ->
                            lineList.add(serializeGauge(
                                PrometheusUtils.mapMetricsName(metric.getName(), metric.getUnit(), false, true),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case DOUBLE_GAUGE:
                        metric.getDoubleGaugeData().getPoints().forEach(point ->
                            lineList.add(serializeGauge(
                                PrometheusUtils.mapMetricsName(metric.getName(), metric.getUnit(), false, true),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case HISTOGRAM:
                        metric.getHistogramData().getPoints().forEach(point ->
                            lineList.add(serializeHistogram(
                                PrometheusUtils.mapMetricsName(metric.getName(), metric.getUnit(), false, false),
                                point)));
                        break;
                    default:
                }
            }

            int size = lineList.stream().mapToInt(line -> line.length() + 1 /*the newline character*/).sum();
            ByteBuf buffer = Unpooled.buffer(size);
            lineList.forEach(line -> {
                buffer.writeCharSequence(line, Charset.defaultCharset());
                buffer.writeCharSequence("\n", Charset.defaultCharset());
            });
            synchronized (uploadBuffer) {
                if (uploadBuffer.writableBytes() < buffer.readableBytes()) {
                    // Upload the buffer immediately
                    flush();
                }
                uploadBuffer.writeBytes(buffer);
            }
        } catch (Exception e) {
            LOGGER.error("Export metrics to S3 failed", e);
            return CompletableResultCode.ofFailure();
        }

        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        synchronized (uploadBuffer) {
            if (uploadBuffer.readableBytes() > 0) {
                try {
                    objectStorage.write(WriteOptions.DEFAULT, getObjectKey(), CompressionUtils.compress(uploadBuffer.slice().asReadOnly())).get();
                } catch (Exception e) {
                    LOGGER.error("Failed to upload metrics to s3", e);
                    return CompletableResultCode.ofFailure();
                } finally {
                    lastUploadTimestamp = System.currentTimeMillis();
                    nextUploadInterval = UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);
                    uploadBuffer.clear();
                }
            }
        }
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        objectStorage.close();
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return AggregationTemporality.CUMULATIVE;
    }

    private String getObjectKey() {
        String hour = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
        return String.format("automq/metrics/%s/%s/%s/%s", config.clusterId(), config.nodeId(), hour, UUID.randomUUID());
    }

    private String serializeCounter(String name, double value, Attributes attributes, long timestampNanos) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");

        root.put("timestamp", TimeUnit.NANOSECONDS.toSeconds(timestampNanos));
        root.put("name", name);
        root.set("counter", objectMapper.createObjectNode().put("value", value));

        ObjectNode tags = objectMapper.createObjectNode();
        defaultTagMap.forEach(tags::put);
        attributes.forEach((k, v) -> tags.put(PrometheusUtils.mapLabelName(k.getKey()), v.toString()));
        root.set("tags", tags);

        return root.toString();
    }

    private String serializeGauge(String name, double value, Attributes attributes, long timestampNanos) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");

        root.put("timestamp", TimeUnit.NANOSECONDS.toSeconds(timestampNanos));
        root.put("name", name);
        root.set("gauge", objectMapper.createObjectNode().put("value", value));

        ObjectNode tags = objectMapper.createObjectNode();
        defaultTagMap.forEach(tags::put);
        attributes.forEach((k, v) -> tags.put(PrometheusUtils.mapLabelName(k.getKey()), v.toString()));
        root.set("tags", tags);

        return root.toString();
    }

    private String serializeHistogram(String name, HistogramPointData point) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");

        root.put("timestamp", TimeUnit.NANOSECONDS.toSeconds(point.getEpochNanos()));
        root.put("name", name);

        ObjectNode histogram = objectMapper.createObjectNode();
        histogram.put("count", point.getCount());
        histogram.put("sum", point.getSum());

        ArrayNode buckets = objectMapper.createArrayNode();
        for (int i = 0; i < point.getCounts().size(); i++) {
            ObjectNode bucket = objectMapper.createObjectNode();
            bucket.put("count", point.getCounts().get(i));
            float upperBound = getBucketUpperBound(point, i);
            if (upperBound == Float.POSITIVE_INFINITY) {
                bucket.put("upper_limit", Float.MAX_VALUE);
            } else {
                bucket.put("upper_limit", upperBound);
            }
            buckets.add(bucket);
        }
        histogram.set("buckets", buckets);
        root.set("histogram", histogram);

        ObjectNode tags = objectMapper.createObjectNode();
        defaultTagMap.forEach(tags::put);
        point.getAttributes().forEach((k, v) -> tags.put(PrometheusUtils.mapLabelName(k.getKey()), v.toString()));
        root.set("tags", tags);

        return root.toString();
    }

    private float getBucketUpperBound(HistogramPointData point, int bucketIndex) {
        List<Double> boundaries = point.getBoundaries();
        return (bucketIndex < boundaries.size())
            ? boundaries.get(bucketIndex).floatValue()
            : Float.MAX_VALUE;
    }
}
