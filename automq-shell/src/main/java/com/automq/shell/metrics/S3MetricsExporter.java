/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.shell.metrics;

import com.automq.shell.auth.CredentialsProviderHolder;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3MetricsExporter implements MetricExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetricsExporter.class);

    private final String clusterId;
    private final S3Operator s3Operator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public S3MetricsExporter(S3MetricsConfig config, String clusterId) {
        this.clusterId = clusterId;
        s3Operator = new DefaultS3Operator(config.s3Endpoint(), config.s3Region(),
            config.s3Bucket(), config.s3PathStyle(), List.of(CredentialsProviderHolder.getAwsCredentialsProvider()), false);
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        try {
            List<String> lineList = new ArrayList<>();
            // TODO: transfer metrics name into prometheus format
            for (MetricData metric : metrics) {
                switch (metric.getType()) {
                    case LONG_SUM:
                        metric.getLongSumData().getPoints().forEach(point ->
                            lineList.add(serializeCounter(metric.getName(), point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case DOUBLE_SUM:
                        metric.getDoubleSumData().getPoints().forEach(point ->
                            lineList.add(serializeCounter(metric.getName(), point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case LONG_GAUGE:
                        metric.getLongGaugeData().getPoints().forEach(point ->
                            lineList.add(serializeGauge(metric.getName(), point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case DOUBLE_GAUGE:
                        metric.getDoubleGaugeData().getPoints().forEach(point ->
                            lineList.add(serializeGauge(metric.getName(), point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case HISTOGRAM:
                        metric.getHistogramData().getPoints().forEach(point ->
                            lineList.add(serializeHistogram(metric.getName(), point)));
                        break;
                    default:
                }
            }

            ByteBuf buffer = Unpooled.buffer();
            lineList.forEach(line -> {
                buffer.writeCharSequence(line, Charset.defaultCharset());
                buffer.writeCharSequence("\n", Charset.defaultCharset());
            });
            s3Operator.write(getObjectKey(), buffer);
        } catch (Exception e) {
            LOGGER.error("Failed to export metrics to S3", e);
            return CompletableResultCode.ofFailure();
        }

        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        s3Operator.close();
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return AggregationTemporality.CUMULATIVE;
    }

    private String getObjectKey() {
        String today = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        return String.format("automq/metrics/cluster/%s/%s/%s", clusterId, today, UUID.randomUUID());
    }

    private String serializeCounter(String name, double value, Attributes attributes, long timestampNanos) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");
        root.put("namespace", "");

        root.put("timestamp", Long.toString(TimeUnit.NANOSECONDS.toSeconds(timestampNanos)));
        root.put("name", name);
        root.set("counter", objectMapper.createObjectNode().put("value", value));

        ObjectNode tags = objectMapper.createObjectNode();
        attributes.forEach((k, v) -> tags.put(k.getKey(), v.toString()));
        root.set("tags", tags);

        return root.toString();
    }

    private String serializeGauge(String name, double value, Attributes attributes, long timestampNanos) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");
        root.put("namespace", "");

        root.put("timestamp", Long.toString(TimeUnit.NANOSECONDS.toSeconds(timestampNanos)));
        root.put("name", name);
        root.set("gauge", objectMapper.createObjectNode().put("value", value));

        ObjectNode tags = objectMapper.createObjectNode();
        attributes.forEach((k, v) -> tags.put(k.getKey(), v.toString()));
        root.set("tags", tags);

        return root.toString();
    }

    private String serializeHistogram(String name, HistogramPointData point) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");
        root.put("namespace", "");

        root.put("timestamp", Long.toString(TimeUnit.NANOSECONDS.toSeconds(point.getEpochNanos())));
        root.put("name", name);

        ObjectNode histogram = objectMapper.createObjectNode();
        histogram.put("count", point.getCount());
        histogram.put("sum", point.getSum());

        ArrayNode buckets = objectMapper.createArrayNode();
        for (int i = 0; i < point.getCounts().size(); i++) {
            ObjectNode bucket = objectMapper.createObjectNode();
            bucket.put("count", point.getCounts().get(i));
            bucket.put("upper_limit", getBucketUpperBound(point, i));
            buckets.add(bucket);
        }
        histogram.set("buckets", buckets);
        root.set("histogram", histogram);

        ObjectNode tags = objectMapper.createObjectNode();
        point.getAttributes().forEach((k, v) -> tags.put(k.getKey(), v.toString()));
        root.set("tags", tags);

        return root.toString();
    }

    private double getBucketUpperBound(HistogramPointData point, int bucketIndex) {
        List<Double> boundaries = point.getBoundaries();
        return (bucketIndex < boundaries.size())
            ? boundaries.get(bucketIndex)
            : Double.POSITIVE_INFINITY;
    }
}
