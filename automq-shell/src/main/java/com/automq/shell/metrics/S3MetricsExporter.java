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
import com.automq.stream.s3.network.ThrottleStrategy;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3MetricsExporter implements MetricExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3MetricsExporter.class);
    private static final String TOTAL_SUFFIX = "_total";

    public static final int UPLOAD_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL") != null ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL")) : 60 * 1000;
    public static final int CLEANUP_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL") != null ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL")) : 2 * 60 * 1000;
    public static final int MAX_JITTER_INTERVAL = 60 * 1000;
    public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

    private final S3MetricsConfig config;
    private final Map<String, String> defalutTagMap = new HashMap<>();

    private final ByteBuf uploadBuffer = Unpooled.directBuffer(DEFAULT_BUFFER_SIZE);
    private final Random random = new Random();
    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private volatile long nextUploadInterval = UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);

    private final S3Operator s3Operator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private volatile boolean closed;
    private final Thread uploadThread;
    private final Thread cleanupThread;

    public S3MetricsExporter(S3MetricsConfig config) {
        this.config = config;
        s3Operator = new DefaultS3Operator(config.s3Endpoint(), config.s3Region(),
            config.s3OpsBucket(), config.s3PathStyle(), List.of(CredentialsProviderHolder.getAwsCredentialsProvider()), false);

        defalutTagMap.put("host_name", getHostName());
        defalutTagMap.put("service_name", config.clusterId());
        defalutTagMap.put("job", config.clusterId());
        defalutTagMap.put("service_instance_id", String.valueOf(config.nodeId()));
        defalutTagMap.put("instance", String.valueOf(config.nodeId()));

        uploadThread = new Thread(new UploadTask());
        uploadThread.setName("s3-metrics-exporter-upload-thread");
        uploadThread.setDaemon(true);

        cleanupThread = new Thread(new CleanupTask());
        cleanupThread.setName("s3-metrics-exporter-cleanup-thread");
        cleanupThread.setDaemon(true);
    }

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
        flush();
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
                    if (closed || !config.isActiveController()) {
                        Thread.sleep(Duration.ofMinutes(1).toMillis());
                        continue;
                    }
                    long expiredTime = System.currentTimeMillis() - CLEANUP_INTERVAL;

                    List<Pair<String, Long>> pairList = s3Operator.list(String.format("automq/metrics/%s", config.clusterId())).join();

                    if (!pairList.isEmpty()) {
                        List<String> keyList = pairList.stream()
                            .filter(pair -> pair.getRight() < expiredTime)
                            .map(Pair::getLeft)
                            .collect(Collectors.toList());
                        s3Operator.delete(keyList).join();
                    }

                    Thread.sleep(Duration.ofMinutes(1).toMillis());
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    LOGGER.error("Cleanup s3 metrics failed", e);
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
            // TODO: transfer metrics name into prometheus format
            for (MetricData metric : metrics) {
                switch (metric.getType()) {
                    case LONG_SUM:
                        metric.getLongSumData().getPoints().forEach(point ->
                            lineList.add(serializeCounter(
                                mapMetricsName(metric.getName(), metric.getUnit(), metric.getLongSumData().isMonotonic(), false),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case DOUBLE_SUM:
                        metric.getDoubleSumData().getPoints().forEach(point ->
                            lineList.add(serializeCounter(
                                mapMetricsName(metric.getName(), metric.getUnit(), metric.getDoubleSumData().isMonotonic(), false),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case LONG_GAUGE:
                        metric.getLongGaugeData().getPoints().forEach(point ->
                            lineList.add(serializeGauge(
                                mapMetricsName(metric.getName(), metric.getUnit(), false, true),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case DOUBLE_GAUGE:
                        metric.getDoubleGaugeData().getPoints().forEach(point ->
                            lineList.add(serializeGauge(
                                mapMetricsName(metric.getName(), metric.getUnit(), false, true),
                                point.getValue(), point.getAttributes(), point.getEpochNanos())));
                        break;
                    case HISTOGRAM:
                        metric.getHistogramData().getPoints().forEach(point ->
                            lineList.add(serializeHistogram(
                                mapMetricsName(metric.getName(), metric.getUnit(), false, false),
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
                    s3Operator.write(getObjectKey(), uploadBuffer.retainedSlice().asReadOnly(), ThrottleStrategy.BYPASS).get();
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
        s3Operator.close();
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

    private String getPrometheusUnit(String unit) {
        if (unit.contains("{")) {
            return "";
        }
        switch (unit) {
            // Time
            case "d":
                return "days";
            case "h":
                return "hours";
            case "min":
                return "minutes";
            case "s":
                return "seconds";
            case "ms":
                return "milliseconds";
            case "us":
                return "microseconds";
            case "ns":
                return "nanoseconds";
            // Bytes
            case "By":
                return "bytes";
            case "KiBy":
                return "kibibytes";
            case "MiBy":
                return "mebibytes";
            case "GiBy":
                return "gibibytes";
            case "TiBy":
                return "tibibytes";
            case "KBy":
                return "kilobytes";
            case "MBy":
                return "megabytes";
            case "GBy":
                return "gigabytes";
            case "TBy":
                return "terabytes";
            // SI
            case "m":
                return "meters";
            case "V":
                return "volts";
            case "A":
                return "amperes";
            case "J":
                return "joules";
            case "W":
                return "watts";
            case "g":
                return "grams";
            // Misc
            case "Cel":
                return "celsius";
            case "Hz":
                return "hertz";
            case "1":
                return "";
            case "%":
                return "percent";
            default:
                return unit;
        }
    }

    private String mapMetricsName(String name, String unit, boolean isCounter, boolean isGauge) {
        // Replace "." into "_"
        name = name.replaceAll("\\.", "_");

        String prometheusUnit = getPrometheusUnit(unit);
        boolean shouldAppendUnit = StringUtils.isNotBlank(prometheusUnit) && !name.contains(prometheusUnit);
        // trim counter's _total suffix so the unit is placed before it.
        if (isCounter && name.endsWith(TOTAL_SUFFIX)) {
            name = name.substring(0, name.length() - TOTAL_SUFFIX.length());
        }
        // append prometheus unit if not null or empty.
        if (shouldAppendUnit) {
            name = name + "_" + prometheusUnit;
        }

        // replace _total suffix, or add if it wasn't already present.
        if (isCounter) {
            name = name + TOTAL_SUFFIX;
        }
        // special case - gauge
        if (unit.equals("1") && isGauge && !name.contains("ratio")) {
            name = name + "_ratio";
        }
        return name;
    }

    private String serializeCounter(String name, double value, Attributes attributes, long timestampNanos) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("kind", "absolute");

        root.put("timestamp", TimeUnit.NANOSECONDS.toSeconds(timestampNanos));
        root.put("name", name);
        root.set("counter", objectMapper.createObjectNode().put("value", value));

        ObjectNode tags = objectMapper.createObjectNode();
        defalutTagMap.forEach(tags::put);
        attributes.forEach((k, v) -> tags.put(k.getKey(), v.toString()));
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
        defalutTagMap.forEach(tags::put);
        attributes.forEach((k, v) -> tags.put(k.getKey(), v.toString()));
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
        defalutTagMap.forEach(tags::put);
        point.getAttributes().forEach((k, v) -> tags.put(k.getKey(), v.toString()));
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
