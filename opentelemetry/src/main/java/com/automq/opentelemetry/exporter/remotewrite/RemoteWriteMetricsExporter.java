package com.automq.opentelemetry.exporter.remotewrite;

import com.automq.opentelemetry.exporter.MetricsExporter;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReaderBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class RemoteWriteMetricsExporter implements MetricsExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteMetricsExporter.class);
    private final long intervalMs;
    private final RemoteWriteURI remoteWriteURI;

    public RemoteWriteMetricsExporter(long intervalMs, String remoteWriteURIStr) {
        if (StringUtils.isBlank(remoteWriteURIStr)) {
            throw new IllegalArgumentException("Remote write URI is required");
        }
        this.intervalMs = intervalMs;
        this.remoteWriteURI = RemoteWriteURI.parse(remoteWriteURIStr);
        LOGGER.info("RemoteWriteMetricsExporter initialized with remoteWriteURI: {}, intervalMs: {}",
            remoteWriteURI, intervalMs);
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public RemoteWriteURI getRemoteWriteURI() {
        return remoteWriteURI;
    }

    @Override
    public MetricReader asMetricReader() {
        RemoteWriteExporter remoteWriteExporter = new RemoteWriteExporter(remoteWriteURI);
        PeriodicMetricReaderBuilder builder = PeriodicMetricReader.builder(remoteWriteExporter);
        return builder.setInterval(Duration.ofMillis(intervalMs)).build();
    }
}
