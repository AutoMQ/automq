package kafka.log.stream.s3.telemetry.exporter;

import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReaderBuilder;

public class KafkaMetricsExporter implements MetricsExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricsExporter.class);
    private final int intervalMs;
    private final KafkaExportURI kafkaExportURI;

    public KafkaMetricsExporter(int intervalMs, String kafkaExportURIStr) {
        if (Utils.isBlank(kafkaExportURIStr)) {
            throw new IllegalArgumentException("Kafka export URI is required");
        }
        this.intervalMs = intervalMs;
        this.kafkaExportURI = KafkaExportURI.parse(kafkaExportURIStr);
        LOGGER.info("KafkaMetricsExporter initialized with kafkaExportURI: {}, intervalMs: {}",
                kafkaExportURI, intervalMs);
    }

    public int getIntervalMs() {
        return intervalMs;
    }

    public KafkaExportURI getKafkaExportURI() {
        return kafkaExportURI;
    }

    @Override
    public MetricReader asMetricReader() {
        KafkaExporter kafkaExporter = new KafkaExporter(kafkaExportURI);
        PeriodicMetricReaderBuilder builder = PeriodicMetricReader.builder(kafkaExporter);
        return builder.setInterval(Duration.ofMillis(intervalMs)).build();
    }
}
