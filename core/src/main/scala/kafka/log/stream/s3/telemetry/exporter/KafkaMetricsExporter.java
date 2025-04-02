package kafka.log.stream.s3.telemetry.exporter;

import org.apache.kafka.common.utils.Utils;

import com.automq.stream.utils.URIUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReaderBuilder;

public class KafkaMetricsExporter implements MetricsExporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMetricsExporter.class);
    private static final String METRICS_COLLECTION_PERIOD = "collectionPeriod";
    private int intervalSeconds = 60;
    private final KafkaExportURI kafkaExportURI;

    public KafkaMetricsExporter(String kafkaExportURIStr) {
        if (Utils.isBlank(kafkaExportURIStr)) {
            throw new IllegalArgumentException("Kafka export URI is required");
        }
        try {
            URI uri = new URI(kafkaExportURIStr);
            Map<String, List<String>> queries = URIUtils.splitQuery(uri);
            // default to 60 seconds
            this.intervalSeconds = Integer.parseInt(URIUtils.getString(queries, METRICS_COLLECTION_PERIOD, "60"));
        } catch (Exception e) {
            LOGGER.error("Invalid Kafka export URI: {}", kafkaExportURIStr, e);
            throw new IllegalArgumentException("Invalid Kafka export URI: " + kafkaExportURIStr, e);
        }

        this.kafkaExportURI = KafkaExportURI.parse(kafkaExportURIStr);
        LOGGER.info("KafkaMetricsExporter initialized with kafkaExportURI: {}, intervalMs: {}",
                kafkaExportURI, intervalSeconds);
    }

    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    public KafkaExportURI getKafkaExportURI() {
        return kafkaExportURI;
    }

    @Override
    public MetricReader asMetricReader() {
        KafkaExporter kafkaExporter = new KafkaExporter(kafkaExportURI);
        PeriodicMetricReaderBuilder builder = PeriodicMetricReader.builder(kafkaExporter);
        return builder.setInterval(Duration.ofSeconds(intervalSeconds)).build();
    }
}
