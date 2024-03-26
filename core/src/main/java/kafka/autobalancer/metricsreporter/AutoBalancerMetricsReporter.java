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

package kafka.autobalancer.metricsreporter;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.MetricsUtils;
import kafka.autobalancer.metricsreporter.metric.YammerMetricProcessor;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.
 */
public class AutoBalancerMetricsReporter implements MetricsRegistryListener, MetricsReporter, Runnable {
    public static final String DEFAULT_BOOTSTRAP_SERVERS_HOST = "localhost";
    public static final String DEFAULT_BOOTSTRAP_SERVERS_PORT = "9092";
    protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoBalancerMetricsReporter.class);
    private final Map<MetricName, Metric> interestedMetrics = new ConcurrentHashMap<>();
    private final MetricsRegistry metricsRegistry = KafkaYammerMetrics.defaultRegistry();
    private YammerMetricProcessor yammerMetricProcessor;
    private KafkaThread metricsReporterRunner;
    private KafkaProducer<String, AutoBalancerMetrics> producer;
    private long reportingIntervalMs;
    private int brokerId;
    private String brokerRack;
    private long lastReportingTime = System.currentTimeMillis();
    private int numMetricSendFailure = 0;
    private volatile boolean shutdown = false;
    private int metricsReporterCreateRetries;
    private long lastErrorReportTime = 0;

    private String getBootstrapServers(Map<String, ?> configs) {
        String listeners = String.valueOf(configs.get(KafkaConfig.ListenersProp()));
        if (!"null".equals(listeners) && !listeners.isEmpty()) {
            // See https://kafka.apache.org/documentation/#listeners for possible responses. If multiple listeners are configured, this function
            // picks the first listener in the list of listeners. Hence, users of this config must adjust their order accordingly.
            String firstListener = listeners.split("\\s*,\\s*")[0];
            String[] protocolHostPort = firstListener.split(":");
            String portToUse = protocolHostPort[protocolHostPort.length - 1];
            // Use host of listener if one is specified.
            return ((protocolHostPort[1].length() == 2) ? DEFAULT_BOOTSTRAP_SERVERS_HOST
                    : protocolHostPort[1].substring(2)) + ":" + portToUse;
        }

        return DEFAULT_BOOTSTRAP_SERVERS_HOST + ":" + DEFAULT_BOOTSTRAP_SERVERS_PORT;
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metricsReporterRunner = new KafkaThread("AutoBalancerMetricsReporterRunner", this, true);
        yammerMetricProcessor = new YammerMetricProcessor();
        metricsReporterRunner.start();
        metricsRegistry.addListener(this);
        LOGGER.info("AutoBalancerMetricsReporter init successful");
    }

    /**
     * On new yammer metric added
     *
     * @param name   the name of the {@link Metric}
     * @param metric the {@link Metric}
     */
    @Override
    public synchronized void onMetricAdded(MetricName name, Metric metric) {
        addMetricIfInterested(name, metric);
    }

    /**
     * On yammer metric removed
     *
     * @param name the name of the {@link com.yammer.metrics.core.Metric}
     */
    @Override
    public synchronized void onMetricRemoved(MetricName name) {
        interestedMetrics.remove(name);
    }

    /**
     * On kafka metric changed
     *
     * @param metric {@link KafkaMetric}
     */
    @Override
    public void metricChange(KafkaMetric metric) {
        // do nothing, we only interested in yammer metrics now
    }

    /**
     * On kafka metric removed
     *
     * @param metric {@link KafkaMetric}
     */
    @Override
    public void metricRemoval(KafkaMetric metric) {
        // do nothing, we only interested in yammer metrics now
    }

    @Override
    public void close() {
        LOGGER.info("Closing Auto Balancer metrics reporter, id={}.", brokerId);
        shutdown = true;
        if (metricsReporterRunner != null) {
            metricsReporterRunner.interrupt();
        }
        if (producer != null) {
            producer.close(PRODUCER_CLOSE_TIMEOUT);
        }
    }

    @Override
    public void configure(Map<String, ?> rawConfigs) {

        Map<String, Object> configs = new HashMap<>(rawConfigs);

        Properties producerProps = AutoBalancerMetricsReporterConfig.parseProducerConfigs(configs);

        // Add BootstrapServers if not set
        if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
            String bootstrapServers = getBootstrapServers(configs);
            producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            LOGGER.info("Using default value of {} for {}", bootstrapServers,
                    AutoBalancerMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }

        // Add SecurityProtocol if not set
        if (!producerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
            String securityProtocol = "PLAINTEXT";
            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            LOGGER.info("Using default value of {} for {}", securityProtocol,
                    AutoBalancerMetricsReporterConfig.config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        }

        AutoBalancerMetricsReporterConfig reporterConfig = new AutoBalancerMetricsReporterConfig(configs, false);

        setIfAbsent(producerProps,
                ProducerConfig.CLIENT_ID_CONFIG,
                reporterConfig.getString(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID));
        setIfAbsent(producerProps, ProducerConfig.LINGER_MS_CONFIG,
                reporterConfig.getLong(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_CONFIG).toString());
        setIfAbsent(producerProps, ProducerConfig.BATCH_SIZE_CONFIG,
                reporterConfig.getInt(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_BATCH_SIZE_CONFIG).toString());
        setIfAbsent(producerProps, ProducerConfig.RETRIES_CONFIG, "5");
        setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.toString());
        setIfAbsent(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setIfAbsent(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
        setIfAbsent(producerProps, ProducerConfig.ACKS_CONFIG, "all");

        metricsReporterCreateRetries = reporterConfig.getInt(
                AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES_CONFIG);

        createAutoBalancerMetricsProducer(producerProps);
        if (producer == null) {
            this.close();
        }

        brokerId = Integer.parseInt((String) configs.get(KafkaConfig.BrokerIdProp()));
        brokerRack = (String) configs.get(KafkaConfig.RackProp());
        if (brokerRack == null) {
            brokerRack = "";
        }

        reportingIntervalMs = reporterConfig.getLong(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG);

        LOGGER.info("AutoBalancerMetricsReporter configuration finished");
    }

    protected void createAutoBalancerMetricsProducer(Properties producerProps) throws KafkaException {
        AutoBalancerMetricsUtils.retry(() -> {
            try {
                producer = new KafkaProducer<>(producerProps);
                return false;
            } catch (KafkaException e) {
                if (e.getCause() instanceof ConfigException) {
                    // Check if the config exception is caused by bootstrap.servers config
                    try {
                        ProducerConfig config = new ProducerConfig(producerProps);
                        ClientUtils.parseAndValidateAddresses(
                                config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                                config.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG));
                    } catch (ConfigException ce) {
                        // dns resolution may not be complete yet, let's retry again later
                        LOGGER.warn("Unable to create auto balancer metrics producer. ", ce.getCause());
                    }
                    return true;
                }
                throw e;
            }
        }, metricsReporterCreateRetries);
    }

    @Override
    public void run() {
        LOGGER.info("Starting auto balancer metrics reporter with reporting interval of {} ms.", reportingIntervalMs);

        try {
            while (!shutdown) {
                long now = System.currentTimeMillis();
                LOGGER.debug("Reporting metrics for time {}.", now);
                try {
                    if (now > lastReportingTime + reportingIntervalMs) {
                        numMetricSendFailure = 0;
                        lastReportingTime = now;
                        reportMetrics(now);
                    }
                    try {
                        producer.flush();
                    } catch (InterruptException ie) {
                        if (shutdown) {
                            LOGGER.info("auto balancer metric reporter is interrupted during flush due to shutdown request.");
                        } else {
                            throw ie;
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Got exception in auto balancer metrics reporter", e);
                }
                // Log failures if there is any.
                if (numMetricSendFailure > 0) {
                    LOGGER.warn("Failed to send {} metrics for time {}", numMetricSendFailure, now);
                }
                numMetricSendFailure = 0;
                long nextReportTime = now + reportingIntervalMs;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Reporting finished for time {} in {} ms. Next reporting time {}",
                            now, System.currentTimeMillis() - now, nextReportTime);
                }
                while (!shutdown && now < nextReportTime) {
                    try {
                        Thread.sleep(nextReportTime - now);
                    } catch (InterruptedException ie) {
                        // let it go
                    }
                    now = System.currentTimeMillis();
                }
            }
        } finally {
            LOGGER.info("auto balancer metrics reporter exited.");
        }
    }

    /**
     * Send a AutoBalancerMetric to the Kafka topic.
     *
     * @param ccm the auto balancer metric to send.
     */
    public void sendAutoBalancerMetric(AutoBalancerMetrics ccm) {
        ProducerRecord<String, AutoBalancerMetrics> producerRecord =
                new ProducerRecord<>(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME, null, ccm.time(), ccm.key(), ccm);
        LOGGER.debug("Sending auto balancer metric {}.", ccm);
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e != null) {
                numMetricSendFailure++;
                if (System.currentTimeMillis() - lastErrorReportTime > 10000) {
                    lastErrorReportTime = System.currentTimeMillis();
                    LOGGER.error("Failed to send auto balancer metric", e);
                }
            }
        });
    }

    private void reportMetrics(long now) throws Exception {
        LOGGER.debug("Reporting metrics.");

        YammerMetricProcessor.Context context = new YammerMetricProcessor.Context(now, brokerId, brokerRack, reportingIntervalMs);
        processYammerMetrics(context);
        for (Map.Entry<String, AutoBalancerMetrics> entry : context.getMetricMap().entrySet()) {
            sendAutoBalancerMetric(entry.getValue());
        }

        LOGGER.debug("Finished reporting metrics, total metrics size: {}, merged size: {}.", interestedMetrics.size(), context.getMetricMap().size());
    }

    private void processYammerMetrics(YammerMetricProcessor.Context context) throws Exception {
        for (Map.Entry<MetricName, Metric> entry : interestedMetrics.entrySet()) {
            LOGGER.trace("Processing yammer metric {}, scope = {}", entry.getKey(), entry.getKey().getScope());
            entry.getValue().processWith(yammerMetricProcessor, entry.getKey(), context);
        }
        addMandatoryPartitionMetrics(context);
    }

    private void addMandatoryPartitionMetrics(YammerMetricProcessor.Context context) {
        for (AutoBalancerMetrics metrics : context.getMetricMap().values()) {
            if (metrics.metricType() == MetricTypes.TOPIC_PARTITION_METRIC
                    && !MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics)) {
                metrics.getMetricValueMap().putIfAbsent(RawMetricTypes.TOPIC_PARTITION_BYTES_IN, 0.0);
                metrics.getMetricValueMap().putIfAbsent(RawMetricTypes.TOPIC_PARTITION_BYTES_OUT, 0.0);
                metrics.getMetricValueMap().putIfAbsent(RawMetricTypes.PARTITION_SIZE, 0.0);
            }
        }
    }

    private void addMetricIfInterested(MetricName name, Metric metric) {
        LOGGER.debug("Checking Yammer metric {}", name);
        if (MetricsUtils.isInterested(name)) {
            LOGGER.debug("Added new metric {} to auto balancer metrics reporter.", name);
            interestedMetrics.put(name, metric);
        }
    }

    private void setIfAbsent(Properties props, String key, String value) {
        if (!props.containsKey(key)) {
            props.setProperty(key, value);
        }
    }
}
