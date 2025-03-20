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

package kafka.autobalancer.metricsreporter;

import kafka.autobalancer.common.Utils;
import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.common.types.MetricVersion;
import kafka.autobalancer.common.types.RawMetricTypes;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.config.StaticAutoBalancerConfig;
import kafka.autobalancer.config.StaticAutoBalancerConfigUtils;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.BrokerMetrics;
import kafka.autobalancer.metricsreporter.metric.Derivator;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.MetricsUtils;
import kafka.autobalancer.metricsreporter.metric.YammerMetricProcessor;

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
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.QuotaConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.automq.stream.s3.metrics.S3StreamMetricsManager;
import com.automq.stream.s3.metrics.stats.StreamOperationStats;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter.
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 */
public class AutoBalancerMetricsReporter implements MetricsRegistryListener, MetricsReporter, Runnable {
    public static final String DEFAULT_BOOTSTRAP_SERVERS_HOST = "localhost";
    public static final String DEFAULT_BOOTSTRAP_SERVERS_PORT = "9092";
    protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoBalancerMetricsReporter.class);
    private final Map<MetricName, Metric> interestedMetrics = new ConcurrentHashMap<>();
    private final MetricsRegistry metricsRegistry = KafkaYammerMetrics.defaultRegistry();
    protected final Derivator appendLatencyAvg = new Derivator();
    protected YammerMetricProcessor yammerMetricProcessor;
    private KafkaThread metricsReporterRunner;
    private KafkaProducer<String, AutoBalancerMetrics> producer;
    private volatile long reportingIntervalMs;
    protected int brokerId;
    protected String brokerRack;
    private long lastReportingTime = System.currentTimeMillis();
    private int numMetricSendFailure = 0;
    private volatile boolean shutdown = false;
    private int metricsReporterCreateRetries;
    private long lastErrorReportTime = 0;

    String getBootstrapServers(Map<String, ?> configs, String expectedListenerName) {
        String listenerStr = String.valueOf(configs.get(SocketServerConfigs.LISTENERS_CONFIG));
        if (!"null".equals(listenerStr) && !listenerStr.isEmpty()) {
            String[] listeners = listenerStr.split("\\s*,\\s*");
            if (listeners.length == 0) {
                throw new ConfigException("No listener found in " + SocketServerConfigs.LISTENERS_CONFIG);
            }
            for (String listener : listeners) {
                String[] protocolHostPort = listener.split(":");
                if (protocolHostPort.length != 3) {
                    throw new ConfigException("Invalid listener format: " + listener);
                }
                String listenerName = protocolHostPort[0];
                if (listenerName.equals("CONTROLLER")) {
                    continue;
                }
                if (expectedListenerName == null || expectedListenerName.isEmpty() || Utils.checkListenerName(listenerName, expectedListenerName)) {
                    String portToUse = protocolHostPort[protocolHostPort.length - 1];
                    // Use host of listener if one is specified.
                    return ((protocolHostPort[1].length() == 2) ? DEFAULT_BOOTSTRAP_SERVERS_HOST
                            : protocolHostPort[1].substring(2)) + ":" + portToUse;
                }
            }
            throw new ConfigException("No listener found for listener name: " + expectedListenerName);
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
    public Set<String> reconfigurableConfigs() {
        return AutoBalancerMetricsReporterConfig.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        try {
            if (configs.containsKey(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG)) {
                long intervalMs = ConfigUtils.getLong(objectConfigs, AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG);
                if (intervalMs <= 0) {
                    throw new ConfigException(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG, intervalMs);
                }
            }
        } catch (Exception e) {
            throw new ConfigException("Reconfiguration validation error " + e.getMessage());
        }

    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        Map<String, Object> objectConfigs = new HashMap<>(configs);
        if (configs.containsKey(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG)) {
            this.reportingIntervalMs = ConfigUtils.getLong(objectConfigs, AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG);
        }
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

        StaticAutoBalancerConfig staticAutoBalancerConfig = new StaticAutoBalancerConfig(configs, false);
        Properties producerProps = AutoBalancerMetricsReporterConfig.parseProducerConfigs(configs);

        // Add BootstrapServers if not set
        if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
            String bootstrapServers = getBootstrapServers(configs, staticAutoBalancerConfig.getString(StaticAutoBalancerConfig.AUTO_BALANCER_CLIENT_LISTENER_NAME_CONFIG));
            producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            LOGGER.info("Using default value of {} for {}", bootstrapServers,
                    AutoBalancerMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }

        AutoBalancerMetricsReporterConfig reporterConfig = new AutoBalancerMetricsReporterConfig(configs, false);

        setIfAbsent(producerProps,
                ProducerConfig.CLIENT_ID_CONFIG,
                QuotaConfigs.INTERNAL_CLIENT_ID_PREFIX + reporterConfig.getString(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID));
        setIfAbsent(producerProps, ProducerConfig.LINGER_MS_CONFIG,
                reporterConfig.getLong(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_CONFIG).toString());
        setIfAbsent(producerProps, ProducerConfig.BATCH_SIZE_CONFIG,
                reporterConfig.getInt(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_BATCH_SIZE_CONFIG).toString());
        setIfAbsent(producerProps, ProducerConfig.RETRIES_CONFIG, "5");
        setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.toString());
        setIfAbsent(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setIfAbsent(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
        setIfAbsent(producerProps, ProducerConfig.ACKS_CONFIG, "all");
        StaticAutoBalancerConfigUtils.addSslConfigs(producerProps, staticAutoBalancerConfig);

        metricsReporterCreateRetries = reporterConfig.getInt(
                AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES_CONFIG);

        createAutoBalancerMetricsProducer(producerProps);
        if (producer == null) {
            this.close();
        }

        brokerId = Integer.parseInt((String) configs.get(KRaftConfigs.NODE_ID_CONFIG));
        brokerRack = (String) configs.get(ServerConfigs.BROKER_RACK_CONFIG);
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

    @SuppressWarnings("NPathComplexity")
    @Override
    public void run() {
        LOGGER.info("Starting auto balancer metrics reporter with reporting interval of {} ms.", reportingIntervalMs);

        try {
            while (!shutdown) {
                long now = System.currentTimeMillis();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Reporting metrics for time {}.", now);
                }
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending auto balancer metric {}.", ccm);
        }
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e != null) {
                numMetricSendFailure++;
                if (System.currentTimeMillis() - lastErrorReportTime > 10000) {
                    lastErrorReportTime = System.currentTimeMillis();
                    LOGGER.warn("Failed to send auto balancer metric", e);
                }
            }
        });
    }

    private void reportMetrics(long now) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reporting metrics.");
        }

        YammerMetricProcessor.Context context = new YammerMetricProcessor.Context(now, brokerId, brokerRack, reportingIntervalMs);
        processMetrics(context);
        addMissingMetrics(context);
        checkMetricCompleteness(context);
        for (Map.Entry<String, AutoBalancerMetrics> entry : context.getMetricMap().entrySet()) {
            sendAutoBalancerMetric(entry.getValue());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Finished reporting metrics, total metrics size: {}, merged size: {}.", interestedMetrics.size(), context.getMetricMap().size());
        }
    }

    protected void checkMetricCompleteness(YammerMetricProcessor.Context context) {
        for (AutoBalancerMetrics metrics : context.getMetricMap().values()) {
            if (metrics.metricType() == MetricTypes.TOPIC_PARTITION_METRIC
                    && !MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics)) {
                throw new IllegalStateException("Missing metrics for topic partition " + metrics.key());
            }
            if (metrics.metricType() == MetricTypes.BROKER_METRIC
                    && !MetricsUtils.sanityCheckBrokerMetricsCompleteness(metrics)) {
                throw new IllegalStateException("Missing metrics for broker " + metrics.key());
            }
        }
    }

    protected void processMetrics(YammerMetricProcessor.Context context) throws Exception {
        processYammerMetrics(context);
        processBrokerMetrics(context);
    }

    protected void processBrokerMetrics(YammerMetricProcessor.Context context) {
        context.merge(new BrokerMetrics(context.time(), brokerId, brokerRack)
                .put(RawMetricTypes.BROKER_METRIC_VERSION, MetricVersion.LATEST_VERSION.value())
                // TODO: fix latency calculation
                .put(RawMetricTypes.BROKER_APPEND_LATENCY_AVG_MS,
                        TimeUnit.NANOSECONDS.toMillis((long) appendLatencyAvg.derive(
                                StreamOperationStats.getInstance().appendStreamLatency.sum(),
                                StreamOperationStats.getInstance().appendStreamLatency.count())))
                .put(RawMetricTypes.BROKER_MAX_PENDING_APPEND_LATENCY_MS,
                        TimeUnit.NANOSECONDS.toMillis(S3StreamMetricsManager.maxPendingStreamAppendLatency()))
                .put(RawMetricTypes.BROKER_MAX_PENDING_FETCH_LATENCY_MS,
                        TimeUnit.NANOSECONDS.toMillis(S3StreamMetricsManager.maxPendingStreamFetchLatency())));
    }

    protected void processYammerMetrics(YammerMetricProcessor.Context context) throws Exception {
        for (Map.Entry<MetricName, Metric> entry : interestedMetrics.entrySet()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Processing yammer metric {}, scope = {}", entry.getKey(), entry.getKey().getScope());
            }
            entry.getValue().processWith(yammerMetricProcessor, entry.getKey(), context);
        }
        Iterator<Map.Entry<String, AutoBalancerMetrics>> iterator = context.getMetricMap().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, AutoBalancerMetrics> entry = iterator.next();
            AutoBalancerMetrics metrics = entry.getValue();
            if (metrics.metricType() == MetricTypes.TOPIC_PARTITION_METRIC
                    && !metrics.getMetricValueMap().containsKey(RawMetricTypes.PARTITION_SIZE)) {
                // remove metrics for closed partition
                iterator.remove();
            }
        }
    }

    protected void addMissingMetrics(YammerMetricProcessor.Context context) {
        for (AutoBalancerMetrics metrics : context.getMetricMap().values()) {
            if (metrics.metricType() == MetricTypes.TOPIC_PARTITION_METRIC) {
                for (byte key : MetricVersion.LATEST_VERSION.requiredPartitionMetrics()) {
                    metrics.getMetricValueMap().putIfAbsent(key, 0.0);
                }
            }
        }
    }

    protected void addMetricIfInterested(MetricName name, Metric metric) {
        if (isInterestedMetric(name)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Added new metric {} to auto balancer metrics reporter.", name);
            }
            interestedMetrics.put(name, metric);
        }
    }

    protected boolean isInterestedMetric(MetricName name) {
        return MetricsUtils.isInterested(name);
    }

    private void setIfAbsent(Properties props, String key, String value) {
        if (!props.containsKey(key)) {
            props.setProperty(key, value);
        }
    }
}
