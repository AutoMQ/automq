/*
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
/*
 * Some portion of this file Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.autobalancer.metricsreporter;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import kafka.autobalancer.config.AutoBalancerMetricsReporterConfig;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.BrokerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.MetricsUtils;
import kafka.autobalancer.metricsreporter.metric.RawMetricType;
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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
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
    // KafkaYammerMetrics class in Kafka 3.3+
    private static final String YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER = "org.apache.kafka.server.metrics.KafkaYammerMetrics";
    // KafkaYammerMetrics class in Kafka 2.6+
    private static final String YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER = "kafka.metrics.KafkaYammerMetrics";
    // KafkaYammerMetrics class in Kafka 2.5-
    private static final String YAMMER_METRICS_IN_KAFKA_2_5_AND_EARLIER = "com.yammer.metrics.Metrics";
    private final Map<MetricName, Metric> interestedMetrics = new ConcurrentHashMap<>();
    private final MetricsRegistry metricsRegistry = metricsRegistry();
    private YammerMetricProcessor yammerMetricProcessor;
    private KafkaThread metricsReporterRunner;
    private KafkaProducer<String, AutoBalancerMetrics> producer;
    private String autoBalancerMetricsTopic;
    private long reportingIntervalMs;
    private int brokerId;
    private String brokerRack;
    private long lastReportingTime = System.currentTimeMillis();
    private int numMetricSendFailure = 0;
    private volatile boolean shutdown = false;
    private int metricsReporterCreateRetries;
    private boolean kubernetesMode;
    private double brokerNwInCapacity;
    private double brokerNwOutCapacity;

    static String getBootstrapServers(Map<String, ?> configs) {
        Object port = configs.get("port");
        String listeners = String.valueOf(configs.get(KafkaConfig.ListenersProp()));
        if (!"null".equals(listeners) && listeners.length() != 0) {
            // See https://kafka.apache.org/documentation/#listeners for possible responses. If multiple listeners are configured, this function
            // picks the first listener in the list of listeners. Hence, users of this config must adjust their order accordingly.
            String firstListener = listeners.split("\\s*,\\s*")[0];
            String[] protocolHostPort = firstListener.split(":");
            // Use port of listener only if no explicit config specified for KafkaConfig.PortProp().
            String portToUse = port == null ? protocolHostPort[protocolHostPort.length - 1] : String.valueOf(port);
            // Use host of listener if one is specified.
            return ((protocolHostPort[1].length() == 2) ? DEFAULT_BOOTSTRAP_SERVERS_HOST : protocolHostPort[1].substring(2)) + ":" + portToUse;
        }

        return DEFAULT_BOOTSTRAP_SERVERS_HOST + ":" + (port == null ? DEFAULT_BOOTSTRAP_SERVERS_PORT : port);
    }

    /**
     * Starting with Kafka 3.3.0 a new class, "org.apache.kafka.server.metrics.KafkaYammerMetrics", provides the default Metrics Registry.
     * <p>
     * This is the third default Metrics Registry class change since Kafka 2.5:
     * - Metrics Registry class in Kafka 3.3+: org.apache.kafka.server.metrics.KafkaYammerMetrics
     * - Metrics Registry class in Kafka 2.6+: kafka.metrics.KafkaYammerMetrics
     * - Metrics Registry class in Kafka 2.5-: com.yammer.metrics.Metrics
     * <p>
     * The older default registries do not work with the newer versions of Kafka. Therefore, if the new class exists, we use it and if
     * it doesn't exist we will fall back on the older ones.
     * <p>
     * Once CC supports only 2.6.0 and newer, we can clean this up and use only KafkaYammerMetrics all the time.
     *
     * @return MetricsRegistry with Kafka metrics
     */
    private static MetricsRegistry metricsRegistry() {
        Object metricsRegistry;
        Class<?> metricsClass;

        try {
            // First we try to get the KafkaYammerMetrics class for Kafka 3.3+
            metricsClass = Class.forName(YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);
            LOGGER.info("Found class {} for Kafka 3.3 and newer.", YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);
        } catch (ClassNotFoundException e) {
            LOGGER.info("Class {} not found. We are probably on Kafka 3.2 or older.", YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);

            // We did not find the KafkaYammerMetrics class from Kafka 3.3+. So we are probably on older Kafka version
            //     => we will try the older class for Kafka 2.6+.
            try {
                metricsClass = Class.forName(YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER);
                LOGGER.info("Found class {} for Kafka 2.6 and newer.", YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER);
            } catch (ClassNotFoundException ee) {
                LOGGER.info("Class {} not found. We are probably on Kafka 2.5 or older.", YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER);

                // We did not find the KafkaYammerMetrics class from Kafka 2.6+. So we are probably on older Kafka version
                //     => we will try the older class for Kafka 2.5-.
                try {
                    metricsClass = Class.forName(YAMMER_METRICS_IN_KAFKA_2_5_AND_EARLIER);
                    LOGGER.info("Found class {} for Kafka 2.5 and earlier.", YAMMER_METRICS_IN_KAFKA_2_5_AND_EARLIER);
                } catch (ClassNotFoundException eee) {
                    // No class was found for any Kafka version => we should fail
                    throw new RuntimeException("Failed to find Yammer Metrics class", eee);
                }
            }
        }

        try {
            Method method = metricsClass.getMethod("defaultRegistry");
            metricsRegistry = method.invoke(null);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Failed to get metrics registry", e);
        }

        if (metricsRegistry instanceof MetricsRegistry) {
            return (MetricsRegistry) metricsRegistry;
        } else {
            throw new RuntimeException("Metrics registry does not have the expected type");
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metricsReporterRunner = new KafkaThread("AutoBalancerMetricsReporterRunner", this, true);
        yammerMetricProcessor = new YammerMetricProcessor();
        metricsReporterRunner.start();
        metricsRegistry.addListener(this);
        addMandatoryBrokerMetrics();
        LOGGER.info("AutoBalancerMetricsReporter init successful");
    }

    private void addMandatoryBrokerMetrics() {
        for (String name : MetricsUtils.getMetricNameMaybeMissing()) {
            interestedMetrics.putIfAbsent(MetricsUtils.buildBrokerMetricName(name), MetricsUtils.getEmptyMetricFor(name));
        }
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
    public void configure(Map<String, ?> configs) {
        Properties producerProps = AutoBalancerMetricsReporterConfig.parseProducerConfigs(configs);

        //Add BootstrapServers if not set
        if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
            String bootstrapServers = getBootstrapServers(configs);
            producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            LOGGER.info("Using default value of {} for {}", bootstrapServers,
                    AutoBalancerMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }

        //Add SecurityProtocol if not set
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
        setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
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
        brokerNwInCapacity = reporterConfig.getDouble(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_BROKER_NW_IN_CAPACITY);
        brokerNwOutCapacity = reporterConfig.getDouble(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_BROKER_NW_OUT_CAPACITY);

        autoBalancerMetricsTopic = reporterConfig.getString(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_TOPIC_CONFIG);
        reportingIntervalMs = reporterConfig.getLong(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG);
        kubernetesMode = reporterConfig.getBoolean(AutoBalancerMetricsReporterConfig.AUTO_BALANCER_METRICS_REPORTER_KUBERNETES_MODE_CONFIG);

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
                new ProducerRecord<>(autoBalancerMetricsTopic, null, ccm.time(), ccm.key(), ccm);
        LOGGER.debug("Sending auto balancer metric {}.", ccm);
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e != null) {
                numMetricSendFailure++;
            }
        });
    }

    private void reportMetrics(long now) throws Exception {
        LOGGER.info("Reporting metrics.");

        YammerMetricProcessor.Context context = new YammerMetricProcessor.Context(now, brokerId, brokerRack, reportingIntervalMs);
        processYammerMetrics(context);
        processCpuMetrics(context);
        for (Map.Entry<String, AutoBalancerMetrics> entry : context.getMetricMap().entrySet()) {
            sendAutoBalancerMetric(entry.getValue());
        }

        LOGGER.info("Finished reporting metrics, total metrics size: {}, merged size: {}.", interestedMetrics.size(), context.getMetricMap().size());
    }

    private void processYammerMetrics(YammerMetricProcessor.Context context) throws Exception {
        for (Map.Entry<MetricName, Metric> entry : interestedMetrics.entrySet()) {
            LOGGER.trace("Processing yammer metric {}, scope = {}", entry.getKey(), entry.getKey().getScope());
            entry.getValue().processWith(yammerMetricProcessor, entry.getKey(), context);
        }
        // add broker capacity info
        context.merge(new BrokerMetrics(context.time(), brokerId, brokerRack)
                .put(RawMetricType.BROKER_CAPACITY_NW_IN, brokerNwInCapacity)
                .put(RawMetricType.BROKER_CAPACITY_NW_OUT, brokerNwOutCapacity));
        addMandatoryPartitionMetrics(context);
    }

    private void addMandatoryPartitionMetrics(YammerMetricProcessor.Context context) {
        for (AutoBalancerMetrics metrics : context.getMetricMap().values()) {
            if (metrics.metricClassId() == AutoBalancerMetrics.MetricClassId.PARTITION_METRIC
                    && !MetricsUtils.sanityCheckTopicPartitionMetricsCompleteness(metrics)) {
                metrics.getMetricTypeValueMap().putIfAbsent(RawMetricType.TOPIC_PARTITION_BYTES_IN, 0.0);
                metrics.getMetricTypeValueMap().putIfAbsent(RawMetricType.TOPIC_PARTITION_BYTES_OUT, 0.0);
                metrics.getMetricTypeValueMap().putIfAbsent(RawMetricType.PARTITION_SIZE, 0.0);
            } else if (metrics.metricClassId() == AutoBalancerMetrics.MetricClassId.BROKER_METRIC
                    && !MetricsUtils.sanityCheckBrokerMetricsCompleteness(metrics)) {
                metrics.getMetricTypeValueMap().putIfAbsent(RawMetricType.ALL_TOPIC_BYTES_IN, 0.0);
                metrics.getMetricTypeValueMap().putIfAbsent(RawMetricType.ALL_TOPIC_BYTES_OUT, 0.0);
                metrics.getMetricTypeValueMap().putIfAbsent(RawMetricType.BROKER_CPU_UTIL, 0.0);
            }
        }
    }

    private void processCpuMetrics(YammerMetricProcessor.Context context) {
        BrokerMetrics brokerMetrics = null;
        try {
            brokerMetrics = MetricsUtils.getCpuMetric(context.time(), brokerId, brokerRack, kubernetesMode);
        } catch (Exception e) {
            LOGGER.error("Create cpu metrics failed: {}", e.getMessage());
        }
        if (brokerMetrics == null) {
            brokerMetrics = new BrokerMetrics(context.time(), brokerId, brokerRack);
            brokerMetrics.put(RawMetricType.BROKER_CPU_UTIL, 0.0);
        }
        context.merge(brokerMetrics);
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
