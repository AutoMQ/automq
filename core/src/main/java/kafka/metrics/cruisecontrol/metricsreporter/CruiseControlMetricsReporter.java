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


package kafka.metrics.cruisecontrol.metricsreporter;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import kafka.metrics.cruisecontrol.metricsreporter.exception.CruiseControlMetricsReporterException;
import kafka.metrics.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import kafka.metrics.cruisecontrol.metricsreporter.metric.MetricSerde;
import kafka.metrics.cruisecontrol.metricsreporter.metric.MetricsUtils;
import kafka.metrics.cruisecontrol.metricsreporter.metric.TopicMetric;
import kafka.metrics.cruisecontrol.metricsreporter.metric.YammerMetricProcessor;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static kafka.metrics.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static kafka.metrics.cruisecontrol.metricsreporter.CruiseControlMetricsUtils.maybeUpdateConfig;

public class CruiseControlMetricsReporter implements MetricsRegistryListener, MetricsReporter, Runnable {
    public static final String DEFAULT_BOOTSTRAP_SERVERS_HOST = "localhost";
    public static final String DEFAULT_BOOTSTRAP_SERVERS_PORT = "9092";
    protected static final String CRUISE_CONTROL_METRICS_TOPIC_CLEAN_UP_POLICY = "delete";
    protected static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final Logger LOG = LoggerFactory.getLogger(CruiseControlMetricsReporter.class);
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
    private KafkaProducer<String, CruiseControlMetric> producer;
    private String cruiseControlMetricsTopic;
    private long reportingIntervalMs;
    private int brokerId;
    private long lastReportingTime = System.currentTimeMillis();
    private int numMetricSendFailure = 0;
    private volatile boolean shutdown = false;
    private NewTopic metricsTopic;
    private AdminClient adminClient;
    private long metricsTopicAutoCreateTimeoutMs;
    private int metricsTopicAutoCreateRetries;
    private int metricsReporterCreateRetries;
    private boolean kubernetesMode;

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
            LOG.info("Found class {} for Kafka 3.3 and newer.", YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);
        } catch (ClassNotFoundException e) {
            LOG.info("Class {} not found. We are probably on Kafka 3.2 or older.", YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);

            // We did not find the KafkaYammerMetrics class from Kafka 3.3+. So we are probably on older Kafka version
            //     => we will try the older class for Kafka 2.6+.
            try {
                metricsClass = Class.forName(YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER);
                LOG.info("Found class {} for Kafka 2.6 and newer.", YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER);
            } catch (ClassNotFoundException ee) {
                LOG.info("Class {} not found. We are probably on Kafka 2.5 or older.", YAMMER_METRICS_IN_KAFKA_2_6_AND_LATER);

                // We did not find the KafkaYammerMetrics class from Kafka 2.6+. So we are probably on older Kafka version
                //     => we will try the older class for Kafka 2.5-.
                try {
                    metricsClass = Class.forName(YAMMER_METRICS_IN_KAFKA_2_5_AND_EARLIER);
                    LOG.info("Found class {} for Kafka 2.5 and earlier.", YAMMER_METRICS_IN_KAFKA_2_5_AND_EARLIER);
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
        metricsReporterRunner = new KafkaThread("CruiseControlMetricsReporterRunner", this, true);
        yammerMetricProcessor = new YammerMetricProcessor();
        metricsReporterRunner.start();
        metricsRegistry.addListener(this);
        LOG.info("CruiseControlMetricsReporter init successful");
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
        LOG.info("Closing Cruise Control metrics reporter.");
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
        Properties producerProps = CruiseControlMetricsReporterConfig.parseProducerConfigs(configs);

        //Add BootstrapServers if not set
        if (!producerProps.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
            String bootstrapServers = getBootstrapServers(configs);
            producerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            LOG.info("Using default value of {} for {}", bootstrapServers,
                CruiseControlMetricsReporterConfig.config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        }

        //Add SecurityProtocol if not set
        if (!producerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
            String securityProtocol = "PLAINTEXT";
            producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            LOG.info("Using default value of {} for {}", securityProtocol,
                CruiseControlMetricsReporterConfig.config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        }

        CruiseControlMetricsReporterConfig reporterConfig = new CruiseControlMetricsReporterConfig(configs, false);

        setIfAbsent(producerProps,
            ProducerConfig.CLIENT_ID_CONFIG,
            reporterConfig.getString(CruiseControlMetricsReporterConfig.config(CommonClientConfigs.CLIENT_ID_CONFIG)));
        setIfAbsent(producerProps, ProducerConfig.LINGER_MS_CONFIG,
            reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG).toString());
        setIfAbsent(producerProps, ProducerConfig.BATCH_SIZE_CONFIG,
            reporterConfig.getInt(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_CONFIG).toString());
        setIfAbsent(producerProps, ProducerConfig.RETRIES_CONFIG, "5");
        setIfAbsent(producerProps, ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        setIfAbsent(producerProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        setIfAbsent(producerProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
        setIfAbsent(producerProps, ProducerConfig.ACKS_CONFIG, "all");

        metricsReporterCreateRetries = reporterConfig.getInt(
            CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES_CONFIG);

        createCruiseControlMetricsProducer(producerProps);
        if (producer == null) {
            this.close();
        }

        brokerId = Integer.parseInt((String) configs.get(KafkaConfig.BrokerIdProp()));

        cruiseControlMetricsTopic = reporterConfig.getString(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
        reportingIntervalMs = reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG);
        kubernetesMode = reporterConfig.getBoolean(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE_CONFIG);

        if (reporterConfig.getBoolean(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG)) {
            try {
                metricsTopic = createMetricsTopicFromReporterConfig(reporterConfig);
                Properties adminClientConfigs = CruiseControlMetricsUtils.addSslConfigs(producerProps, reporterConfig);
                adminClient = CruiseControlMetricsUtils.createAdminClient(adminClientConfigs);
                metricsTopicAutoCreateTimeoutMs = reporterConfig.getLong(
                    CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_CONFIG);
                metricsTopicAutoCreateRetries = reporterConfig.getInt(
                    CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_CONFIG);
            } catch (CruiseControlMetricsReporterException e) {
                LOG.warn("Cruise Control metrics topic auto creation was disabled", e);
            }
        }
        LOG.info("CruiseControlMetricsReporter configuration finished");
    }

    protected NewTopic createMetricsTopicFromReporterConfig(CruiseControlMetricsReporterConfig reporterConfig)
        throws CruiseControlMetricsReporterException {
        String cruiseControlMetricsTopic =
            reporterConfig.getString(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_CONFIG);
        Integer cruiseControlMetricsTopicNumPartition =
            reporterConfig.getInt(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG);
        Short cruiseControlMetricsTopicReplicaFactor =
            reporterConfig.getShort(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG);
        Short cruiseControlMetricsTopicMinInsyncReplicas =
            reporterConfig.getShort(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_MIN_INSYNC_REPLICAS_CONFIG);

        if (cruiseControlMetricsTopicReplicaFactor <= 0 || cruiseControlMetricsTopicNumPartition <= 0) {
            throw new CruiseControlMetricsReporterException("The topic configuration must explicitly set the replication factor and the num partitions");
        }

        NewTopic newTopic = new NewTopic(cruiseControlMetricsTopic, cruiseControlMetricsTopicNumPartition, cruiseControlMetricsTopicReplicaFactor);

        Map<String, String> config = new HashMap<>();
        config.put(TopicConfig.RETENTION_MS_CONFIG,
            Long.toString(reporterConfig.getLong(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS_CONFIG)));
        config.put(TopicConfig.CLEANUP_POLICY_CONFIG, CRUISE_CONTROL_METRICS_TOPIC_CLEAN_UP_POLICY);
        if (cruiseControlMetricsTopicMinInsyncReplicas > 0) {
            // If the user has set the minISR for the metrics topic we need to check that the replication factor is set to a level that allows the
            // minISR to be met.
            if (cruiseControlMetricsTopicReplicaFactor >= cruiseControlMetricsTopicMinInsyncReplicas) {
                config.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(cruiseControlMetricsTopicMinInsyncReplicas));
            } else {
                throw new CruiseControlMetricsReporterException(String.format(
                    "The configured topic replication factor (%d) must be greater than or equal to the configured topic minimum insync replicas (%d)",
                    cruiseControlMetricsTopicReplicaFactor,
                    cruiseControlMetricsTopicMinInsyncReplicas));
            }
            // If the user does not set the metrics minISR we do not set that config and use the Kafka cluster's default.
        }
        newTopic.configs(config);
        return newTopic;
    }

    protected void createCruiseControlMetricsProducer(Properties producerProps) throws KafkaException {
        CruiseControlMetricsUtils.retry(() -> {
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
                        LOG.warn("Unable to create Cruise Control metrics producer. ", ce.getCause());
                    }
                    return true;
                }
                throw e;
            }
        }, metricsReporterCreateRetries);
    }

    protected void createCruiseControlMetricsTopic() throws TopicExistsException {
        CruiseControlMetricsUtils.retry(() -> {
            try {
                CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(metricsTopic));
                createTopicsResult.values().get(metricsTopic.name()).get(metricsTopicAutoCreateTimeoutMs, TimeUnit.MILLISECONDS);
                LOG.info("Cruise Control metrics topic {} is created.", metricsTopic.name());
                return false;
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    throw (TopicExistsException) e.getCause();
                }
                LOG.warn("Unable to create Cruise Control metrics topic {}.", metricsTopic.name(), e);
                return true;
            }
        }, metricsTopicAutoCreateRetries);
    }

    protected void maybeUpdateCruiseControlMetricsTopic() {
        maybeUpdateTopicConfig();
        maybeIncreaseTopicPartitionCount();
    }

    protected void maybeUpdateTopicConfig() {
        try {
            // Retrieve topic config to check and update.
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, cruiseControlMetricsTopic);
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singleton(topicResource));
            Config topicConfig = describeConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Set<AlterConfigOp> alterConfigOps = new HashSet<>();
            Map<String, String> configsToSet = new HashMap<>();
            configsToSet.put(TopicConfig.RETENTION_MS_CONFIG, metricsTopic.configs().get(TopicConfig.RETENTION_MS_CONFIG));
            configsToSet.put(TopicConfig.CLEANUP_POLICY_CONFIG, metricsTopic.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
            maybeUpdateConfig(alterConfigOps, configsToSet, topicConfig);
            if (!alterConfigOps.isEmpty()) {
                AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource, alterConfigOps));
                alterConfigsResult.values().get(topicResource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.warn("Unable to update config of Cruise Cruise Control metrics topic {}", cruiseControlMetricsTopic, e);
        }
    }

    protected void maybeIncreaseTopicPartitionCount() {
        String cruiseControlMetricsTopic = metricsTopic.name();
        try {
            // Retrieve topic partition count to check and update.
            TopicDescription topicDescription =
                adminClient.describeTopics(Collections.singletonList(cruiseControlMetricsTopic)).values()
                    .get(cruiseControlMetricsTopic).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (topicDescription.partitions().size() < metricsTopic.numPartitions()) {
                adminClient.createPartitions(Collections.singletonMap(cruiseControlMetricsTopic,
                    NewPartitions.increaseTo(metricsTopic.numPartitions())));
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.warn("Partition count increase to {} for topic {} failed{}.", metricsTopic.numPartitions(), cruiseControlMetricsTopic,
                (e.getCause() instanceof ReassignmentInProgressException) ? " due to ongoing reassignment" : "", e);
        }
    }

    @Override
    public void run() {
        LOG.info("Starting Cruise Control metrics reporter with reporting interval of {} ms.", reportingIntervalMs);
        checkAndCreateTopic();

        try {
            while (!shutdown) {
                long now = System.currentTimeMillis();
                LOG.debug("Reporting metrics for time {}.", now);
                try {
                    if (now > lastReportingTime + reportingIntervalMs) {
                        numMetricSendFailure = 0;
                        lastReportingTime = now;
                        reportYammerMetrics(now);
                        reportCpuUtils(now);
                    }
                    try {
                        producer.flush();
                    } catch (InterruptException ie) {
                        if (shutdown) {
                            LOG.info("Cruise Control metric reporter is interrupted during flush due to shutdown request.");
                        } else {
                            throw ie;
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Got exception in Cruise Control metrics reporter", e);
                }
                // Log failures if there is any.
                if (numMetricSendFailure > 0) {
                    LOG.warn("Failed to send {} metrics for time {}", numMetricSendFailure, now);
                }
                numMetricSendFailure = 0;
                long nextReportTime = now + reportingIntervalMs;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reporting finished for time {} in {} ms. Next reporting time {}",
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
            LOG.info("Cruise Control metrics reporter exited.");
        }
    }

    private void checkAndCreateTopic() {
        if (metricsTopic == null || adminClient == null) {
            return;
        }
        try {
            createCruiseControlMetricsTopic();
        } catch (TopicExistsException e) {
            maybeUpdateCruiseControlMetricsTopic();
        } finally {
            CruiseControlMetricsUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    /**
     * Send a CruiseControlMetric to the Kafka topic.
     *
     * @param ccm the Cruise Control metric to send.
     */
    public void sendCruiseControlMetric(CruiseControlMetric ccm) {
        // Use topic name as key if existing so that the same sampler will be able to collect all the information
        // of a topic.
        String key = ccm.metricClassId() == CruiseControlMetric.MetricClassId.TOPIC_METRIC ? ((TopicMetric) ccm).topic()
            : Integer.toString(ccm.brokerId());
        ProducerRecord<String, CruiseControlMetric> producerRecord =
            new ProducerRecord<>(cruiseControlMetricsTopic, null, ccm.time(), key, ccm);
        LOG.debug("Sending Cruise Control metric {}.", ccm);
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e != null) {
                LOG.warn("Failed to send Cruise Control metric {}", ccm);
                numMetricSendFailure++;
            }
        });
    }

    private void reportYammerMetrics(long now) throws Exception {
        LOG.debug("Reporting yammer metrics.");
        YammerMetricProcessor.Context context = new YammerMetricProcessor.Context(this, now, brokerId, reportingIntervalMs);
        for (Map.Entry<com.yammer.metrics.core.MetricName, Metric> entry : interestedMetrics.entrySet()) {
            LOG.trace("Processing yammer metric {}, scope = {}", entry.getKey(), entry.getKey().getScope());
            entry.getValue().processWith(yammerMetricProcessor, entry.getKey(), context);

        }
        LOG.debug("Finished reporting yammer metrics.");
    }

    private void reportCpuUtils(long now) {
        LOG.debug("Reporting CPU util.");
        try {
            sendCruiseControlMetric(MetricsUtils.getCpuMetric(now, brokerId, kubernetesMode));
            LOG.debug("Finished reporting CPU util.");
        } catch (IOException e) {
            LOG.warn("Failed reporting CPU util.", e);
        }
    }

    private void addMetricIfInterested(MetricName name, Metric metric) {
        LOG.debug("Checking Yammer metric {}", name);
        if (MetricsUtils.isInterested(name)) {
            LOG.debug("Added new metric {} to Cruise Control metrics reporter.", name);
            interestedMetrics.put(name, metric);
        }
    }

    private void setIfAbsent(Properties props, String key, String value) {
        if (!props.containsKey(key)) {
            props.setProperty(key, value);
        }
    }
}
