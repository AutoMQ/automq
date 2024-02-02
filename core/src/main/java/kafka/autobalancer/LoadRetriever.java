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

package kafka.autobalancer;

import com.automq.stream.utils.LogContext;
import kafka.autobalancer.common.AutoBalancerConstants;
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.config.AutoBalancerConfig;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.TopicPartitionMetrics;
import kafka.autobalancer.model.ClusterModel;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoadRetriever implements BrokerStatusListener {
    public static final Random RANDOM = new Random();
    protected final Logger logger;
    private final Map<Integer, BrokerEndpoints> bootstrapServerMap;
    private final String metricReporterTopic;
    private final int metricReporterTopicPartition;
    private final long metricReporterTopicRetentionTime;
    private final String metricReporterTopicCleanupPolicy;
    protected final long consumerPollTimeout;
    protected final String consumerClientIdPrefix;
    protected final long consumerRetryBackOffMs;
    protected final ClusterModel clusterModel;
    private final Lock lock;
    private final Condition cond;
    private final Controller controller;
    private final ScheduledExecutorService mainExecutorService;
    private final Set<Integer> brokerIdsInUse;
    private final Set<TopicPartition> currentAssignment = new HashSet<>();
    private volatile boolean leaderEpochInitialized;
    private volatile boolean isLeader;
    private volatile Consumer<String, AutoBalancerMetrics> consumer;
    private volatile boolean shutdown;

    public LoadRetriever(AutoBalancerControllerConfig config, Controller controller, ClusterModel clusterModel) {
        this(config, controller, clusterModel, null);
    }

    public LoadRetriever(AutoBalancerControllerConfig config, Controller controller, ClusterModel clusterModel, LogContext logContext) {
        if (logContext == null) {
            logContext = new LogContext("[LoadRetriever] ");
        }
        this.logger = logContext.logger(AutoBalancerConstants.AUTO_BALANCER_LOGGER_CLAZZ);
        this.controller = controller;
        this.clusterModel = clusterModel;
        this.bootstrapServerMap = new HashMap<>();
        this.brokerIdsInUse = new HashSet<>();
        this.lock = new ReentrantLock();
        this.cond = lock.newCondition();
        this.mainExecutorService = Executors.newSingleThreadScheduledExecutor(new AutoBalancerThreadFactory("load-retriever-main"));
        leaderEpochInitialized = false;
        metricReporterTopic = config.getString(AutoBalancerConfig.AUTO_BALANCER_TOPIC_CONFIG);
        metricReporterTopicPartition = config.getInt(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG);
        metricReporterTopicRetentionTime = config.getLong(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS_CONFIG);
        metricReporterTopicCleanupPolicy = config.getString(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY);
        consumerPollTimeout = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT);
        consumerClientIdPrefix = config.getString(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX);
        consumerRetryBackOffMs = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS);
    }

    public void start() {
        this.shutdown = false;
        this.mainExecutorService.schedule(this::retrieve, 0, TimeUnit.MILLISECONDS);
        logger.info("Started");
    }

    public void shutdown() {
        this.shutdown = true;
        this.mainExecutorService.shutdown();
        try {
            if (!mainExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                this.mainExecutorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {

        }

        if (this.consumer != null) {
            try {
                this.consumer.close(Duration.ofMillis(5000));
            } catch (Exception e) {
                logger.error("Exception when close consumer: {}", e.getMessage());
            }
        }
        logger.info("Shutdown completed");
    }

    protected KafkaConsumer<String, AutoBalancerMetrics> createConsumer(String bootstrapServer) {
        long randomToken = RANDOM.nextLong();
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientIdPrefix + "-consumer-" + randomToken);
        consumerProps.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, Long.toString(consumerRetryBackOffMs));
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricSerde.class.getName());
        return new KafkaConsumer<>(consumerProps);
    }

    static class BrokerEndpoints {
        private final int brokerId;
        private Set<String> endpoints = new HashSet<>();

        private boolean isFenced;

        public BrokerEndpoints(int brokerId) {
            this.brokerId = brokerId;
        }

        public int brokerId() {
            return this.brokerId;
        }

        public Set<String> getEndpoints() {
            return this.endpoints;
        }

        public void setEndpoints(Set<String> endpoints) {
            this.endpoints = new HashSet<>(endpoints);
        }

        public BrokerEndpoints setFenced(boolean isFenced) {
            this.isFenced = isFenced;
            return this;
        }

        public boolean isFenced() {
            return this.isFenced;
        }

        public boolean isValid() {
            return !this.isFenced && !this.endpoints.isEmpty();
        }

    }

    @Override
    public void onBrokerRegister(RegisterBrokerRecord record) {
        lock.lock();
        try {
            boolean isFenced = record.fenced() || record.inControlledShutdown();
            Set<String> endpoints = new HashSet<>();
            for (RegisterBrokerRecord.BrokerEndpoint endpoint : record.endPoints()) {
                String url = endpoint.host() + ":" + endpoint.port();
                endpoints.add(url);
            }
            BrokerEndpoints brokerEndpoints = new BrokerEndpoints(record.brokerId());
            brokerEndpoints.setFenced(isFenced);
            brokerEndpoints.setEndpoints(endpoints);
            this.bootstrapServerMap.put(record.brokerId(), brokerEndpoints);
            cond.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onBrokerUnregister(UnregisterBrokerRecord record) {
        lock.lock();
        try {
            this.bootstrapServerMap.remove(record.brokerId());
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void onBrokerRegistrationChanged(BrokerRegistrationChangeRecord record) {
        boolean isFenced = record.fenced() == BrokerRegistrationFencingChange.FENCE.value()
                || record.inControlledShutdown() == BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN.value();
        lock.lock();
        try {
            BrokerEndpoints brokerEndpoints = this.bootstrapServerMap.get(record.brokerId());
            if (brokerEndpoints != null) {
                brokerEndpoints.setFenced(isFenced);
            }
            cond.signal();
        } finally {
            lock.unlock();
        }
    }

    private boolean hasAvailableBrokerInUse() {
        if (brokerIdsInUse.isEmpty()) {
            return false;
        }
        for (int brokerId : brokerIdsInUse) {
            BrokerEndpoints brokerEndpoints = this.bootstrapServerMap.get(brokerId);
            if (brokerEndpoints != null && brokerEndpoints.isValid()) {
                return true;
            }
        }
        return false;
    }

    private boolean hasAvailableBroker() {
        if (this.bootstrapServerMap.isEmpty()) {
            return false;
        }
        for (BrokerEndpoints brokerEndpoints : this.bootstrapServerMap.values()) {
            if (brokerEndpoints.isValid()) {
                return true;
            }
        }
        return false;
    }

    public String buildBootstrapServer() {
        Set<String> endpoints = new HashSet<>();
        this.brokerIdsInUse.clear();
        for (BrokerEndpoints brokerEndpoints : this.bootstrapServerMap.values()) {
            if (brokerEndpoints.isValid()) {
                endpoints.add(brokerEndpoints.getEndpoints().iterator().next());
                this.brokerIdsInUse.add(brokerEndpoints.brokerId());
            }
        }
        return String.join(",", endpoints);
    }

    private void checkAndCreateConsumer() {
        String bootstrapServer = "";
        this.lock.lock();
        try {
            if (!hasAvailableBrokerInUse()) {
                if (this.consumer != null) {
                    logger.info("No available broker in use, try to close current consumer");
                    this.consumer.close(Duration.ofSeconds(5));
                    this.consumer = null;
                    this.currentAssignment.clear();
                    logger.info("Consumer closed");
                }
                while (!shutdown && !hasAvailableBroker()) {
                    try {
                        this.cond.await();
                    } catch (InterruptedException ignored) {

                    }
                }
                if (this.shutdown) {
                    return;
                }
                bootstrapServer = buildBootstrapServer();
                logger.info("Available brokers changed, new bootstrap server {}", bootstrapServer);
            }
        } finally {
            lock.unlock();
        }
        if (this.consumer == null && !bootstrapServer.isEmpty()) {
            checkAndCreateTopic();
            //TODO: fetch metadata from controller
            this.consumer = createConsumer(bootstrapServer);
            logger.info("Created consumer on {}", bootstrapServer);
        }
    }

    private void checkAndCreateTopic() {
        //TODO: check with cache
        if (!leaderEpochInitialized || !isLeader) {
            return;
        }

        if (!hasAvailableBroker()) {
            return;
        }

        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreateTopicsRequestData.CreatableTopicCollection topicCollection = new CreateTopicsRequestData.CreatableTopicCollection();
        CreateTopicsRequestData.CreateableTopicConfigCollection configCollection = new CreateTopicsRequestData.CreateableTopicConfigCollection();
        configCollection.add(new CreateTopicsRequestData.CreateableTopicConfig()
                .setName(TopicConfig.RETENTION_MS_CONFIG)
                .setValue(Long.toString(metricReporterTopicRetentionTime)));
        configCollection.add(new CreateTopicsRequestData.CreateableTopicConfig()
                .setName(TopicConfig.CLEANUP_POLICY_CONFIG)
                .setValue(metricReporterTopicCleanupPolicy));

        topicCollection.add(new CreateTopicsRequestData.CreatableTopic()
                .setName(metricReporterTopic)
                .setNumPartitions(metricReporterTopicPartition)
                .setReplicationFactor((short) 1)
                .setConfigs(configCollection));
        request.setTopics(topicCollection);
        CompletableFuture<CreateTopicsResponseData> future = this.controller.createTopics(
                new ControllerRequestContext(null, null, OptionalLong.empty()),
                request,
                Collections.emptySet());
        try {
            CreateTopicsResponseData rsp = future.get();
            CreateTopicsResponseData.CreatableTopicResult result = rsp.topics().find(metricReporterTopic);
            if (result.errorCode() == Errors.NONE.code()) {
                logger.info("Create metrics reporter topic {} succeed", metricReporterTopic);
            }
            if (result.errorCode() != Errors.NONE.code() && result.errorCode() != Errors.TOPIC_ALREADY_EXISTS.code()) {
                logger.warn("Create metrics reporter topic {} failed: {}", metricReporterTopic, result.errorMessage());
            }
        } catch (Exception e) {
            logger.error("Create metrics reporter topic {} exception: {}", metricReporterTopic, e.getMessage());
        }
    }

    public void retrieve() {
        while (!shutdown) {
            checkAndCreateConsumer();
            if (shutdown) {
                break;
            }
            try {
                if (!refreshAssignment()) {
                    checkAndCreateTopic();
                    this.mainExecutorService.schedule(this::retrieve, 1, TimeUnit.SECONDS);
                    break;
                }
                ConsumerRecords<String, AutoBalancerMetrics> records = this.consumer.poll(Duration.ofMillis(consumerPollTimeout));
                for (ConsumerRecord<String, AutoBalancerMetrics> record : records) {
                    if (record == null) {
                        // This means we cannot parse the metrics. It might happen when a newer type of metrics has been added and
                        // the current code is still old. We simply ignore that metric in this case.
                        logger.warn("Cannot parse record, maybe controller version is outdated.");
                        continue;
                    }
                    updateClusterModel(record.value());
                }
                logger.debug("Finished consuming {} metrics from {}.", records.count(), metricReporterTopic);
            } catch (InvalidTopicException e) {
                checkAndCreateTopic();
            } catch (Exception e) {
                logger.error("Consumer poll error: {}", e.getMessage());
            }
        }
    }

    private boolean refreshAssignment() {
        List<PartitionInfo> partitionInfos = this.consumer.partitionsFor(metricReporterTopic);
        if (partitionInfos.isEmpty()) {
            logger.info("No partitions found for topic {}", metricReporterTopic);
            return false;
        }
        if (partitionInfos.size() != currentAssignment.size()) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                currentAssignment.add(topicPartition);
            }
            this.consumer.assign(currentAssignment);
        }
        return true;
    }

    public void onLeaderChanged(boolean isLeader) {
        this.leaderEpochInitialized = true;
        this.isLeader = isLeader;
    }

    protected void updateClusterModel(AutoBalancerMetrics metrics) {
        switch (metrics.metricType()) {
            case MetricTypes.TOPIC_PARTITION_METRIC:
                TopicPartitionMetrics partitionMetrics = (TopicPartitionMetrics) metrics;
                clusterModel.updateTopicPartitionMetrics(partitionMetrics.brokerId(),
                        new TopicPartition(partitionMetrics.topic(), partitionMetrics.partition()),
                        partitionMetrics.getMetricValueMap(), partitionMetrics.time());
                clusterModel.updateBrokerMetrics(partitionMetrics.brokerId(), new HashMap<>(), partitionMetrics.time());
                break;
            default:
                logger.error("Not supported metrics version {}", metrics.metricType());
        }
    }
}
