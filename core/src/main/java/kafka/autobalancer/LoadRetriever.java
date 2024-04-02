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
import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.common.types.MetricTypes;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.TopicPartitionMetrics;
import kafka.autobalancer.model.ClusterModel;
import kafka.autobalancer.services.AbstractResumableService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.ConfigUtils;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;

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

public class LoadRetriever extends AbstractResumableService implements BrokerStatusListener {
    public static final Random RANDOM = new Random();
    private final Map<Integer, BrokerEndpoints> bootstrapServerMap;
    private volatile int metricReporterTopicPartition;
    private final long metricReporterTopicRetentionTime;
    protected volatile long consumerPollTimeout;
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

    public LoadRetriever(AutoBalancerControllerConfig config, Controller controller, ClusterModel clusterModel) {
        this(config, controller, clusterModel, null);
    }

    public LoadRetriever(AutoBalancerControllerConfig config, Controller controller, ClusterModel clusterModel, LogContext logContext) {
        super(logContext);
        this.controller = controller;
        this.clusterModel = clusterModel;
        this.bootstrapServerMap = new HashMap<>();
        this.brokerIdsInUse = new HashSet<>();
        this.lock = new ReentrantLock();
        this.cond = lock.newCondition();
        this.mainExecutorService = Executors.newSingleThreadScheduledExecutor(new AutoBalancerThreadFactory("load-retriever-main"));
        leaderEpochInitialized = false;
        metricReporterTopicPartition = config.getInt(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG);
        metricReporterTopicRetentionTime = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_METRICS_TOPIC_RETENTION_MS_CONFIG);
        consumerPollTimeout = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT);
        consumerClientIdPrefix = config.getString(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX);
        consumerRetryBackOffMs = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_RETRY_BACKOFF_MS);
    }

    @Override
    protected void doStart() {
        // seek to the latest offset if consumer exists
        if (this.consumer != null) {
            this.consumer.seekToEnd(Collections.emptyList());
        }
        scheduleRetrieve(this.epoch.get());
    }

    @Override
    protected void doShutdown() {
        this.mainExecutorService.shutdown();
        try {
            if (!mainExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                this.mainExecutorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {

        }
        shutdownConsumer();
    }

    @Override
    protected void doPause() {

    }

    private void scheduleRetrieve(int epoch) {
        this.mainExecutorService.schedule(() -> retrieve(epoch), 0, TimeUnit.MILLISECONDS);
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

    private void checkAndCreateConsumer(int epoch) {
        String bootstrapServer;
        this.lock.lock();
        try {
            if (!isRunnable(epoch)) {
                return;
            }
            if (!hasAvailableBrokerInUse()) {
                logger.info("No available broker in use, try to close current consumer");
                shutdownConsumer();
                while (isRunnable(epoch) && !hasAvailableBroker()) {
                    try {
                        this.cond.await();
                    } catch (InterruptedException ignored) {

                    }
                }
                if (!isRunnable(epoch)) {
                    return;
                }
            }
            bootstrapServer = buildBootstrapServer();
            if (this.consumer == null && !bootstrapServer.isEmpty()) {
                //TODO: fetch metadata from controller
                this.consumer = createConsumer(bootstrapServer);
                logger.info("Created consumer on {}", bootstrapServer);
            }
        } finally {
            lock.unlock();
        }
    }

    private void shutdownConsumer() {
        this.lock.lock();
        try {
            if (this.consumer != null) {
                try {
                    this.consumer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    logger.error("Exception when close consumer: {}", e.getMessage());
                }
                this.consumer = null;
                this.currentAssignment.clear();
                logger.info("Consumer closed");
            }
        } finally {
            lock.unlock();
        }
    }

    private void createTopic() {
        if (!isValidState()) {
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
                .setValue(TopicConfig.CLEANUP_POLICY_DELETE));

        topicCollection.add(new CreateTopicsRequestData.CreatableTopic()
                .setName(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME)
                .setNumPartitions(metricReporterTopicPartition)
                .setReplicationFactor((short) 1)
                .setConfigs(configCollection));
        request.setTopics(topicCollection);

        try {
            CompletableFuture<CreateTopicsResponseData> future = this.controller.createTopics(
                    new ControllerRequestContext(null, null, OptionalLong.empty()),
                    request,
                    Collections.emptySet());
            CreateTopicsResponseData rsp = future.get();
            CreateTopicsResponseData.CreatableTopicResult result = rsp.topics().find(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME);
            if (result.errorCode() == Errors.NONE.code()) {
                logger.info("Create metrics reporter topic {} succeed", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME);
            } else if (result.errorCode() != Errors.NONE.code() && result.errorCode() != Errors.TOPIC_ALREADY_EXISTS.code()) {
                logger.warn("Create metrics reporter topic {} failed: {}", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME, result.errorMessage());
            }
        } catch (Exception e) {
            logger.error("Create metrics reporter topic {} exception", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME, e);
        }
    }

    private void createTopicPartitions() {
        if (!isValidState()) {
            return;
        }

        if (currentAssignment.size() > metricReporterTopicPartition) {
            logger.info("Current partition number {} exceeds expected {}, skip alter topic partitions",
                    currentAssignment.size(), metricReporterTopicPartition);
            return;
        }

        CreatePartitionsRequestData.CreatePartitionsTopic topic = new CreatePartitionsRequestData.CreatePartitionsTopic()
                .setName(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME)
                .setCount(metricReporterTopicPartition)
                .setAssignments(null);
        try {
            CompletableFuture<List<CreatePartitionsResponseData.CreatePartitionsTopicResult>> future =
                    this.controller.createPartitions(new ControllerRequestContext(null, null, OptionalLong.empty()),
                    List.of(topic), false);
            List<CreatePartitionsResponseData.CreatePartitionsTopicResult> result = future.get();
            for (CreatePartitionsResponseData.CreatePartitionsTopicResult r : result) {
                if (r.errorCode() == Errors.NONE.code()) {
                    logger.info("Create metrics reporter topic {} with {} partitions succeed", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME, metricReporterTopicPartition);
                } else {
                    logger.warn("Create metrics reporter topic {} with {} partitions failed: {}", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME,
                            metricReporterTopicPartition, r.errorMessage());
                }
            }
        } catch (Exception e) {
            logger.error("Create metrics reporter topic {} with {} partitions exception", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME,
                    metricReporterTopicPartition, e);
        }
    }

    private boolean isValidState() {
        if (!leaderEpochInitialized || !isLeader) {
            return false;
        }

        return hasAvailableBroker();
    }

    public void retrieve(int epoch) {
        while (isRunnable(epoch)) {
            try {
                checkAndCreateConsumer(epoch);
                if (!isRunnable(epoch)) {
                    return;
                }
                TopicAction action = refreshAssignment();
                if (action != TopicAction.NONE) {
                    if (action == TopicAction.CREATE) {
                        createTopic();
                    } else {
                        createTopicPartitions();
                    }
                    shutdownConsumer();
                    this.mainExecutorService.schedule(() -> retrieve(epoch), 1, TimeUnit.SECONDS);
                    return;
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
                if (logger.isDebugEnabled()) {
                    logger.debug("Finished consuming {} metrics from {}.", records.count(), Topic.AUTO_BALANCER_METRICS_TOPIC_NAME);
                }
            } catch (InvalidTopicException e) {
                createTopic();
                this.mainExecutorService.schedule(() -> retrieve(epoch), 1, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                logger.error("Consumer poll error", e);
                this.mainExecutorService.schedule(() -> retrieve(epoch), 1, TimeUnit.SECONDS);
                return;
            } catch (Throwable t) {
                logger.error("Consumer poll error and exit retrieve loop", t);
                return;
            }
        }
    }

    private TopicAction refreshAssignment() {
        List<PartitionInfo> partitionInfos = this.consumer.partitionsFor(Topic.AUTO_BALANCER_METRICS_TOPIC_NAME);
        if (partitionInfos.isEmpty()) {
            logger.info("No partitions found for topic {}, try to create topic", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME);
            return TopicAction.CREATE;
        }
        if (partitionInfos.size() != currentAssignment.size()) {
            currentAssignment.clear();
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                currentAssignment.add(topicPartition);
            }
            this.consumer.assign(currentAssignment);
            logger.info("Partition changed for {}, assigned to {} partitions", Topic.AUTO_BALANCER_METRICS_TOPIC_NAME, currentAssignment.size());
        }
        if (partitionInfos.size() < metricReporterTopicPartition) {
            logger.info("Partition num {} less than expected {}, try to alter partition number", partitionInfos.size(), metricReporterTopicPartition);
            return TopicAction.ALTER;
        }
        return TopicAction.NONE;
    }

    public void onLeaderChanged(boolean isLeader) {
        this.leaderEpochInitialized = true;
        this.isLeader = isLeader;
    }

    protected void updateClusterModel(AutoBalancerMetrics metrics) {
        switch (metrics.metricType()) {
            case MetricTypes.TOPIC_PARTITION_METRIC:
                TopicPartitionMetrics partitionMetrics = (TopicPartitionMetrics) metrics;
                clusterModel.updateBrokerMetrics(partitionMetrics.brokerId(), new HashMap<>(), partitionMetrics.time());
                clusterModel.updateTopicPartitionMetrics(partitionMetrics.brokerId(),
                        new TopicPartition(partitionMetrics.topic(), partitionMetrics.partition()),
                        partitionMetrics.getMetricValueMap(), partitionMetrics.time());
                break;
            default:
                logger.error("Not supported metrics version {}", metrics.metricType());
        }
    }

    private enum TopicAction {
        NONE,
        CREATE,
        ALTER
    }
}
