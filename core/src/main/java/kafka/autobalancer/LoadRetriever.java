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

package kafka.autobalancer;

import kafka.autobalancer.common.AutoBalancerThreadFactory;
import kafka.autobalancer.config.AutoBalancerConfig;
import kafka.autobalancer.config.AutoBalancerControllerConfig;
import kafka.autobalancer.listeners.BrokerStatusListener;
import kafka.autobalancer.metricsreporter.metric.AutoBalancerMetrics;
import kafka.autobalancer.metricsreporter.metric.BrokerMetrics;
import kafka.autobalancer.metricsreporter.metric.MetricSerde;
import kafka.autobalancer.metricsreporter.metric.TopicPartitionMetrics;
import kafka.autobalancer.model.ClusterModel;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ControllerRequestContext;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

public class LoadRetriever implements Runnable, BrokerStatusListener {
    public static final Random RANDOM = new Random();
    private final Logger logger;
    private final Map<Integer, BrokerEndpoints> bootstrapServerMap;
    private final String metricReporterTopic;
    private final int metricReporterTopicPartition;
    private final long metricReporterTopicRetentionTime;
    private final String metricReporterTopicCleanupPolicy;
    private final long consumerPollTimeout;
    private final String consumerClientIdPrefix;
    private final String consumerGroupIdPrefix;
    private final ClusterModel clusterModel;
    private final KafkaThread retrieveTask;
    private final Lock lock;
    private final Condition cond;
    private final Controller controller;
    private final ScheduledExecutorService executorService;
    private final Set<Integer> brokerIdsInUse;
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
        this.logger = logContext.logger(LoadRetriever.class);
        this.controller = controller;
        this.clusterModel = clusterModel;
        this.bootstrapServerMap = new HashMap<>();
        this.brokerIdsInUse = new HashSet<>();
        this.lock = new ReentrantLock();
        this.cond = lock.newCondition();
        this.executorService = Executors.newSingleThreadScheduledExecutor(new AutoBalancerThreadFactory("metric-topic-initializer"));
        leaderEpochInitialized = false;
        metricReporterTopic = config.getString(AutoBalancerConfig.AUTO_BALANCER_TOPIC_CONFIG);
        metricReporterTopicPartition = config.getInt(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_NUM_PARTITIONS_CONFIG);
        metricReporterTopicRetentionTime = config.getLong(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_RETENTION_MS_CONFIG);
        metricReporterTopicCleanupPolicy = config.getString(AutoBalancerConfig.AUTO_BALANCER_METRICS_TOPIC_CLEANUP_POLICY);
        consumerPollTimeout = config.getLong(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_POLL_TIMEOUT);
        consumerClientIdPrefix = config.getString(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_CLIENT_ID_PREFIX);
        consumerGroupIdPrefix = config.getString(AutoBalancerControllerConfig.AUTO_BALANCER_CONTROLLER_CONSUMER_GROUP_ID_PREFIX);
        retrieveTask = KafkaThread.daemon("retrieve-load-task", this);
    }

    public void start() {
        this.shutdown = false;
        this.executorService.scheduleAtFixedRate(this::checkAndCreateTopic, 0, 1L, TimeUnit.MINUTES);
        retrieveTask.start();
        logger.info("Started");
    }

    public void shutdown() {
        this.shutdown = true;
        this.executorService.shutdown();
        retrieveTask.interrupt();
        logger.info("Shutdown completed");
    }

    private KafkaConsumer<String, AutoBalancerMetrics> createConsumer(String bootstrapServer) {
        long randomToken = RANDOM.nextLong();
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientIdPrefix + "-consumer-" + randomToken);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupIdPrefix + "-group-" + randomToken);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
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
            if (brokerEndpoints != null) {
                if (!brokerEndpoints.isFenced && !brokerEndpoints.getEndpoints().isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasAvailableBroker() {
        if (this.bootstrapServerMap.isEmpty()) {
            return false;
        }
        for (BrokerEndpoints brokerEndpoints : this.bootstrapServerMap.values()) {
            if (!brokerEndpoints.isFenced() && !brokerEndpoints.getEndpoints().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public String buildBootstrapServer() {
        Set<String> endpoints = new HashSet<>();
        this.brokerIdsInUse.clear();
        for (BrokerEndpoints brokerEndpoints : this.bootstrapServerMap.values()) {
            if (!brokerEndpoints.isFenced() && !brokerEndpoints.getEndpoints().isEmpty()) {
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
            }
        } finally {
            lock.unlock();
        }
        if (this.consumer == null && !bootstrapServer.isEmpty()) {
            checkAndCreateTopic();
            //TODO: fetch metadata from controller
            this.consumer = createConsumer(bootstrapServer);
            this.consumer.subscribe(Collections.singleton(metricReporterTopic));
            logger.info("Created consumer on {}", bootstrapServer);
        }
    }

    private void checkAndCreateTopic() {
        //TODO: check with cache
        if (!leaderEpochInitialized || !isLeader) {
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

    @Override
    public void run() {
        while (!shutdown) {
            checkAndCreateConsumer();
            if (shutdown) {
                break;
            }
            try {
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
            } catch (Exception e) {
                logger.error("Consumer poll error: {}", e.getMessage());
            }
        }
        if (this.consumer != null) {
            try {
                this.consumer.close(Duration.ofMillis(5000));
            } catch (Exception e) {
                logger.error("Exception when close consumer: {}", e.getMessage());
            }
        }
    }

    public void onLeaderChanged(boolean isLeader) {
        this.leaderEpochInitialized = true;
        this.isLeader = isLeader;
    }

    private void updateClusterModel(AutoBalancerMetrics metrics) {
        switch (metrics.metricClassId()) {
            case BROKER_METRIC:
                clusterModel.updateBroker((BrokerMetrics) metrics);
                break;
            case PARTITION_METRIC:
                clusterModel.updateTopicPartition((TopicPartitionMetrics) metrics);
                break;
            default:
                logger.error("Not supported metrics version {}", metrics.metricClassId());
        }
    }
}
