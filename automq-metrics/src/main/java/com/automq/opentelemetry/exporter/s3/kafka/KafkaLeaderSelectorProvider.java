package com.automq.opentelemetry.exporter.s3.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.automq.opentelemetry.exporter.s3.UploaderNodeSelector;
import com.automq.opentelemetry.exporter.s3.UploaderNodeSelectorProvider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A UploaderNodeSelectorProvider implementation that relies on Kafka consumer group membership for
 * leader election. When using a topic with a single partition, only one consumer in the group will
 * receive the assignment, which becomes the primary uploader. Other consumers act as standbys.
 */
public class KafkaLeaderSelectorProvider implements UploaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLeaderSelectorProvider.class);

    public static final String TYPE = "kafka";

    private static final String DEFAULT_TOPIC_PREFIX = "__automq_telemetry_s3_leader_";
    private static final String DEFAULT_GROUP_PREFIX = "automq-telemetry-s3-";
    private static final String DEFAULT_CLIENT_PREFIX = "automq-telemetry-s3";

    private static final long DEFAULT_TOPIC_RETENTION_MS = TimeUnit.MINUTES.toMillis(30);
    private static final int DEFAULT_POLL_INTERVAL_MS = 1000;
    private static final long DEFAULT_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(5);
    private static final int DEFAULT_SESSION_TIMEOUT_MS = 10000;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;

    private static final Set<String> RESERVED_KEYS;

    static {
        Set<String> keys = new HashSet<>();
        Collections.addAll(keys,
            "bootstrap.servers",
            "topic",
            "group.id",
            "client.id",
            "auto.create.topic",
            "topic.partitions",
            "topic.replication.factor",
            "topic.retention.ms",
            "poll.interval.ms",
            "retry.backoff.ms",
            "session.timeout.ms",
            "heartbeat.interval.ms",
            "request.timeout.ms"
        );
        RESERVED_KEYS = Collections.unmodifiableSet(keys);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public UploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) throws Exception {
        KafkaLeaderElectionConfig electionConfig = KafkaLeaderElectionConfig.from(clusterId, nodeId, config);
        KafkaLeaderSelector selector = new KafkaLeaderSelector(electionConfig);
        selector.start();
        return selector;
    }

    private static final class KafkaLeaderSelector implements UploaderNodeSelector {
        private final KafkaLeaderElectionConfig config;
        private final AtomicBoolean isLeader = new AtomicBoolean(false);
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile KafkaConsumer<byte[], byte[]> consumer;

        KafkaLeaderSelector(KafkaLeaderElectionConfig config) {
            this.config = config;
        }

        void start() {
            Thread thread = new Thread(this::runElectionLoop,
                String.format("s3-metrics-kafka-selector-%s-%d", config.clusterId, config.nodeId));
            thread.setDaemon(true);
            thread.start();
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown,
                String.format("s3-metrics-kafka-selector-shutdown-%s-%d", config.clusterId, config.nodeId)));
        }

        private void runElectionLoop() {
            while (running.get()) {
                try {
                    ensureTopicExists();
                    runConsumerLoop();
                } catch (WakeupException e) {
                    if (!running.get()) {
                        break;
                    }
                    LOGGER.warn("Kafka leader selector interrupted unexpectedly for cluster {} node {}",
                        config.clusterId, config.nodeId, e);
                    sleep(config.retryBackoffMs);
                } catch (Exception e) {
                    if (!running.get()) {
                        break;
                    }
                    LOGGER.warn("Kafka leader selector loop failed for cluster {} node {}: {}",
                        config.clusterId, config.nodeId, e.getMessage(), e);
                    sleep(config.retryBackoffMs);
                }
            }
        }

        private void runConsumerLoop() {
            Properties consumerProps = config.buildConsumerProperties();
            try (KafkaConsumer<byte[], byte[]> kafkaConsumer =
                     new KafkaConsumer<>(consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                this.consumer = kafkaConsumer;
                ConsumerRebalanceListener rebalanceListener = new LeaderElectionRebalanceListener();
                kafkaConsumer.subscribe(Collections.singletonList(config.topic), rebalanceListener);
                LOGGER.info("Kafka selector subscribed to topic {} with group {}", config.topic, config.groupId);
                while (running.get()) {
                    kafkaConsumer.poll(Duration.ofMillis(config.pollIntervalMs));
                }
            } finally {
                this.consumer = null;
                demote();
            }
        }

        private void ensureTopicExists() throws Exception {
            if (!config.autoCreateTopic) {
                return;
            }
            Properties adminProps = config.buildAdminProperties();
            try (Admin admin = Admin.create(adminProps)) {
                NewTopic newTopic = new NewTopic(config.topic, config.topicPartitions, config.topicReplicationFactor);
                Map<String, String> topicConfig = new HashMap<>();
                if (config.topicRetentionMs > 0) {
                    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.topicRetentionMs));
                }
                if (!topicConfig.isEmpty()) {
                    newTopic.configs(topicConfig);
                }
                CreateTopicsOptions options = new CreateTopicsOptions().validateOnly(false);
                admin.createTopics(Collections.singleton(newTopic), options).all().get();
                LOGGER.info("Kafka selector created leader topic {} with partitions={} replicationFactor={}",
                    config.topic, config.topicPartitions, config.topicReplicationFactor);
            } catch (TopicExistsException ignored) {
                // Topic already exists - expected on subsequent runs
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                Throwable cause = e.getCause();
                if (!(cause instanceof TopicExistsException)) {
                    throw e;
                }
            }
        }

        @Override
        public boolean isPrimaryUploader() {
            return isLeader.get();
        }

        private void demote() {
            if (isLeader.getAndSet(false)) {
                LOGGER.info("Kafka selector demoted node {} for cluster {}", config.nodeId, config.clusterId);
            }
        }

        private void promote() {
            if (isLeader.compareAndSet(false, true)) {
                LOGGER.info("Kafka selector elected node {} as primary uploader for cluster {}", config.nodeId, config.clusterId);
            }
        }

        private void shutdown() {
            if (running.compareAndSet(true, false)) {
                KafkaConsumer<byte[], byte[]> localConsumer = consumer;
                if (localConsumer != null) {
                    localConsumer.wakeup();
                }
            }
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private class LeaderElectionRebalanceListener implements ConsumerRebalanceListener {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    LOGGER.info("Kafka selector lost leadership on partitions {}", partitions);
                }
                demote();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    promote();
                }
            }
        }
    }

    private static final class KafkaLeaderElectionConfig {
        private final String clusterId;
        private final int nodeId;
        private final String bootstrapServers;
        private final String topic;
        private final String groupId;
        private final String clientId;
        private final boolean autoCreateTopic;
        private final int topicPartitions;
        private final short topicReplicationFactor;
        private final long topicRetentionMs;
        private final int pollIntervalMs;
        private final long retryBackoffMs;
        private final int sessionTimeoutMs;
        private final int heartbeatIntervalMs;
        private final int requestTimeoutMs;
        private final Properties clientOverrides;

        static KafkaLeaderElectionConfig from(String clusterId, int nodeId, Map<String, String> config) {
            Map<String, String> effectiveConfig = config == null ? Collections.emptyMap() : config;

            String bootstrapServers = findBootstrapServers(effectiveConfig);
            if (StringUtils.isBlank(bootstrapServers)) {
                throw new IllegalArgumentException("Kafka selector requires 'bootstrap.servers' configuration");
            }

            String normalizedClusterId = StringUtils.isBlank(clusterId) ? "default" : clusterId;
            String topic = effectiveConfig.getOrDefault("topic", DEFAULT_TOPIC_PREFIX + normalizedClusterId);
            String groupId = effectiveConfig.getOrDefault("group.id", DEFAULT_GROUP_PREFIX + normalizedClusterId);
            String clientId = effectiveConfig.getOrDefault("client.id",
                DEFAULT_CLIENT_PREFIX + "-" + normalizedClusterId + "-" + nodeId);

            boolean autoCreateTopic = Boolean.parseBoolean(effectiveConfig.getOrDefault("auto.create.topic", "true"));
            int partitions = parseInt(effectiveConfig.get("topic.partitions"), 1, 1);
            short replicationFactor = (short) parseInt(effectiveConfig.get("topic.replication.factor"), 1, 1);
            long retentionMs = parseLong(effectiveConfig.get("topic.retention.ms"), DEFAULT_TOPIC_RETENTION_MS);

            int pollIntervalMs = parseInt(effectiveConfig.get("poll.interval.ms"), DEFAULT_POLL_INTERVAL_MS, 100);
            long retryBackoffMs = parseLong(effectiveConfig.get("retry.backoff.ms"), DEFAULT_RETRY_BACKOFF_MS);
            int sessionTimeoutMs = parseInt(effectiveConfig.get("session.timeout.ms"), DEFAULT_SESSION_TIMEOUT_MS, 1000);
            int heartbeatIntervalMs = parseInt(effectiveConfig.get("heartbeat.interval.ms"), DEFAULT_HEARTBEAT_INTERVAL_MS, 500);
            int requestTimeoutMs = parseInt(effectiveConfig.get("request.timeout.ms"), 15000, 1000);

            Properties overrides = extractClientOverrides(effectiveConfig);

            return builder()
                .clusterId(clusterId)
                .nodeId(nodeId)
                .bootstrapServers(bootstrapServers)
                .topic(topic)
                .groupId(groupId)
                .clientId(clientId)
                .autoCreateTopic(autoCreateTopic)
                .topicPartitions(partitions)
                .topicReplicationFactor(replicationFactor)
                .topicRetentionMs(retentionMs)
                .pollIntervalMs(pollIntervalMs)
                .retryBackoffMs(retryBackoffMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .heartbeatIntervalMs(heartbeatIntervalMs)
                .requestTimeoutMs(requestTimeoutMs)
                .clientOverrides(overrides)
                .build();
        }

        private KafkaLeaderElectionConfig(Builder builder) {
            this.clusterId = builder.clusterId;
            this.nodeId = builder.nodeId;
            this.bootstrapServers = builder.bootstrapServers;
            this.topic = builder.topic;
            this.groupId = builder.groupId;
            this.clientId = builder.clientId;
            this.autoCreateTopic = builder.autoCreateTopic;
            this.topicPartitions = builder.topicPartitions;
            this.topicReplicationFactor = builder.topicReplicationFactor;
            this.topicRetentionMs = builder.topicRetentionMs;
            this.pollIntervalMs = builder.pollIntervalMs;
            this.retryBackoffMs = builder.retryBackoffMs;
            this.sessionTimeoutMs = builder.sessionTimeoutMs;
            this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
            this.requestTimeoutMs = builder.requestTimeoutMs;
            this.clientOverrides = builder.clientOverrides;
        }

        Properties buildConsumerProperties() {
            Properties props = new Properties();
            props.putAll(clientOverrides);
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId + "-consumer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT));
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
            props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, String.valueOf(TimeUnit.SECONDS.toMillis(10)));
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Math.max(pollIntervalMs * 3, 3000));
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
            props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
            props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            return props;
        }

        Properties buildAdminProperties() {
            Properties props = new Properties();
            props.putAll(clientOverrides);
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId + "-admin");
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
            return props;
        }

        private static Properties extractClientOverrides(Map<String, String> config) {
            Properties props = new Properties();
            for (Map.Entry<String, String> entry : config.entrySet()) {
                String key = entry.getKey();
                if (RESERVED_KEYS.contains(key)) {
                    continue;
                }
                props.put(key, entry.getValue());
            }
            return props;
        }

        private static String findBootstrapServers(Map<String, String> config) {
            String directValue = config.get("bootstrap.servers");
            if (StringUtils.isNotBlank(directValue)) {
                return directValue;
            }
            return config.get("bootstrapServers");
        }

        private static int parseInt(String value, int defaultValue, int minimum) {
            if (StringUtils.isBlank(value)) {
                return defaultValue;
            }
            try {
                int parsed = Integer.parseInt(value.trim());
                return Math.max(parsed, minimum);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }

        private static long parseLong(String value, long defaultValue) {
            if (StringUtils.isBlank(value)) {
                return defaultValue;
            }
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }

        private static Builder builder() {
            return new Builder();
        }

        private static final class Builder {
            private String clusterId;
            private int nodeId;
            private String bootstrapServers;
            private String topic;
            private String groupId;
            private String clientId;
            private boolean autoCreateTopic;
            private int topicPartitions;
            private short topicReplicationFactor;
            private long topicRetentionMs;
            private int pollIntervalMs;
            private long retryBackoffMs;
            private int sessionTimeoutMs;
            private int heartbeatIntervalMs;
            private int requestTimeoutMs;
            private Properties clientOverrides;

            Builder clusterId(String value) {
                this.clusterId = value;
                return this;
            }

            Builder nodeId(int value) {
                this.nodeId = value;
                return this;
            }

            Builder bootstrapServers(String value) {
                this.bootstrapServers = value;
                return this;
            }

            Builder topic(String value) {
                this.topic = value;
                return this;
            }

            Builder groupId(String value) {
                this.groupId = value;
                return this;
            }

            Builder clientId(String value) {
                this.clientId = value;
                return this;
            }

            Builder autoCreateTopic(boolean value) {
                this.autoCreateTopic = value;
                return this;
            }

            Builder topicPartitions(int value) {
                this.topicPartitions = value;
                return this;
            }

            Builder topicReplicationFactor(short value) {
                this.topicReplicationFactor = value;
                return this;
            }

            Builder topicRetentionMs(long value) {
                this.topicRetentionMs = value;
                return this;
            }

            Builder pollIntervalMs(int value) {
                this.pollIntervalMs = value;
                return this;
            }

            Builder retryBackoffMs(long value) {
                this.retryBackoffMs = value;
                return this;
            }

            Builder sessionTimeoutMs(int value) {
                this.sessionTimeoutMs = value;
                return this;
            }

            Builder heartbeatIntervalMs(int value) {
                this.heartbeatIntervalMs = value;
                return this;
            }

            Builder requestTimeoutMs(int value) {
                this.requestTimeoutMs = value;
                return this;
            }

            Builder clientOverrides(Properties value) {
                this.clientOverrides = value;
                return this;
            }

            KafkaLeaderElectionConfig build() {
                return new KafkaLeaderElectionConfig(this);
            }
        }
    }
}
