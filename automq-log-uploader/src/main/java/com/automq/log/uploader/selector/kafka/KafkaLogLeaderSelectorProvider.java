package com.automq.log.uploader.selector.kafka;

import com.automq.log.uploader.selector.LogUploaderNodeSelector;
import com.automq.log.uploader.selector.LogUploaderNodeSelectorProvider;
import org.apache.commons.lang3.StringUtils;
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
 * Leader election based on Kafka consumer group membership.
 */
public class KafkaLogLeaderSelectorProvider implements LogUploaderNodeSelectorProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLogLeaderSelectorProvider.class);

    public static final String TYPE = "kafka";

    private static final String DEFAULT_TOPIC_PREFIX = "__automq_log_uploader_leader_";
    private static final String DEFAULT_GROUP_PREFIX = "automq-log-uploader-";
    private static final String DEFAULT_CLIENT_PREFIX = "automq-log-uploader";

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
    public LogUploaderNodeSelector createSelector(String clusterId, int nodeId, Map<String, String> config) throws Exception {
        KafkaSelectorConfig selectorConfig = KafkaSelectorConfig.from(clusterId, nodeId, config);
        KafkaSelector selector = new KafkaSelector(selectorConfig);
        selector.start();
        return selector;
    }

    private static final class KafkaSelector implements LogUploaderNodeSelector {
        private final KafkaSelectorConfig config;
        private final AtomicBoolean isLeader = new AtomicBoolean(false);
        private final AtomicBoolean running = new AtomicBoolean(true);

        private volatile KafkaConsumer<byte[], byte[]> consumer;

        KafkaSelector(KafkaSelectorConfig config) {
            this.config = config;
        }

        void start() {
            Thread thread = new Thread(this::runLoop,
                String.format(Locale.ROOT, "log-uploader-kafka-selector-%s-%d", config.clusterId, config.nodeId));
            thread.setDaemon(true);
            thread.start();
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown,
                String.format(Locale.ROOT, "log-uploader-kafka-selector-shutdown-%s-%d", config.clusterId, config.nodeId)));
        }

        private void runLoop() {
            while (running.get()) {
                try {
                    ensureTopicExists();
                    runConsumer();
                } catch (WakeupException e) {
                    if (!running.get()) {
                        break;
                    }
                    LOGGER.warn("Kafka selector interrupted unexpectedly", e);
                    sleep(config.retryBackoffMs);
                } catch (Exception e) {
                    if (!running.get()) {
                        break;
                    }
                    LOGGER.warn("Kafka selector loop failed: {}", e.getMessage(), e);
                    sleep(config.retryBackoffMs);
                }
            }
        }

        private void runConsumer() {
            Properties consumerProps = config.buildConsumerProps();
            try (KafkaConsumer<byte[], byte[]> kafkaConsumer =
                     new KafkaConsumer<>(consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                this.consumer = kafkaConsumer;
                ConsumerRebalanceListener listener = new LeaderRebalanceListener();
                kafkaConsumer.subscribe(Collections.singletonList(config.topic), listener);
                LOGGER.info("Kafka log selector subscribed to topic {} with group {}", config.topic, config.groupId);
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
            Properties adminProps = config.buildAdminProps();
            try (Admin admin = Admin.create(adminProps)) {
                NewTopic topic = new NewTopic(config.topic, config.topicPartitions, config.topicReplicationFactor);
                Map<String, String> topicConfig = new HashMap<>();
                if (config.topicRetentionMs > 0) {
                    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.topicRetentionMs));
                }
                if (!topicConfig.isEmpty()) {
                    topic.configs(topicConfig);
                }
                admin.createTopics(Collections.singleton(topic), new CreateTopicsOptions().validateOnly(false)).all().get();
                LOGGER.info("Kafka log selector ensured topic {} exists", config.topic);
            } catch (TopicExistsException ignored) {
                // already exists
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

        private void promote() {
            if (isLeader.compareAndSet(false, true)) {
                LOGGER.info("Node {} became primary log uploader for cluster {}", config.nodeId, config.clusterId);
            }
        }

        private void demote() {
            if (isLeader.getAndSet(false)) {
                LOGGER.info("Node {} lost log uploader leadership for cluster {}", config.nodeId, config.clusterId);
            }
        }

        private void shutdown() {
            if (running.compareAndSet(true, false)) {
                KafkaConsumer<byte[], byte[]> current = consumer;
                if (current != null) {
                    current.wakeup();
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

        private class LeaderRebalanceListener implements ConsumerRebalanceListener {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                if (!partitions.isEmpty()) {
                    LOGGER.debug("Kafka log selector revoked partitions {}", partitions);
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

    private static final class KafkaSelectorConfig {
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

        private KafkaSelectorConfig(Builder builder) {
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

        static KafkaSelectorConfig from(String clusterId, int nodeId, Map<String, String> rawConfig) {
            Map<String, String> config = rawConfig == null ? Collections.emptyMap() : rawConfig;
            String bootstrapServers = findBootstrapServers(config);
            if (StringUtils.isBlank(bootstrapServers)) {
                throw new IllegalArgumentException("Kafka selector requires 'bootstrap.servers'");
            }
            String normalizedCluster = StringUtils.isBlank(clusterId) ? "default" : clusterId;
            Builder builder = new Builder();
            builder.clusterId = clusterId;
            builder.nodeId = nodeId;
            builder.bootstrapServers = bootstrapServers;
            builder.topic = config.getOrDefault("topic", DEFAULT_TOPIC_PREFIX + normalizedCluster);
            builder.groupId = config.getOrDefault("group.id", DEFAULT_GROUP_PREFIX + normalizedCluster);
            builder.clientId = config.getOrDefault("client.id", DEFAULT_CLIENT_PREFIX + "-" + normalizedCluster + "-" + nodeId);
            builder.autoCreateTopic = Boolean.parseBoolean(config.getOrDefault("auto.create.topic", "true"));
            builder.topicPartitions = parseInt(config.get("topic.partitions"), 1, 1);
            builder.topicReplicationFactor = (short) parseInt(config.get("topic.replication.factor"), 1, 1);
            builder.topicRetentionMs = parseLong(config.get("topic.retention.ms"), DEFAULT_TOPIC_RETENTION_MS);
            builder.pollIntervalMs = parseInt(config.get("poll.interval.ms"), DEFAULT_POLL_INTERVAL_MS, 100);
            builder.retryBackoffMs = parseLong(config.get("retry.backoff.ms"), DEFAULT_RETRY_BACKOFF_MS);
            builder.sessionTimeoutMs = parseInt(config.get("session.timeout.ms"), DEFAULT_SESSION_TIMEOUT_MS, 1000);
            builder.heartbeatIntervalMs = parseInt(config.get("heartbeat.interval.ms"), DEFAULT_HEARTBEAT_INTERVAL_MS, 500);
            builder.requestTimeoutMs = parseInt(config.get("request.timeout.ms"), 15000, 1000);
            builder.clientOverrides = extractOverrides(config);
            return builder.build();
        }

        Properties buildConsumerProps() {
            Properties props = new Properties();
            props.putAll(clientOverrides);
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId + "-consumer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT));
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Math.max(pollIntervalMs * 3, 3000));
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
            props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
            props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            return props;
        }

        Properties buildAdminProps() {
            Properties props = new Properties();
            props.putAll(clientOverrides);
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId + "-admin");
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
            return props;
        }

        private static Properties extractOverrides(Map<String, String> config) {
            Properties props = new Properties();
            for (Map.Entry<String, String> entry : config.entrySet()) {
                if (RESERVED_KEYS.contains(entry.getKey())) {
                    continue;
                }
                props.put(entry.getKey(), entry.getValue());
            }
            return props;
        }

        private static String findBootstrapServers(Map<String, String> config) {
            String value = config.get("bootstrap.servers");
            if (StringUtils.isNotBlank(value)) {
                return value;
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
            private Properties clientOverrides = new Properties();

            private KafkaSelectorConfig build() {
                return new KafkaSelectorConfig(this);
            }
        }
    }
}
