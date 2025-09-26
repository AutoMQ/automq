package org.apache.kafka.connect.automq;

import com.automq.log.uploader.DefaultS3LogConfig;
import com.automq.log.uploader.LogConfigConstants;
import com.automq.log.uploader.S3LogConfig;
import com.automq.log.uploader.S3LogConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides S3 log uploader configuration for Kafka Connect workers.
 */
public class ConnectS3LogConfigProvider implements S3LogConfigProvider {
    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConnectS3LogConfigProvider.class);
    }
    private static final AtomicReference<Properties> CONFIG = new AtomicReference<>();
    private static final long WAIT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final CountDownLatch INIT = new CountDownLatch(1);

    public static void initialize(Properties workerProps) {
        try {
            if (workerProps == null) {
                CONFIG.set(null);
                return;
            }
            Properties copy = new Properties();
            for (Map.Entry<Object, Object> entry : workerProps.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    copy.put(entry.getKey(), entry.getValue());
                }
            }
            CONFIG.set(copy);
        } finally {
            INIT.countDown();
        }
        getLogger().info("Initializing ConnectS3LogConfigProvider");
    }

    @Override
    public S3LogConfig get() {
        
        try {
            if (!INIT.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                getLogger().warn("S3 log uploader config not initialized within timeout; uploader disabled.");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            getLogger().warn("Interrupted while waiting for S3 log uploader config; uploader disabled.");
            return null;
        }

        Properties source = CONFIG.get();
        if (source == null) {
            getLogger().warn("S3 log upload configuration was not provided; uploader disabled.");
            return null;
        }

        Properties effective = buildEffectiveProperties(source);
        if (!Boolean.parseBoolean(effective.getProperty(LogConfigConstants.LOG_S3_ENABLE_KEY, "false"))) {
            getLogger().info("S3 log uploader is disabled via {}", LogConfigConstants.LOG_S3_ENABLE_KEY);
            return null;
        }
        return new DefaultS3LogConfig(effective);
    }

    private Properties buildEffectiveProperties(Properties workerProps) {
        Properties effective = new Properties();
        workerProps.forEach((k, v) -> effective.put(String.valueOf(k), String.valueOf(v)));

        copyIfPresent(workerProps, "automq.log.s3.bucket", effective, LogConfigConstants.LOG_S3_BUCKET_KEY);
        copyIfPresent(workerProps, "automq.log.s3.enable", effective, LogConfigConstants.LOG_S3_ENABLE_KEY);
        copyIfPresent(workerProps, "automq.log.s3.region", effective, LogConfigConstants.LOG_S3_REGION_KEY);
        copyIfPresent(workerProps, "automq.log.s3.endpoint", effective, LogConfigConstants.LOG_S3_ENDPOINT_KEY);
        copyIfPresent(workerProps, "automq.log.s3.access.key", effective, LogConfigConstants.LOG_S3_ACCESS_KEY);
        copyIfPresent(workerProps, "automq.log.s3.secret.key", effective, LogConfigConstants.LOG_S3_SECRET_KEY);
        copyIfPresent(workerProps, "automq.log.s3.primary.node", effective, LogConfigConstants.LOG_S3_PRIMARY_NODE_KEY);
        copyIfPresent(workerProps, "automq.log.s3.selector.type", effective, LogConfigConstants.LOG_S3_SELECTOR_TYPE_KEY);
        copyIfPresent(workerProps, "automq.log.s3.selector.primary.node.id", effective, LogConfigConstants.LOG_S3_SELECTOR_PRIMARY_NODE_ID_KEY);

        // Default cluster ID
        if (!effective.containsKey(LogConfigConstants.LOG_S3_CLUSTER_ID_KEY)) {
            String groupId = workerProps.getProperty("group.id", LogConfigConstants.DEFAULT_LOG_S3_CLUSTER_ID);
            effective.setProperty(LogConfigConstants.LOG_S3_CLUSTER_ID_KEY, groupId);
        }

        // Default node ID
        if (!effective.containsKey(LogConfigConstants.LOG_S3_NODE_ID_KEY)) {
            String nodeId = resolveNodeId(workerProps);
            effective.setProperty(LogConfigConstants.LOG_S3_NODE_ID_KEY, nodeId);
        }

        // Selector defaults
        if (!effective.containsKey(LogConfigConstants.LOG_S3_SELECTOR_TYPE_KEY)) {
            effective.setProperty(LogConfigConstants.LOG_S3_SELECTOR_TYPE_KEY, "kafka");
        }

        String selectorPrefix = LogConfigConstants.LOG_S3_SELECTOR_PREFIX;
        String bootstrapKey = selectorPrefix + "kafka.bootstrap.servers";
        if (!effective.containsKey(bootstrapKey)) {
            String bootstrap = workerProps.getProperty("automq.log.s3.selector.kafka.bootstrap.servers",
                workerProps.getProperty("bootstrap.servers"));
            if (!isBlank(bootstrap)) {
                effective.setProperty(bootstrapKey, bootstrap);
            }
        }

        String clusterId = effective.getProperty(LogConfigConstants.LOG_S3_CLUSTER_ID_KEY, "connect");
        String groupKey = selectorPrefix + "kafka.group.id";
        if (!effective.containsKey(groupKey)) {
            effective.setProperty(groupKey, "automq-log-uploader-" + clusterId);
        }

        String topicKey = selectorPrefix + "kafka.topic";
        if (!effective.containsKey(topicKey)) {
            effective.setProperty(topicKey, "__automq_connect_log_leader_" + clusterId.replaceAll("[^A-Za-z0-9_-]", ""));
        }

        String clientKey = selectorPrefix + "kafka.client.id";
        if (!effective.containsKey(clientKey)) {
            effective.setProperty(clientKey, "automq-log-uploader-client-" + effective.getProperty(LogConfigConstants.LOG_S3_NODE_ID_KEY));
        }

        String autoCreateKey = selectorPrefix + "kafka.auto.create.topic";
        effective.putIfAbsent(autoCreateKey, "true");

        // Map any existing selector.* overrides from worker props
        for (String name : workerProps.stringPropertyNames()) {
            if (name.startsWith(selectorPrefix)) {
                effective.setProperty(name, workerProps.getProperty(name));
            }
        }

        return effective;
    }

    private void copyIfPresent(Properties src, String srcKey, Properties dest, String destKey) {
        String value = src.getProperty(srcKey);
        if (!isBlank(value)) {
            dest.setProperty(destKey, value.trim());
        }
    }

    private String resolveNodeId(Properties workerProps) {
        String fromConfig = workerProps.getProperty(LogConfigConstants.LOG_S3_NODE_ID_KEY);
        if (!isBlank(fromConfig)) {
            return fromConfig.trim();
        }
        String env = System.getenv("CONNECT_NODE_ID");
        if (!isBlank(env)) {
            return env.trim();
        }
        String host = workerProps.getProperty("automq.log.s3.node.hostname");
        if (isBlank(host)) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = System.getenv().getOrDefault("HOSTNAME", "0");
            }
        }
        return Integer.toString(Math.abs(host.hashCode()));
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
