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

package kafka.autobalancer.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class was modified based on Cruise Control: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.
 */
public class AutoBalancerMetricsReporterConfig extends AbstractConfig {
    private static final Set<String> PRODUCER_CONFIGS = new HashSet<>();
    public static final ConfigDef CONFIG_DEF = new ConfigDef();
    /* Configurations */
    private static final String PREFIX = "autobalancer.reporter.";
    public static final String AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES_CONFIG = PREFIX + "producer.create.retries";
    public static final String AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG = PREFIX + "metrics.reporting.interval.ms";
    public static final String AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID = PREFIX + ProducerConfig.CLIENT_ID_CONFIG;
    public static final String AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_CONFIG = PREFIX + ProducerConfig.LINGER_MS_CONFIG;
    public static final String AUTO_BALANCER_METRICS_REPORTER_BATCH_SIZE_CONFIG = PREFIX + ProducerConfig.BATCH_SIZE_CONFIG;
    /* Default values */
    public static final String DEFAULT_AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID = "AutoBalancerMetricsReporterProducer";
    public static final long DEFAULT_AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
    public static final int DEFAULT_AUTO_BALANCER_METRICS_REPORTER_LINGER_MS = (int) TimeUnit.SECONDS.toMillis(1);
    public static final int DEFAULT_AUTO_BALANCER_METRICS_BATCH_SIZE = 800 * 1000;
    public static final int DEFAULT_AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES = 2;
    /* Documents */
    private static final String AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES_DOC = "Number of times the Auto Balancer metrics reporter will "
            + "attempt to create the producer while starting up.";
    private static final String AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_DOC = "The interval in milliseconds the "
            + "metrics reporter should report the metrics.";
    public static final String AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID_DOC = CommonClientConfigs.CLIENT_ID_DOC;
    private static final String AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_DOC = "The linger.ms configuration of KafkaProducer used in AutoBalancer "
            + " metrics reporter. Set this config and autobalancer.reporter.batch.size to a large number to have better batching.";
    private static final String AUTO_BALANCER_METRICS_REPORTER_BATCH_SIZE_DOC = "The batch.size configuration of KafkaProducer used in AutoBalancer "
            + " metrics reporter. Set this config and autobalancer.reporter.linger.ms to a large number to have better batching.";

    static {
        ProducerConfig.configNames().forEach(name -> PRODUCER_CONFIGS.add(PREFIX + name));
        CONFIG_DEF.define(AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS,
                        ConfigDef.Importance.HIGH,
                        AUTO_BALANCER_METRICS_REPORTER_INTERVAL_MS_DOC)
                .define(AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES,
                        ConfigDef.Importance.LOW,
                        AUTO_BALANCER_METRICS_REPORTER_CREATE_RETRIES_DOC)
                .define(AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_AUTO_BALANCER_METRICS_REPORTER_LINGER_MS,
                        ConfigDef.Importance.LOW,
                        AUTO_BALANCER_METRICS_REPORTER_LINGER_MS_DOC)
                .define(AUTO_BALANCER_METRICS_REPORTER_BATCH_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_AUTO_BALANCER_METRICS_BATCH_SIZE,
                        ConfigDef.Importance.LOW,
                        AUTO_BALANCER_METRICS_REPORTER_BATCH_SIZE_DOC)
                .define(AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID,
                        ConfigDef.Type.STRING,
                        DEFAULT_AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID,
                        ConfigDef.Importance.LOW,
                        AUTO_BALANCER_METRICS_REPORTER_PRODUCER_CLIENT_ID_DOC);
    }

    public AutoBalancerMetricsReporterConfig(Map<?, ?> originals, boolean doLog) {
        super(CONFIG_DEF, originals, doLog);
    }

    /**
     * @param baseConfigName Base config name.
     * @return Auto balancer metrics reporter config name.
     */
    public static String config(String baseConfigName) {
        String configName = PREFIX + baseConfigName;
        if (!PRODUCER_CONFIGS.contains(configName)) {
            throw new IllegalArgumentException("The base config name " + baseConfigName + " is not defined.");
        }
        return configName;
    }

    public static Properties parseProducerConfigs(Map<String, ?> configMap) {
        Properties props = new Properties();
        for (Map.Entry<String, ?> entry : configMap.entrySet()) {
            if (entry.getKey().startsWith(PREFIX)) {
                props.put(entry.getKey().replace(PREFIX, ""), entry.getValue());
            }
        }
        return props;
    }
}
