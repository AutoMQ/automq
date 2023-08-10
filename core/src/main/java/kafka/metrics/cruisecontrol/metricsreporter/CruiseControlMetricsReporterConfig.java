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
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CruiseControlMetricsReporterConfig extends AbstractConfig {
    public static final String PREFIX = "cruise.control.metrics.reporter.";
    // Configurations
    public static final String CRUISE_CONTROL_METRICS_TOPIC_CONFIG = "cruise.control.metrics.topic";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG = "cruise.control.metrics.topic.auto.create";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_CONFIG = "cruise.control.metrics.topic.auto.create.timeout.ms";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_CONFIG = "cruise.control.metrics.topic.auto.create.retries";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG = "cruise.control.metrics.topic.num.partitions";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG = "cruise.control.metrics.topic.replication.factor";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS_CONFIG = "cruise.control.metrics.topic.retention.ms";
    public static final String CRUISE_CONTROL_METRICS_TOPIC_MIN_INSYNC_REPLICAS_CONFIG = "cruise.control.metrics.topic.min.insync.replicas";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES_CONFIG = "cruise.control.metrics.reporter.create.retries";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG = PREFIX + "metrics.reporting.interval.ms";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG = PREFIX + "linger.ms";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_CONFIG = PREFIX + "max.block.ms";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_CONFIG = PREFIX + "batch.size";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE_CONFIG = PREFIX + "kubernetes.mode";
    public static final String CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE_DOC = "Cruise Control metrics reporter will report "
            + "metrics using methods that are aware of container boundaries.";
    // Default values
    public static final String DEFAULT_CRUISE_CONTROL_METRICS_TOPIC = "__CruiseControlMetrics";
    public static final Integer DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS = -1;
    public static final boolean DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE = false;
    public static final long DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final Integer DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES = 5;
    public static final Short DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR = -1;
    public static final long DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS = TimeUnit.HOURS.toMillis(5);
    public static final Short DEFAULT_CRUISE_CONTROL_MIN_INSYNC_REPLICAS = -1;
    public static final long DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
    public static final String PRODUCER_ID = "CruiseControlMetricsReporter";
    public static final int DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS = (int) TimeUnit.SECONDS.toMillis(1);
    public static final int DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS = (int) TimeUnit.MINUTES.toMillis(1);
    public static final int DEFAULT_CRUISE_CONTROL_METRICS_BATCH_SIZE = 800 * 1000;
    public static final boolean DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE = false;
    public static final int DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES = 2;
    private static final ConfigDef CONFIG;
    private static final Set<String> CONFIGS = new HashSet<>();
    private static final String CRUISE_CONTROL_METRICS_TOPIC_DOC = "The topic to which Cruise Control metrics reporter "
            + "should send messages";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_DOC = "Cruise Control metrics reporter will enforce the creation of the"
            + " topic at launch";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_DOC = "Timeout on the Cruise Control metrics topic creation";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_DOC = "Number of retries of the Cruise Control metrics reporter"
            + " for the topic creation";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions of Cruise Control metrics topic";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor of Cruise Control metrics topic";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS_DOC = "The retention time in milliseconds of Cruise Control metrics topic.";
    private static final String CRUISE_CONTROL_METRICS_TOPIC_MIN_INSYNC_REPLICAS_DOC = "The minimum number of insync replicas for the Cruise "
            + "Control metrics topic";
    private static final String CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES_DOC = "Number of times the Cruise Control metrics reporter will "
            + "attempt to create the producer while starting up.";
    private static final String CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_DOC = "The interval in milliseconds the "
            + "metrics reporter should report the metrics.";
    private static final String CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_DOC = "The linger.ms configuration of KafkaProducer used in Cruise "
            + "Control metrics reporter. Set this config and cruise.control.metrics.reporter.batch.size to a large number to have better batching.";
    private static final String CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_DOC = "The max.block.ms configuration of KafkaProducer used in Cruise "
            + "Control metrics reporter.";
    private static final String CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_DOC = "The batch.size configuration of KafkaProducer used in Cruise "
            + "Control metrics reporter. Set this config and cruise.control.metrics.reporter.linger.ms to a large number to have better batching.";

    static {
        ProducerConfig.configNames().forEach(name -> CONFIGS.add(PREFIX + name));
        CONFIG = new ConfigDef()
                .define(PREFIX + CommonClientConfigs.CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        PRODUCER_ID,
                        ConfigDef.Importance.LOW,
                        "The producer id for Cruise Control metrics reporter")
                .define(CRUISE_CONTROL_METRICS_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC,
                        ConfigDef.Importance.HIGH,
                        CRUISE_CONTROL_METRICS_TOPIC_DOC)
                .define(CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS,
                        ConfigDef.Importance.HIGH,
                        CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_DOC)
                .define(CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_REPORTER_KUBERNETES_MODE_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_TIMEOUT_MS_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_RETRIES_DOC)
                .define(CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_REPORTER_CREATE_RETRIES_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_RETENTION_MS_DOC)
                .define(CRUISE_CONTROL_METRICS_TOPIC_MIN_INSYNC_REPLICAS_CONFIG,
                        ConfigDef.Type.SHORT,
                        DEFAULT_CRUISE_CONTROL_MIN_INSYNC_REPLICAS,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_TOPIC_MIN_INSYNC_REPLICAS_DOC)
                .define(CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_REPORTER_LINGER_MS_DOC)
                .define(CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_REPORTER_MAX_BLOCK_MS_DOC)
                .define(CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_CRUISE_CONTROL_METRICS_BATCH_SIZE,
                        ConfigDef.Importance.LOW,
                        CRUISE_CONTROL_METRICS_REPORTER_BATCH_SIZE_DOC);
    }

    public CruiseControlMetricsReporterConfig(Map<?, ?> originals, boolean doLog) {
        super(CONFIG, originals, doLog);
    }

    /**
     * @param baseConfigName Base config name.
     * @return Cruise Control metrics reporter config name.
     */
    public static String config(String baseConfigName) {
        String configName = PREFIX + baseConfigName;
        if (!CONFIGS.contains(configName)) {
            throw new IllegalArgumentException("The base config name " + baseConfigName + " is not defined.");
        }
        return configName;
    }

    static Properties parseProducerConfigs(Map<String, ?> configMap) {
        Properties props = new Properties();
        for (Map.Entry<String, ?> entry : configMap.entrySet()) {
            if (entry.getKey().startsWith(PREFIX)) {
                props.put(entry.getKey().replace(PREFIX, ""), entry.getValue());
            }
        }
        return props;
    }
}
