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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.ForwardingAdmin;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.time.Duration;

/** Shared config properties used by MirrorSourceConnector, MirrorCheckpointConnector, and MirrorHeartbeatConnector.
 *  <p>
 *  Generally, these properties are filled-in automatically by MirrorMaker based on a top-level mm2.properties file.
 *  However, when running MM2 connectors as plugins on a Connect-as-a-Service cluster, these properties must be configured manually,
 *  e.g. via the Connect REST API.
 *  </p>
 *  <p>
 *  An example configuration when running on Connect (not via MirrorMaker driver):
 *  </p>
 *  <pre>
 *      {
 *        "name": "MirrorSourceConnector",
 *        "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
 *        "replication.factor": "1",
 *        "source.cluster.alias": "backup",
 *        "target.cluster.alias": "primary",
 *        "source.cluster.bootstrap.servers": "vip1:9092",
 *        "target.cluster.bootstrap.servers": "vip2:9092",
 *        "topics": ".*test-topic-.*",
 *        "groups": "consumer-group-.*",
 *        "emit.checkpoints.interval.seconds": "1",
 *        "emit.heartbeats.interval.seconds": "1",
 *        "sync.topic.acls.enabled": "false"
 *      }
 *  </pre>
 */
public abstract class MirrorConnectorConfig extends AbstractConfig {

    static final String ENABLED_SUFFIX = ".enabled";
    static final String INTERVAL_SECONDS_SUFFIX = ".interval.seconds";

    static final String ENABLED = "enabled";
    static final String ENABLED_DOC = "Whether to replicate source->target.";
    public static final String SOURCE_CLUSTER_ALIAS = "source.cluster.alias";
    public static final String SOURCE_CLUSTER_ALIAS_DEFAULT = "source";
    private static final String SOURCE_CLUSTER_ALIAS_DOC = "Alias of source cluster";
    public static final String TARGET_CLUSTER_ALIAS = "target.cluster.alias";
    public static final String TARGET_CLUSTER_ALIAS_DEFAULT = "target";
    private static final String TARGET_CLUSTER_ALIAS_DOC = "Alias of target cluster. Used in metrics reporting.";

    public static final String REPLICATION_POLICY_CLASS = MirrorClientConfig.REPLICATION_POLICY_CLASS;
    public static final Class<?> REPLICATION_POLICY_CLASS_DEFAULT = MirrorClientConfig.REPLICATION_POLICY_CLASS_DEFAULT;
    private static final String REPLICATION_POLICY_CLASS_DOC = "Class which defines the remote topic naming convention.";
    public static final String REPLICATION_POLICY_SEPARATOR = MirrorClientConfig.REPLICATION_POLICY_SEPARATOR;
    private static final String REPLICATION_POLICY_SEPARATOR_DOC = "Separator used in remote topic naming convention.";
    public static final String REPLICATION_POLICY_SEPARATOR_DEFAULT =
            MirrorClientConfig.REPLICATION_POLICY_SEPARATOR_DEFAULT;

    public static final String ADMIN_TASK_TIMEOUT_MILLIS = "admin.timeout.ms";
    private static final String ADMIN_TASK_TIMEOUT_MILLIS_DOC = "Timeout for administrative tasks, e.g. detecting new topics.";
    public static final long ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT = 60000L;

    public static final String FORWARDING_ADMIN_CLASS = MirrorClientConfig.FORWARDING_ADMIN_CLASS;
    public static final Class<?> FORWARDING_ADMIN_CLASS_DEFAULT = MirrorClientConfig.FORWARDING_ADMIN_CLASS_DEFAULT;
    private static final String FORWARDING_ADMIN_CLASS_DOC = MirrorClientConfig.FORWARDING_ADMIN_CLASS_DOC;

    protected static final String SOURCE_CLUSTER_PREFIX = MirrorMakerConfig.SOURCE_CLUSTER_PREFIX;
    protected static final String TARGET_CLUSTER_PREFIX = MirrorMakerConfig.TARGET_CLUSTER_PREFIX;
    protected static final String SOURCE_PREFIX = MirrorMakerConfig.SOURCE_PREFIX;
    protected static final String TARGET_PREFIX = MirrorMakerConfig.TARGET_PREFIX;
    protected static final String PRODUCER_CLIENT_PREFIX = "producer.";
    protected static final String CONSUMER_CLIENT_PREFIX = "consumer.";
    protected static final String ADMIN_CLIENT_PREFIX = "admin.";

    public static final String TOPIC_FILTER_CLASS = "topic.filter.class";
    public static final String TOPIC_FILTER_CLASS_DOC = "TopicFilter to use. Selects topics to replicate.";
    public static final Class<?> TOPIC_FILTER_CLASS_DEFAULT = DefaultTopicFilter.class;

    public static final String OFFSET_SYNCS_TOPIC_LOCATION = "offset-syncs.topic.location";
    public static final String OFFSET_SYNCS_TOPIC_LOCATION_DEFAULT = SOURCE_CLUSTER_ALIAS_DEFAULT;
    public static final String OFFSET_SYNCS_TOPIC_LOCATION_DOC = "The location (source/target) of the offset-syncs topic.";

    protected MirrorConnectorConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props, true);
    }

    String connectorName() {
        return getString(ConnectorConfig.NAME_CONFIG);
    }

    boolean enabled() {
        return getBoolean(ENABLED);
    }

    Duration adminTimeout() {
        return Duration.ofMillis(getLong(ADMIN_TASK_TIMEOUT_MILLIS));
    }

    String sourceClusterAlias() {
        return getString(SOURCE_CLUSTER_ALIAS);
    }

    String targetClusterAlias() {
        return getString(TARGET_CLUSTER_ALIAS);
    }

    ReplicationPolicy replicationPolicy() {
        return getConfiguredInstance(REPLICATION_POLICY_CLASS, ReplicationPolicy.class);
    }

    Map<String, Object> sourceProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(PRODUCER_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_PREFIX + PRODUCER_CLIENT_PREFIX));
        return props;
    }

    Map<String, Object> sourceConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(CONSUMER_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_PREFIX + CONSUMER_CLIENT_PREFIX));
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    Map<String, Object> targetAdminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(TARGET_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(TARGET_PREFIX + ADMIN_CLIENT_PREFIX));
        return props;
    }

    Map<String, Object> targetProducerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(TARGET_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(PRODUCER_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(TARGET_PREFIX + PRODUCER_CLIENT_PREFIX));
        return props;
    }

    Map<String, Object> targetConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(TARGET_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(CONSUMER_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(TARGET_PREFIX + CONSUMER_CLIENT_PREFIX));
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    Map<String, Object> sourceAdminConfig() {
        Map<String, Object> props = new HashMap<>();
        props.putAll(originalsWithPrefix(SOURCE_CLUSTER_PREFIX));
        props.keySet().retainAll(MirrorClientConfig.CLIENT_CONFIG_DEF.names());
        props.putAll(originalsWithPrefix(ADMIN_CLIENT_PREFIX));
        props.putAll(originalsWithPrefix(SOURCE_PREFIX + ADMIN_CLIENT_PREFIX));
        return props;
    }

    List<MetricsReporter> metricsReporters() {
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(this);
        MetricsContext metricsContext = new KafkaMetricsContext("kafka.connect.mirror");

        for (MetricsReporter reporter : reporters) {
            reporter.contextChange(metricsContext);
        }

        return reporters;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    ForwardingAdmin forwardingAdmin(Map<String, Object> config) {
        try {
            return Utils.newParameterizedInstance(
                    getClass(FORWARDING_ADMIN_CLASS).getName(), (Class<Map<String, Object>>) (Class) Map.class, config
            );
        } catch (ClassNotFoundException e) {
            throw new KafkaException("Can't create instance of " + get(FORWARDING_ADMIN_CLASS), e);
        }
    }

    @SuppressWarnings("deprecation")
    protected static final ConfigDef BASE_CONNECTOR_CONFIG_DEF = new ConfigDef(ConnectorConfig.configDef())
            .define(
                    ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    ENABLED_DOC)
            .define(
                    SOURCE_CLUSTER_ALIAS,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    SOURCE_CLUSTER_ALIAS_DOC)
            .define(
                    TARGET_CLUSTER_ALIAS,
                    ConfigDef.Type.STRING,
                    TARGET_CLUSTER_ALIAS_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    TARGET_CLUSTER_ALIAS_DOC)
            .define(
                    ADMIN_TASK_TIMEOUT_MILLIS,
                    ConfigDef.Type.LONG,
                    ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    ADMIN_TASK_TIMEOUT_MILLIS_DOC)
            .define(
                    REPLICATION_POLICY_CLASS,
                    ConfigDef.Type.CLASS,
                    REPLICATION_POLICY_CLASS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REPLICATION_POLICY_CLASS_DOC)
            .define(
                    REPLICATION_POLICY_SEPARATOR,
                    ConfigDef.Type.STRING,
                    REPLICATION_POLICY_SEPARATOR_DEFAULT,
                    ConfigDef.Importance.LOW,
                    REPLICATION_POLICY_SEPARATOR_DOC)
            .define(
                    FORWARDING_ADMIN_CLASS,
                    ConfigDef.Type.CLASS,
                    FORWARDING_ADMIN_CLASS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    FORWARDING_ADMIN_CLASS_DOC)
            .define(
                    CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
            .define(
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                    ConfigDef.Type.STRING,
                    CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                    in(Utils.enumOptions(SecurityProtocol.class)),
                    ConfigDef.Importance.MEDIUM,
                    CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .define(
                    CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    CommonClientConfigs.AUTO_INCLUDE_JMX_REPORTER_DOC
            )
            .withClientSslSupport()
            .withClientSaslSupport();
}
