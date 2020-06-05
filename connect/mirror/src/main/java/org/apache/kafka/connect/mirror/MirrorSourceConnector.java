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

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsOptions;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replicate data, configuration, and ACLs between clusters.
 *
 *  @see MirrorConnectorConfig for supported config properties.
 */
public class MirrorSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceConnector.class);
    private static final ResourcePatternFilter ANY_TOPIC = new ResourcePatternFilter(ResourceType.TOPIC,
        null, PatternType.ANY);
    private static final AclBindingFilter ANY_TOPIC_ACL = new AclBindingFilter(ANY_TOPIC, AccessControlEntryFilter.ANY);

    private Scheduler scheduler;
    private MirrorConnectorConfig config;
    private SourceAndTarget sourceAndTarget;
    private String connectorName;
    private TopicFilter topicFilter;
    private ConfigPropertyFilter configPropertyFilter;
    private List<TopicPartition> knownSourceTopicPartitions = Collections.emptyList();
    private List<TopicPartition> knownTargetTopicPartitions = Collections.emptyList();
    private ReplicationPolicy replicationPolicy;
    private int replicationFactor;
    private AdminClient sourceAdminClient;
    private AdminClient targetAdminClient;

    public MirrorSourceConnector() {
        // nop
    }

    // visible for testing
    MirrorSourceConnector(List<TopicPartition> knownSourceTopicPartitions, MirrorConnectorConfig config) {
        this.knownSourceTopicPartitions = knownSourceTopicPartitions;
        this.config = config;
    }

    // visible for testing
    MirrorSourceConnector(SourceAndTarget sourceAndTarget, ReplicationPolicy replicationPolicy,
            TopicFilter topicFilter, ConfigPropertyFilter configPropertyFilter) {
        this.sourceAndTarget = sourceAndTarget;
        this.replicationPolicy = replicationPolicy;
        this.topicFilter = topicFilter;
        this.configPropertyFilter = configPropertyFilter;
    } 

    @Override
    public void start(Map<String, String> props) {
        long start = System.currentTimeMillis();
        config = new MirrorConnectorConfig(props);
        if (!config.enabled()) {
            return;
        }
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        topicFilter = config.topicFilter();
        configPropertyFilter = config.configPropertyFilter();
        replicationPolicy = config.replicationPolicy();
        replicationFactor = config.replicationFactor();
        sourceAdminClient = AdminClient.create(config.sourceAdminConfig());
        targetAdminClient = AdminClient.create(config.targetAdminConfig());
        scheduler = new Scheduler(MirrorSourceConnector.class, config.adminTimeout());
        scheduler.execute(this::createOffsetSyncsTopic, "creating upstream offset-syncs topic");
        scheduler.execute(this::loadTopicPartitions, "loading initial set of topic-partitions");
        scheduler.execute(this::computeAndCreateTopicPartitions, "creating downstream topic-partitions");
        scheduler.execute(this::refreshKnownTargetTopics, "refreshing known target topics");
        scheduler.scheduleRepeating(this::syncTopicAcls, config.syncTopicAclsInterval(), "syncing topic ACLs");
        scheduler.scheduleRepeating(this::syncTopicConfigs, config.syncTopicConfigsInterval(),
            "syncing topic configs");
        scheduler.scheduleRepeatingDelayed(this::refreshTopicPartitions, config.refreshTopicsInterval(),
            "refreshing topics");
        log.info("Started {} with {} topic-partitions.", connectorName, knownSourceTopicPartitions.size());
        log.info("Starting {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        if (!config.enabled()) {
            return;
        }
        Utils.closeQuietly(scheduler, "scheduler");
        Utils.closeQuietly(topicFilter, "topic filter");
        Utils.closeQuietly(configPropertyFilter, "config property filter");
        Utils.closeQuietly(sourceAdminClient, "source admin client");
        Utils.closeQuietly(targetAdminClient, "target admin client");
        log.info("Stopping {} took {} ms.", connectorName, System.currentTimeMillis() - start);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorSourceTask.class;
    }

    // divide topic-partitions among tasks
    // since each mirrored topic has different traffic and number of partitions, to balance the load
    // across all mirrormaker instances (workers), 'roundrobin' helps to evenly assign all
    // topic-partition to the tasks, then the tasks are further distributed to workers.
    // For example, 3 tasks to mirror 3 topics with 8, 2 and 2 partitions respectively.
    // 't1' denotes 'task 1', 't0p5' denotes 'topic 0, partition 5'
    // t1 -> [t0p0, t0p3, t0p6, t1p1]
    // t2 -> [t0p1, t0p4, t0p7, t2p0]
    // t3 -> [t0p2, t0p5, t1p0, t2p1]
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!config.enabled() || knownSourceTopicPartitions.isEmpty()) {
            return Collections.emptyList();
        }
        int numTasks = Math.min(maxTasks, knownSourceTopicPartitions.size());
        List<List<TopicPartition>> roundRobinByTask = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            roundRobinByTask.add(new ArrayList<>());
        }
        int count = 0;
        for (TopicPartition partition : knownSourceTopicPartitions) {
            int index = count % numTasks;
            roundRobinByTask.get(index).add(partition);
            count++;
        }

        return roundRobinByTask.stream().map(config::taskConfigForTopicPartitions)
            .collect(Collectors.toList());
    }

    @Override
    public ConfigDef config() {
        return MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1";
    }

    // visible for testing
    List<TopicPartition> findSourceTopicPartitions()
            throws InterruptedException, ExecutionException {
        Set<String> topics = listTopics(sourceAdminClient).stream()
            .filter(this::shouldReplicateTopic)
            .collect(Collectors.toSet());
        return describeTopics(sourceAdminClient, topics).stream()
            .flatMap(MirrorSourceConnector::expandTopicDescription)
            .collect(Collectors.toList());
    }

    // visible for testing
    List<TopicPartition> findTargetTopicPartitions()
            throws InterruptedException, ExecutionException {
        Set<String> topics = listTopics(targetAdminClient).stream()
            .filter(t -> sourceAndTarget.source().equals(replicationPolicy.topicSource(t)))
            .collect(Collectors.toSet());
        return describeTopics(targetAdminClient, topics).stream()
                .flatMap(MirrorSourceConnector::expandTopicDescription)
                .collect(Collectors.toList());
    }

    // visible for testing
    void refreshTopicPartitions()
            throws InterruptedException, ExecutionException {
        knownSourceTopicPartitions = findSourceTopicPartitions();
        knownTargetTopicPartitions = findTargetTopicPartitions();
        List<TopicPartition> upstreamTargetTopicPartitions = knownTargetTopicPartitions.stream()
                .map(x -> new TopicPartition(replicationPolicy.upstreamTopic(x.topic()), x.partition()))
                .collect(Collectors.toList());

        Set<TopicPartition> newTopicPartitions = new HashSet<>(knownSourceTopicPartitions);
        newTopicPartitions.removeAll(upstreamTargetTopicPartitions);
        Set<TopicPartition> deadTopicPartitions = new HashSet<>(upstreamTargetTopicPartitions);
        deadTopicPartitions.removeAll(knownSourceTopicPartitions);
        if (!newTopicPartitions.isEmpty() || !deadTopicPartitions.isEmpty()) {
            log.info("Found {} topic-partitions on {}. {} are new. {} were removed. Previously had {}.",
                    knownSourceTopicPartitions.size(), sourceAndTarget.source(), newTopicPartitions.size(),
                    deadTopicPartitions.size(), knownSourceTopicPartitions.size());
            log.trace("Found new topic-partitions: {}", newTopicPartitions);
            computeAndCreateTopicPartitions();
            context.requestTaskReconfiguration();
        }
    }

    private void loadTopicPartitions()
            throws InterruptedException, ExecutionException {
        knownSourceTopicPartitions = findSourceTopicPartitions();
        knownTargetTopicPartitions = findTargetTopicPartitions();
    }

    private void refreshKnownTargetTopics()
            throws InterruptedException, ExecutionException {
        knownTargetTopicPartitions = findTargetTopicPartitions();
    }

    private Set<String> topicsBeingReplicated() {
        Set<String> knownTargetTopics = toTopics(knownTargetTopicPartitions);
        return knownSourceTopicPartitions.stream()
            .map(x -> x.topic())
            .distinct()
            .filter(x -> knownTargetTopics.contains(formatRemoteTopic(x)))
            .collect(Collectors.toSet());
    }

    private Set<String> toTopics(Collection<TopicPartition> tps) {
        return tps.stream()
                .map(x -> x.topic())
                .collect(Collectors.toSet());
    }

    private void syncTopicAcls()
            throws InterruptedException, ExecutionException {
        List<AclBinding> bindings = listTopicAclBindings().stream()
            .filter(x -> x.pattern().resourceType() == ResourceType.TOPIC)
            .filter(x -> x.pattern().patternType() == PatternType.LITERAL)
            .filter(this::shouldReplicateAcl)
            .filter(x -> shouldReplicateTopic(x.pattern().name()))
            .map(this::targetAclBinding)
            .collect(Collectors.toList());
        updateTopicAcls(bindings);
    }

    private void syncTopicConfigs()
            throws InterruptedException, ExecutionException {
        Map<String, Config> sourceConfigs = describeTopicConfigs(topicsBeingReplicated());
        Map<String, Config> targetConfigs = sourceConfigs.entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> targetConfig(x.getValue())));
        updateTopicConfigs(targetConfigs);
    }

    private void createOffsetSyncsTopic() {
        MirrorUtils.createSinglePartitionCompactedTopic(config.offsetSyncsTopic(), config.offsetSyncsTopicReplicationFactor(), config.sourceAdminConfig());
    }

    // visible for testing
    void computeAndCreateTopicPartitions()
            throws InterruptedException, ExecutionException {
        Map<String, Long> partitionCounts = knownSourceTopicPartitions.stream()
            .collect(Collectors.groupingBy(x -> x.topic(), Collectors.counting())).entrySet().stream()
            .collect(Collectors.toMap(x -> formatRemoteTopic(x.getKey()), x -> x.getValue()));
        Set<String> knownTargetTopics = toTopics(knownTargetTopicPartitions);
        List<NewTopic> newTopics = partitionCounts.entrySet().stream()
            .filter(x -> !knownTargetTopics.contains(x.getKey()))
            .map(x -> new NewTopic(x.getKey(), x.getValue().intValue(), (short) replicationFactor))
            .collect(Collectors.toList());
        Map<String, NewPartitions> newPartitions = partitionCounts.entrySet().stream()
            .filter(x -> knownTargetTopics.contains(x.getKey()))
            .collect(Collectors.toMap(x -> x.getKey(), x -> NewPartitions.increaseTo(x.getValue().intValue())));
        createTopicPartitions(partitionCounts, newTopics, newPartitions);
    }

    // visible for testing
    void createTopicPartitions(Map<String, Long> partitionCounts, List<NewTopic> newTopics,
            Map<String, NewPartitions> newPartitions) {
        targetAdminClient.createTopics(newTopics, new CreateTopicsOptions()).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not create topic {}.", k, e);
            } else {
                log.info("Created remote topic {} with {} partitions.", k, partitionCounts.get(k));
            }
        }));
        targetAdminClient.createPartitions(newPartitions).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e instanceof InvalidPartitionsException) {
                // swallow, this is normal
            } else if (e != null) {
                log.warn("Could not create topic-partitions for {}.", k, e);
            } else {
                log.info("Increased size of {} to {} partitions.", k, partitionCounts.get(k));
            }
        }));
    }

    private Set<String> listTopics(AdminClient adminClient)
            throws InterruptedException, ExecutionException {
        return adminClient.listTopics().names().get();
    }

    private Collection<AclBinding> listTopicAclBindings()
            throws InterruptedException, ExecutionException {
        return sourceAdminClient.describeAcls(ANY_TOPIC_ACL).values().get();
    }

    private static Collection<TopicDescription> describeTopics(AdminClient adminClient, Collection<String> topics)
            throws InterruptedException, ExecutionException {
        return adminClient.describeTopics(topics).all().get().values();
    }

    @SuppressWarnings("deprecation")
    // use deprecated alterConfigs API for broker compatibility back to 0.11.0
    private void updateTopicConfigs(Map<String, Config> topicConfigs)
            throws InterruptedException, ExecutionException {
        Map<ConfigResource, Config> configs = topicConfigs.entrySet().stream()
            .collect(Collectors.toMap(x ->
                new ConfigResource(ConfigResource.Type.TOPIC, x.getKey()), x -> x.getValue()));
        log.trace("Syncing configs for {} topics.", configs.size());
        targetAdminClient.alterConfigs(configs).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not alter configuration of topic {}.", k.name(), e);
            }
        }));
    }

    private void updateTopicAcls(List<AclBinding> bindings)
            throws InterruptedException, ExecutionException {
        log.trace("Syncing {} topic ACL bindings.", bindings.size());
        targetAdminClient.createAcls(bindings).values().forEach((k, v) -> v.whenComplete((x, e) -> {
            if (e != null) {
                log.warn("Could not sync ACL of topic {}.", k.pattern().name(), e);
            }
        }));
    }

    private static Stream<TopicPartition> expandTopicDescription(TopicDescription description) {
        String topic = description.name();
        return description.partitions().stream()
            .map(x -> new TopicPartition(topic, x.partition()));
    }

    private Map<String, Config> describeTopicConfigs(Set<String> topics)
            throws InterruptedException, ExecutionException {
        Set<ConfigResource> resources = topics.stream()
            .map(x -> new ConfigResource(ConfigResource.Type.TOPIC, x))
            .collect(Collectors.toSet());
        return sourceAdminClient.describeConfigs(resources).all().get().entrySet().stream()
            .collect(Collectors.toMap(x -> x.getKey().name(), x -> x.getValue()));
    }

    Config targetConfig(Config sourceConfig) {
        List<ConfigEntry> entries = sourceConfig.entries().stream()
            .filter(x -> !x.isDefault() && !x.isReadOnly() && !x.isSensitive())
            .filter(x -> x.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
            .filter(x -> shouldReplicateTopicConfigurationProperty(x.name()))
            .collect(Collectors.toList());
        return new Config(entries);
    }

    private static AccessControlEntry downgradeAllowAllACL(AccessControlEntry entry) {
        return new AccessControlEntry(entry.principal(), entry.host(), AclOperation.READ, entry.permissionType());
    }

    AclBinding targetAclBinding(AclBinding sourceAclBinding) {
        String targetTopic = formatRemoteTopic(sourceAclBinding.pattern().name());
        final AccessControlEntry entry;
        if (sourceAclBinding.entry().permissionType() == AclPermissionType.ALLOW
                && sourceAclBinding.entry().operation() == AclOperation.ALL) {
            entry = downgradeAllowAllACL(sourceAclBinding.entry());
        } else {
            entry = sourceAclBinding.entry();
        }
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, targetTopic, PatternType.LITERAL), entry);
    }

    boolean shouldReplicateTopic(String topic) {
        return (topicFilter.shouldReplicateTopic(topic) || isHeartbeatTopic(topic))
            && !replicationPolicy.isInternalTopic(topic) && !isCycle(topic);
    }

    boolean shouldReplicateAcl(AclBinding aclBinding) {
        return !(aclBinding.entry().permissionType() == AclPermissionType.ALLOW
            && aclBinding.entry().operation() == AclOperation.WRITE);
    }

    boolean shouldReplicateTopicConfigurationProperty(String property) {
        return configPropertyFilter.shouldReplicateConfigProperty(property);
    }

    // Recurse upstream to detect cycles, i.e. whether this topic is already on the target cluster
    boolean isCycle(String topic) {
        String source = replicationPolicy.topicSource(topic);
        if (source == null) {
            return false;
        } else if (source.equals(sourceAndTarget.target())) {
            return true;
        } else {
            return isCycle(replicationPolicy.upstreamTopic(topic));
        }
    }

    // e.g. heartbeats, us-west.heartbeats
    boolean isHeartbeatTopic(String topic) {
        return MirrorClientConfig.HEARTBEATS_TOPIC.equals(replicationPolicy.originalTopic(topic));
    }

    String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceAndTarget.source(), topic);
    }
}
