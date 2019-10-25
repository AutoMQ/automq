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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MockAdminClient extends AdminClient {
    public static final String DEFAULT_CLUSTER_ID = "I4ZmrWqfT2e-upky_4fdPA";

    private final List<Node> brokers;
    private final Map<String, TopicMetadata> allTopics = new HashMap<>();
    private final String clusterId;

    private Node controller;
    private int timeoutNextRequests = 0;

    private Map<MetricName, Metric> mockMetrics = new HashMap<>();

    /**
     * Creates MockAdminClient for a cluster with the given brokers. The Kafka cluster ID uses the default value from
     * DEFAULT_CLUSTER_ID.
     *
     * @param brokers list of brokers in the cluster
     * @param controller node that should start as the controller
     */
    public MockAdminClient(List<Node> brokers, Node controller) {
        this(brokers, controller, DEFAULT_CLUSTER_ID);
    }

    /**
     * Creates MockAdminClient for a cluster with the given brokers.
     * @param brokers list of brokers in the cluster
     * @param controller node that should start as the controller
     */
    public MockAdminClient(List<Node> brokers, Node controller, String clusterId) {
        this.brokers = brokers;
        controller(controller);
        this.clusterId = clusterId;
    }

    public void controller(Node controller) {
        if (!brokers.contains(controller))
            throw new IllegalArgumentException("The controller node must be in the list of brokers");
        this.controller = controller;
    }

    public void addTopic(boolean internal,
                         String name,
                         List<TopicPartitionInfo> partitions,
                         Map<String, String> configs) {
        if (allTopics.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Topic %s was already added.", name));
        }
        List<Node> replicas = null;
        for (TopicPartitionInfo partition : partitions) {
            if (!brokers.contains(partition.leader())) {
                throw new IllegalArgumentException("Leader broker unknown");
            }
            if (!brokers.containsAll(partition.replicas())) {
                throw new IllegalArgumentException("Unknown brokers in replica list");
            }
            if (!brokers.containsAll(partition.isr())) {
                throw new IllegalArgumentException("Unknown brokers in isr list");
            }

            if (replicas == null) {
                replicas = partition.replicas();
            } else if (!replicas.equals(partition.replicas())) {
                throw new IllegalArgumentException("All partitions need to have the same replica nodes.");
            }
        }

        allTopics.put(name, new TopicMetadata(internal, partitions, configs));
    }

    public void markTopicForDeletion(final String name) {
        if (!allTopics.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Topic %s did not exist.", name));
        }

        allTopics.get(name).markedForDeletion = true;
    }

    public void timeoutNextRequest(int numberOfRequest) {
        timeoutNextRequests = numberOfRequest;
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        KafkaFutureImpl<Collection<Node>> nodesFuture = new KafkaFutureImpl<>();
        KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        KafkaFutureImpl<String> brokerIdFuture = new KafkaFutureImpl<>();
        KafkaFutureImpl<Set<AclOperation>> authorizedOperationsFuture = new KafkaFutureImpl<>();

        if (timeoutNextRequests > 0) {
            nodesFuture.completeExceptionally(new TimeoutException());
            controllerFuture.completeExceptionally(new TimeoutException());
            brokerIdFuture.completeExceptionally(new TimeoutException());
            authorizedOperationsFuture.completeExceptionally(new TimeoutException());
            --timeoutNextRequests;
        } else {
            nodesFuture.complete(brokers);
            controllerFuture.complete(controller);
            brokerIdFuture.complete(clusterId);
            authorizedOperationsFuture.complete(Collections.emptySet());
        }

        return new DescribeClusterResult(nodesFuture, controllerFuture, brokerIdFuture, authorizedOperationsFuture);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        Map<String, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> createTopicResult = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (final NewTopic newTopic : newTopics) {
                String topicName = newTopic.name();

                KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                createTopicResult.put(topicName, future);
            }

            --timeoutNextRequests;
            return new CreateTopicsResult(createTopicResult);
        }

        for (final NewTopic newTopic : newTopics) {
            KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> future = new KafkaFutureImpl<>();

            String topicName = newTopic.name();
            if (allTopics.containsKey(topicName)) {
                future.completeExceptionally(new TopicExistsException(String.format("Topic %s exists already.", topicName)));
                createTopicResult.put(topicName, future);
                continue;
            }
            int replicationFactor = newTopic.replicationFactor();
            List<Node> replicas = new ArrayList<>(replicationFactor);
            for (int i = 0; i < replicationFactor; ++i) {
                replicas.add(brokers.get(i));
            }

            int numberOfPartitions = newTopic.numPartitions();
            List<TopicPartitionInfo> partitions = new ArrayList<>(numberOfPartitions);
            for (int p = 0; p < numberOfPartitions; ++p) {
                partitions.add(new TopicPartitionInfo(p, brokers.get(0), replicas, Collections.emptyList()));
            }
            allTopics.put(topicName, new TopicMetadata(false, partitions, newTopic.configs()));
            future.complete(null);
            createTopicResult.put(topicName, future);
        }

        return new CreateTopicsResult(createTopicResult);
    }

    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        Map<String, TopicListing> topicListings = new HashMap<>();

        if (timeoutNextRequests > 0) {
            KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new TimeoutException());

            --timeoutNextRequests;
            return new ListTopicsResult(future);
        }

        for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
            String topicName = topicDescription.getKey();
            if (topicDescription.getValue().fetchesRemainingUntilVisible > 0) {
                topicDescription.getValue().fetchesRemainingUntilVisible--;
            } else {
                topicListings.put(topicName, new TopicListing(topicName, topicDescription.getValue().isInternalTopic));
            }
        }

        KafkaFutureImpl<Map<String, TopicListing>> future = new KafkaFutureImpl<>();
        future.complete(topicListings);
        return new ListTopicsResult(future);
    }

    @Override
    public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        Map<String, KafkaFuture<TopicDescription>> topicDescriptions = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (String requestedTopic : topicNames) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                topicDescriptions.put(requestedTopic, future);
            }

            --timeoutNextRequests;
            return new DescribeTopicsResult(topicDescriptions);
        }

        for (String requestedTopic : topicNames) {
            for (Map.Entry<String, TopicMetadata> topicDescription : allTopics.entrySet()) {
                String topicName = topicDescription.getKey();
                if (topicName.equals(requestedTopic) && !topicDescription.getValue().markedForDeletion) {
                    if (topicDescription.getValue().fetchesRemainingUntilVisible > 0) {
                        topicDescription.getValue().fetchesRemainingUntilVisible--;
                    } else {
                        TopicMetadata topicMetadata = topicDescription.getValue();
                        KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                        future.complete(new TopicDescription(topicName, topicMetadata.isInternalTopic, topicMetadata.partitions,
                                Collections.emptySet()));
                        topicDescriptions.put(topicName, future);
                        break;
                    }
                }
            }
            if (!topicDescriptions.containsKey(requestedTopic)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new UnknownTopicOrPartitionException("Topic " + requestedTopic + " not found."));
                topicDescriptions.put(requestedTopic, future);
            }
        }

        return new DescribeTopicsResult(topicDescriptions);
    }

    @Override
    public DeleteTopicsResult deleteTopics(Collection<String> topicsToDelete, DeleteTopicsOptions options) {
        Map<String, KafkaFuture<Void>> deleteTopicsResult = new HashMap<>();

        if (timeoutNextRequests > 0) {
            for (final String topicName : topicsToDelete) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new TimeoutException());
                deleteTopicsResult.put(topicName, future);
            }

            --timeoutNextRequests;
            return new DeleteTopicsResult(deleteTopicsResult);
        }

        for (final String topicName : topicsToDelete) {
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();

            if (allTopics.remove(topicName) == null) {
                future.completeExceptionally(new UnknownTopicOrPartitionException(String.format("Topic %s does not exist.", topicName)));
            } else {
                future.complete(null);
            }
            deleteTopicsResult.put(topicName, future);
        }

        return new DeleteTopicsResult(deleteTopicsResult);
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        Map<TopicPartition, KafkaFuture<DeletedRecords>> deletedRecordsResult = new HashMap<>();
        if (recordsToDelete.isEmpty()) {
            return new DeleteRecordsResult(deletedRecordsResult);
        } else {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Deprecated
    @Override
    public ElectPreferredLeadersResult electPreferredLeaders(Collection<TopicPartition> partitions, ElectPreferredLeadersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ElectLeadersResult electLeaders(
            ElectionType electionType,
            Set<TopicPartition> partitions,
            ElectLeadersOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        Map<ConfigResource, KafkaFuture<Config>> configescriptions = new HashMap<>();

        for (ConfigResource resource : resources) {
            if (resource.type() == ConfigResource.Type.TOPIC) {
                Map<String, String> configs = allTopics.get(resource.name()).configs;
                List<ConfigEntry> configEntries = new ArrayList<>();
                for (Map.Entry<String, String> entry : configs.entrySet()) {
                    configEntries.add(new ConfigEntry(entry.getKey(), entry.getValue()));
                }
                KafkaFutureImpl<Config> future = new KafkaFutureImpl<>();
                future.complete(new Config(configEntries));
                configescriptions.put(resource, future);
            } else {
                throw new UnsupportedOperationException("Not implemented yet");
            }
        }

        return new DescribeConfigsResult(configescriptions);
    }

    @Override
    @Deprecated
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                      AlterConfigsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
                                                                         AlterPartitionReassignmentsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions, ListPartitionReassignmentsOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public void close(Duration timeout) {}


    private final static class TopicMetadata {
        final boolean isInternalTopic;
        final List<TopicPartitionInfo> partitions;
        final Map<String, String> configs;
        int fetchesRemainingUntilVisible;

        public boolean markedForDeletion;

        TopicMetadata(boolean isInternalTopic,
                      List<TopicPartitionInfo> partitions,
                      Map<String, String> configs) {
            this.isInternalTopic = isInternalTopic;
            this.partitions = partitions;
            this.configs = configs != null ? configs : Collections.emptyMap();
            this.markedForDeletion = false;
            this.fetchesRemainingUntilVisible = 0;
        }
    }

    public void setMockMetrics(MetricName name, Metric metric) {
        mockMetrics.put(name, metric);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return mockMetrics;
    }

    public void setFetchesRemainingUntilVisible(String topicName, int fetchesRemainingUntilVisible) {
        TopicMetadata metadata = allTopics.get(topicName);
        if (metadata == null) {
            throw new RuntimeException("No such topic as " + topicName);
        }
        metadata.fetchesRemainingUntilVisible = fetchesRemainingUntilVisible;
    }
}
