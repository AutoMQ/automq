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

package com.automq.shell.command;

import com.automq.shell.AutoMQCLI;
import com.automq.shell.stream.ClientStreamManager;
import com.automq.shell.util.CLIUtils;
import com.automq.stream.s3.metadata.StreamMetadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "force-close", description = "Stream lossy failure recovery operation.")
public class ForceClose implements Callable<Integer> {
    @ParentCommand
    AutoMQCLI cli;

    @ArgGroup(multiplicity = "1")
    Exclusive exclusive;

    static class TopicPartition {
        @Option(names = {"-t", "--topic"}, description = "The topic you want to close.", required = true)
        String topic;

        @Option(names = {"-p", "--partition"}, description = "The partition you want to close.", required = true)
        List<Integer> partitionList;
    }

    static class Exclusive {
        @Option(names = {"-n", "--node-id"}, description = "The Kafka node id.")
        int nodeId = -1;

        @ArgGroup(exclusive = false, multiplicity = "0..1")
        TopicPartition topicPartition;
    }

    @Option(names = {"-c", "--max-count"}, description = "Max partition count in a batch.", defaultValue = "1000")
    int maxCount;

    @Option(names = {"-d", "--dry-run"}, description = "Display the affected stream(s).")
    boolean dryRun;

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cli.bootstrapServer);
        Admin admin = Admin.create(properties);

        Optional<Node> controllerOptional = findAnyControllerNode(admin);
        if (controllerOptional.isEmpty()) {
            System.err.println("No controller node found.");
            return 1;
        }

        NetworkClient client = CLIUtils.buildNetworkClient("automq-cli", new AdminClientConfig(new Properties()), new Metrics(), Time.SYSTEM, new LogContext());
        ClientStreamManager manager = new ClientStreamManager(client, controllerOptional.get());

        Map<Integer, List<StreamMetadata>> map;
        if (exclusive.topicPartition == null) {
            map = getStreamMetadataByNode(admin, manager, exclusive.nodeId);
        } else {
            map = getStreamMetadataByTopicPartition(admin, manager, exclusive.topicPartition.topic, exclusive.topicPartition.partitionList);
        }

        for (Map.Entry<Integer, List<StreamMetadata>> entry : map.entrySet()) {
            int nodeId = entry.getKey();
            List<StreamMetadata> streamMetadataList = entry.getValue();

            for (StreamMetadata metadata : streamMetadataList) {
                if (dryRun) {
                    System.out.println("Node: " + nodeId + ", Stream: " + metadata.streamId() + ", Epoch: " + metadata.epoch());
                } else {
                    manager.closeStream(metadata.streamId(), metadata.epoch(), nodeId);
                    System.out.println("Node: " + nodeId + ", Stream: " + metadata.streamId() + ", Epoch: " + metadata.epoch() + " closed.");
                }
            }
        }

        return 0;
    }

    private Optional<Node> findAnyControllerNode(Admin admin) throws Exception {
        QuorumInfo info = admin.describeMetadataQuorum().quorumInfo().get();
        int controllerLeaderId = info.leaderId();
        return admin.describeCluster().nodes().get()
            .stream().filter(node -> node.id() == controllerLeaderId).findFirst();
    }

    private Map<Integer, List<StreamMetadata>> getStreamMetadataByNode(Admin admin, ClientStreamManager manager,
        int nodeId) throws Exception {
        DescribeClusterResult result = admin.describeCluster();
        Collection<Node> nodes = result.nodes().get();

        Optional<Node> nodeOptional = nodes.stream().filter(node -> node.id() == nodeId).findFirst();
        if (nodeOptional.isEmpty()) {
            return Map.of();
        }

        return Map.of(nodeId, manager.getOpeningStreams(nodeOptional.get(), System.currentTimeMillis(), false));
    }

    private Map<Integer, List<StreamMetadata>> getStreamMetadataByTopicPartition(Admin admin,
        ClientStreamManager manager,
        String topic, List<Integer> partitionList) throws Exception {
        DescribeTopicsResult result = admin.describeTopics(List.of(topic));
        Map<Uuid, TopicDescription> map = result.allTopicIds().get();
        if (map.isEmpty()) {
            return Map.of();
        }

        String topicId = map.keySet().stream().findFirst().get().toString();
        TopicDescription topicDescription = map.values().stream().findFirst().get();
        Set<Integer> partitionSet = new HashSet<>(partitionList);

        List<Node> nodeList = topicDescription.partitions()
            .stream()
            .filter(partition -> partitionSet.contains(partition.partition()))
            .map(TopicPartitionInfo::leader)
            .collect(Collectors.toList());

        Map<Integer, List<StreamMetadata>> resultMap = new HashMap<>();
        for (Node node : nodeList) {
            manager.getOpeningStreams(node, System.currentTimeMillis(), false)
                .stream()
                .filter(stream -> {
                    Map<String, String> tagMap = stream.tagMap();
                    return topicId.equals(tagMap.get("0")) && partitionSet.contains(Integer.parseInt(tagMap.get("1")));
                })
                .forEach(metadata -> resultMap.computeIfAbsent(node.id(), id -> new ArrayList<>()).add(metadata));
        }
        return resultMap;
    }
}
