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
import com.automq.stream.s3.metadata.StreamState;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DescribeStreamsResponseData;
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
        @Option(names = {"-t", "--topic-name"}, description = "The topic you want to close.", required = true)
        String topicName;

        @Option(names = {"-p", "--partition"}, description = "The partition you want to close.", required = true)
        List<Integer> partitionList;
    }

    static class Exclusive {
        @Option(names = {"-s", "--stream"}, description = "The stream id.")
        int streamId = -1;

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

        List<DescribeStreamsResponseData.StreamMetadata> list;
        if (exclusive.streamId > 0) {
            list = manager.describeStreamsByStream(exclusive.streamId);
        } else if (exclusive.nodeId > 0) {
            list = manager.describeStreamsByNode(exclusive.nodeId);
        } else {
            list = manager.describeStreamsByTopicPartition(Map.of(exclusive.topicPartition.topicName, exclusive.topicPartition.partitionList));
        }

        if (list.isEmpty()) {
            System.out.println("No stream found.");
            return 0;
        }

        list = list.subList(0, Math.min(maxCount, list.size()));

        if (dryRun) {
            System.out.println("Dry run mode, no stream will be closed.");
            System.out.println("Found following stream(s):");
        } else {
            list.stream()
                .filter(metadata -> StreamState.valueOf(metadata.state()) == StreamState.CLOSED)
                .forEach(metadata -> System.out.println("Node: " + metadata.nodeId() + ", Stream: " + metadata.streamId() + ", Epoch: " + metadata.epoch() + " will not be closed."));
            System.out.println();
            System.out.println("Force close " + list.size() + " stream(s)...");
        }

        for (DescribeStreamsResponseData.StreamMetadata metadata : list) {
            if (dryRun) {
                System.out.println(metadata);
            } else {
                if (StreamState.valueOf(metadata.state()) == StreamState.OPENED) {
                    manager.closeStream(metadata.streamId(), metadata.epoch(), metadata.nodeId());
                    System.out.println("Node: " + metadata.nodeId() + ", Stream: " + metadata.streamId() + ", Epoch: " + metadata.epoch() + " closed.");
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
}
