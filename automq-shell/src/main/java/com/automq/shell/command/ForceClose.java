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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DescribeStreamsResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "force-close", description = "Stream lossy failure recovery operation.", mixinStandardHelpOptions = true)
public class ForceClose implements Callable<Integer> {
    @ParentCommand
    AutoMQCLI cli;

    @ArgGroup(multiplicity = "1")
    Exclusive exclusive;

    static class TopicPartition {
        @Option(names = {"-t", "--topic-name"}, description = "The topic you want to close.", required = true)
        String topicName;

        @Option(names = {"-p", "--partition"}, description = "The partition you want to close.", required = true, paramLabel = "<partition>")
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

    @Option(names = {"-m", "--max-count"}, description = "Max partition count in a batch.", defaultValue = "1000", paramLabel = "<count>")
    int maxCount;

    @Option(names = {"-f", "--force"}, description = "Confirm force close the stream(s). @|bold,yellow This will result in the loss of uncommitted data.|@")
    boolean force;

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        if (cli.commandConfig != null) {
            try {
                properties = Utils.loadProps(cli.commandConfig);
            } catch (Exception e) {
                System.err.println("Error loading command config file: " + ExceptionUtils.getRootCauseMessage(e));
                return 1;
            }
        }
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cli.bootstrapServer);
        Admin admin = Admin.create(properties);

        Optional<Node> nodeOptional;
        try {
            nodeOptional = admin.describeCluster().nodes().get().stream().findFirst();
        } catch (Exception e) {
            System.err.println("Failed to get Kafka node: " + ExceptionUtils.getRootCauseMessage(e));
            return 1;
        }
        if (nodeOptional.isEmpty()) {
            System.err.println("No controller node found.");
            return 1;
        }

        NetworkClient client = CLIUtils.buildNetworkClient("automq-cli", new AdminClientConfig(properties), new Metrics(), Time.SYSTEM, new LogContext());
        ClientStreamManager manager = new ClientStreamManager(client, nodeOptional.get());

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

        if (force) {
            System.out.println();
            System.out.println("Force close " + list.size() + " stream(s)...");

            for (DescribeStreamsResponseData.StreamMetadata metadata : list) {
                if (StreamState.valueOf(metadata.state()) == StreamState.OPENED) {
                    manager.closeStream(metadata.streamId(), metadata.epoch(), metadata.nodeId());
                    System.out.println("Node: " + metadata.nodeId() + ", Stream: " + metadata.streamId() + ", TopicPartition: " + metadata.topicName() + "-" + metadata.partitionIndex() + " closed.");
                } else {
                    System.out.println("Node: " + metadata.nodeId() + ", Stream: " + metadata.streamId() + ", TopicPartition: " + metadata.topicName() + "-" + metadata.partitionIndex() + " is already closed.");
                }
            }
        } else {
            String info = CommandLine.Help.Ansi.AUTO.string("@|bold Dry run mode, no stream will be closed.|@");
            System.out.println(info);
            System.out.println();
            System.out.println("Found following stream(s):");

            for (DescribeStreamsResponseData.StreamMetadata metadata : list) {
                System.out.println("Node: " + metadata.nodeId() + ", Stream: " + metadata.streamId() + ", TopicPartition: " + metadata.topicName() + "-" + metadata.partitionIndex() + ", State: " + StreamState.valueOf(metadata.state()));
            }

            System.out.println();
            String warning = CommandLine.Help.Ansi.AUTO.string("@|bold,yellow WARNING: Closing the stream forcefully will result in the loss of uncommitted data. To proceed, please confirm by using --force to force close the stream(s).|@");
            System.out.println(warning);
        }

        return 0;
    }
}
