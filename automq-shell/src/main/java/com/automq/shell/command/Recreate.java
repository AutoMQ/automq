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
import com.automq.shell.stream.ClientKVClient;
import com.automq.shell.util.CLIUtils;
import com.automq.stream.api.KeyValue;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import picocli.CommandLine;

@CommandLine.Command(name = "recreate", description = "Discard all data and recreate partition(s).", mixinStandardHelpOptions = true)
public class Recreate implements Callable<Integer> {
    @CommandLine.ParentCommand
    AutoMQCLI cli;

    @CommandLine.Option(names = {"-n", "--namespace"}, description = "The stream namespace.")
    String namespace;

    @CommandLine.Option(names = {"-t", "--topic-name"}, description = "The topic you want to recreate.", required = true)
    String topicName;

    @CommandLine.Option(names = {"-p", "--partition"}, description = "The partition you want to recreate.", required = true, paramLabel = "<partition>")
    List<Integer> partitionList;

    @CommandLine.Option(names = {"-f", "--force"}, description = "Confirm recreate the partition(s). @|bold,yellow This will result in the loss all data.|@")
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
            System.err.println("No Kafka node found.");
            return 1;
        }

        NetworkClient client = CLIUtils.buildNetworkClient("automq-cli", new AdminClientConfig(properties), new Metrics(), Time.SYSTEM, new LogContext());
        ClientKVClient clientKVClient = new ClientKVClient(client, nodeOptional.get());

        if (StringUtils.isBlank(namespace)) {
            String clusterId = admin.describeCluster().clusterId().get();
            namespace = "_kafka_" + clusterId;
        }

        TopicDescription topicDescription;
        try {
            topicDescription = admin.describeTopics(List.of(topicName)).topicNameValues().get(topicName).get();
        } catch (UnknownTopicOrPartitionException e) {
            System.err.println("Topic " + topicName + " not found.");
            return 1;
        } catch (Exception e) {
            System.err.println("Failed to describe topic " + topicName + ": " + ExceptionUtils.getRootCauseMessage(e));
            return 1;
        }

        Uuid topicId = topicDescription.topicId();

        String info;
        if (!force) {
            info = CommandLine.Help.Ansi.AUTO.string("@|bold Dry run mode, no partition will be recreated.|@");
        } else {
            info = CommandLine.Help.Ansi.AUTO.string("@|bold,yellow Remove the metadata stream related to specified partition, all data will be lost.|@");
        }
        System.out.println(info);

        for (Integer partition : partitionList) {
            String streamKey = formatStreamKey(namespace, topicId.toString(), partition);
            KeyValue.Value value;
            if (force) {
                value = clientKVClient.deleteKV(streamKey);
            } else {
                value = clientKVClient.getKV(streamKey);
            }

            if (value == null || value.get() == null || value.get().remaining() < Long.BYTES) {
                System.out.println("No stream found for partition " + topicName + "-" + partition);
                continue;
            }
            System.out.println("The stream id related to partition " + topicName + "-" + partition + " is " + value.get().getLong());
        }

        return 0;
    }

    private String formatStreamKey(String namespace, String topicId, int partition) {
        return namespace + "/" + topicId + "/" + partition;
    }
}
