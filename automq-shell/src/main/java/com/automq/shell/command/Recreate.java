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
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import picocli.CommandLine;

@CommandLine.Command(name = "recreate", description = "Discard all data and recreate partition.", mixinStandardHelpOptions = true)
public class Recreate implements Callable<Integer> {
    @CommandLine.ParentCommand
    AutoMQCLI cli;

    @CommandLine.Option(names = {"-t", "--namespace"}, description = "The stream namespace.")
    String namespace;

    @CommandLine.Option(names = {"-t", "--topic-name"}, description = "The topic you want to close.", required = true)
    String topicName;

    @CommandLine.Option(names = {"-p", "--partition"}, description = "The partition you want to close.", required = true, paramLabel = "<partition>")
    List<Integer> partitionList;

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cli.bootstrapServer);
        Admin admin = Admin.create(properties);

        Optional<Node> nodeOptional = findAnyNode(admin);
        if (nodeOptional.isEmpty()) {
            System.err.println("No controller node found.");
            return 1;
        }

        NetworkClient client = CLIUtils.buildNetworkClient("automq-cli", new AdminClientConfig(new Properties()), new Metrics(), Time.SYSTEM, new LogContext());
        ClientKVClient clientKVClient = new ClientKVClient(client, nodeOptional.get());

        if (StringUtils.isBlank(namespace)) {
            String clusterId = admin.describeCluster().clusterId().get();
            namespace = "_kafka_" + clusterId;
        }

        Uuid topicId = admin.describeTopics(List.of(topicName)).topicNameValues().get(topicName).get().topicId();

        for (Integer partition : partitionList) {
            String streamKey = formatStreamKey(namespace, topicId.toString(), partition);
            clientKVClient.delKV(KeyValue.Key.of(streamKey));
        }

        return 0;
    }

    private String formatStreamKey(String namespace, String topicId, int partition) {
        return namespace + "/" + topicId + "/" + partition;
    }

    private Optional<Node> findAnyNode(Admin admin) throws Exception {
        return admin.describeCluster().nodes().get().stream().findFirst();
    }
}
