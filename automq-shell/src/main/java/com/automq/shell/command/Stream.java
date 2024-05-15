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
import com.automq.stream.s3.metadata.StreamMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import com.automq.shell.util.CLIUtils;
import com.automq.shell.stream.ClientStreamManager;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import picocli.CommandLine;

@CommandLine.Command(name = "stream", description = "Metadata operations.")
public class Stream {
    @CommandLine.ParentCommand
    AutoMQCLI cli;

    @SuppressWarnings("unused")
    @CommandLine.Command(description = "Get stream metadata from specified node.")
    public int get(
        @CommandLine.Option(names = {"-n", "--node-id"}, description = "The Kafka node id.", required = true)
        int nodeId
    ) throws Exception {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cli.bootstrapServer);
        Admin admin = Admin.create(properties);

        DescribeClusterResult result = admin.describeCluster();
        Collection<Node> nodes = result.nodes().get();

        Optional<Node> nodeOptional = nodes.stream().filter(node -> node.id() == nodeId).findFirst();
        if (nodeOptional.isEmpty()) {
            System.out.println("Node not found.");
            return 1;
        }

        NetworkClient client = CLIUtils.buildNetworkClient("automq-cli", new AdminClientConfig(new Properties()), new Metrics(), Time.SYSTEM, new LogContext());
        ClientStreamManager manager = new ClientStreamManager(client);
        List<StreamMetadata> metadataList = manager.getOpeningStreams(nodeOptional.get(), System.currentTimeMillis(), false);
        for (StreamMetadata metadata : metadataList) {
            System.out.println(metadata);
        }
        return 0;
    }
}
