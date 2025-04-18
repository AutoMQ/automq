/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package com.automq.shell.commands.cluster;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GetNodesOptions;
import org.apache.kafka.clients.admin.GetNodesResult;
import org.apache.kafka.clients.admin.NodeMetadata;
import org.apache.kafka.common.utils.Utils;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import picocli.CommandLine;

@CommandLine.Command(name = "describe", description = "Describe the AutoMQ cluster", mixinStandardHelpOptions = true)
public class Describe implements Callable<Integer> {
    @CommandLine.Option(names = {"-b", "--bootstrap-server"}, description = "The Kafka server to connect to.")
    public String bootstrapServer;
    @CommandLine.Option(names = {"-c", "--command-config"}, description = "Property file containing configs to be passed to Admin Client.")
    public String commandConfig;


    @CommandLine.ParentCommand
    Cluster cluster;

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        if (commandConfig != null) {
            try {
                properties = Utils.loadProps(commandConfig);
            } catch (Exception e) {
                System.err.println("Error loading command config file: " + ExceptionUtils.getRootCauseMessage(e));
                return 1;
            }
        }
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        try (Admin admin = Admin.create(properties)) {
            GetNodesResult rst = admin.getNodes(Collections.emptyList(), new GetNodesOptions());
            List<NodeMetadata> nodeMetadataList = rst.nodes().get();
            nodeMetadataList.sort(Comparator.comparingInt(NodeMetadata::getNodeId));
            for (NodeMetadata nodeMetadata : nodeMetadataList) {
                System.out.println(nodeMetadata);
            }
        }
        return 0;
    }
}
