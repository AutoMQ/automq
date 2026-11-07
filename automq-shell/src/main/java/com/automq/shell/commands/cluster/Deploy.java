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

import org.apache.kafka.common.utils.Exit;

import com.automq.shell.model.ClusterTopology;
import com.automq.shell.model.Env;
import com.automq.shell.model.Node;
import com.automq.stream.s3.Constants;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import picocli.CommandLine;

@CommandLine.Command(name = "deploy", description = "Deploy AutoMQ cluster", mixinStandardHelpOptions = true)
public class Deploy implements Callable<Integer> {
    @CommandLine.Parameters(index = "0", description = "cluster project path")
    private String clusterProjectPath;

    @CommandLine.Option(names = "--dry-run", arity = "0", required = true, description = "Dry run mode, print the startup cmd")
    boolean dryRun;

    @Override
    public Integer call() throws Exception {
        String topoPath = clusterProjectPath + File.separator + "topo.yaml";
        if (!Files.exists(Paths.get(topoPath))) {
            System.out.printf("Cannot find %s for cluster[%s]\n", topoPath, clusterProjectPath);
            return 1;
        }
        if (!dryRun) {
            System.out.println("Currently cluster deploy only support --dry-run");
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        ClusterTopology topo = mapper.readValue(new File(topoPath), ClusterTopology.class);
        System.out.println("############  Start Commandline ##############");
        System.out.println("To start an AutoMQ Kafka server, please navigate to the directory where your AutoMQ tgz file is located and run the following command.");
        System.out.println();

        bucketReadinessCheck(topo);

        for (Node node : topo.getControllers()) {
            System.out.println("Host: " + node.getHost());
            System.out.println(genServerStartupCmd(topo, node));
            System.out.println();
        }

        for (Node node : topo.getBrokers()) {
            System.out.println("Host: " + node.getHost());
            System.out.println(genBrokerStartupCmd(topo, node));
            System.out.println();
        }

        System.out.println("Before running the command, make sure that Java 17 is installed on your host. You can verify the Java version by executing 'java -version'.\n");
        System.out.println("TIPS: Start controllers first and then the brokers.\n");
        return 0;
    }

    public static void main(String... args) throws Exception {
        Deploy deploy = new Deploy();
        deploy.clusterProjectPath = "clusters/demo";
        deploy.dryRun = true;
        deploy.call();
    }

    private static void bucketReadinessCheck(ClusterTopology topo) {
        try {
            Properties properties = new Properties();
            properties.load(new StringReader(topo.getGlobal().getConfig()));
            String dataBuckets = properties.getProperty("s3.data.buckets");
            String opsBuckets = properties.getProperty("s3.ops.buckets");
            if (StringUtils.isBlank(dataBuckets) || StringUtils.isBlank(opsBuckets)) {
                System.err.println("The s3.data.buckets and s3.ops.buckets are required.");
                Exit.exit(-1);
            }
            String globalAccessKey = null;
            String globalSecretKey = null;
            for (Env env : topo.getGlobal().getEnvs()) {
                if ("KAFKA_S3_ACCESS_KEY".equals(env.getName()) ||
                    "AWS_ACCESS_KEY_ID".equals(env.getName())) {
                    globalAccessKey = env.getValue();
                } else if ("KAFKA_S3_SECRET_KEY".equals(env.getName()) ||
                            "AWS_SECRET_ACCESS_KEY".equals(env.getName())) {
                    globalSecretKey = env.getValue();
                }
            }
            List<BucketURI> buckets = new LinkedList<>();
            buckets.addAll(BucketURI.parseBuckets(dataBuckets));
            buckets.addAll(BucketURI.parseBuckets(opsBuckets));
            for (BucketURI bucket : buckets) {
                if (StringUtils.isNotBlank(bucket.extensionString(BucketURI.ACCESS_KEY_KEY, null))) {
                    continue;
                }
                if (StringUtils.isNotBlank(globalAccessKey) && StringUtils.isNotBlank(globalSecretKey)) {
                    bucket.addExtension(BucketURI.ACCESS_KEY_KEY, globalAccessKey);
                    bucket.addExtension(BucketURI.SECRET_KEY_KEY, globalSecretKey);
                }
                ObjectStorage objectStorage = ObjectStorageFactory.instance().builder(bucket).build();
                if (!objectStorage.readinessCheck()) {
                    Exit.exit(-1);
                }
                objectStorage.close();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static String genServerStartupCmd(ClusterTopology topo, Node node) {
        StringBuilder sb = new StringBuilder();
        appendEnvs(sb, topo);
        sb.append("./bin/kafka-server-start.sh -daemon config/kraft/server.properties ");
        appendCommonConfigsOverride(sb, topo, node);
        appendExtConfigsOverride(sb, topo.getGlobal().getConfig());
        return sb.toString();
    }

    private static String genBrokerStartupCmd(ClusterTopology topo, Node node) {
        StringBuilder sb = new StringBuilder();
        appendEnvs(sb, topo);
        sb.append("./bin/kafka-server-start.sh -daemon config/kraft/broker.properties ");
        appendCommonConfigsOverride(sb, topo, node);
        appendExtConfigsOverride(sb, topo.getGlobal().getConfig());
        return sb.toString();
    }

    private static void appendEnvs(StringBuilder sb, ClusterTopology topo) {
        topo.getGlobal().getEnvs().forEach(env -> sb.append(env.getName()).append("='").append(env.getValue()).append("' "));
    }

    private static void appendCommonConfigsOverride(StringBuilder sb, ClusterTopology topo, Node node) {
        if (node.getNodeId() == Constants.NOOP_NODE_ID) {
            throw new IllegalArgumentException(String.format("The host[%s]'s nodeId is required", node.getHost()));
        }
        sb.append("--override cluster.id=").append(topo.getGlobal().getClusterId()).append(" ");
        sb.append("--override node.id=").append(node.getNodeId()).append(" ");
        sb.append("--override controller.quorum.voters=").append(getQuorumVoters(topo)).append(" ");
        sb.append("--override controller.quorum.bootstrap.servers=").append(getBootstrapServers(topo)).append(" ");
        sb.append("--override advertised.listeners=").append("PLAINTEXT://").append(node.getHost()).append(":9092").append(" ");
    }

    private static void appendExtConfigsOverride(StringBuilder sb, String rawConfigs) {
        try {
            Properties properties = new Properties();
            properties.load(new StringReader(rawConfigs));
            properties.forEach((k, v) -> sb.append("--override ").append(k).append("='").append(v).append("' "));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static String getQuorumVoters(ClusterTopology topo) {
        List<Node> nodes = topo.getControllers();
        if (!(nodes.size() == 1 || nodes.size() == 3)) {
            throw new IllegalArgumentException("Only support 1 or 3 controllers");
        }
        return nodes.stream()
            .map(node -> node.getNodeId() + "@" + node.getHost() + ":9093")
            .collect(Collectors.joining(","));
    }

    private static String getBootstrapServers(ClusterTopology topo) {
        List<Node> nodes = topo.getControllers();
        if (!(nodes.size() == 1 || nodes.size() == 3)) {
            throw new IllegalArgumentException("Only support 1 or 3 controllers");
        }
        return nodes.stream()
            .map(node -> node.getHost() + ":9093")
            .collect(Collectors.joining(","));
    }
}
