/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.shell.commands.cluster;

import com.automq.shell.model.ClusterTopology;
import com.automq.shell.model.Env;
import com.automq.shell.model.Node;
import com.automq.stream.s3.Constants;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.utils.PingS3Helper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.utils.Exit;
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
            properties.load(new StringReader(topo.getGlobal().getConfigs()));
            String dataBuckets = properties.getProperty("s3.data.buckets");
            String opsBuckets = properties.getProperty("s3.ops.buckets");
            if (StringUtils.isBlank(dataBuckets) || StringUtils.isBlank(opsBuckets)) {
                System.err.println("The s3.data.buckets and s3.ops.buckets are required.");
                Exit.exit(-1);
            }
            String globalAccessKey = null;
            String globalSecretKey = null;
            for (Env env : topo.getGlobal().getEnvs()) {
                if ("KAFKA_S3_ACCESS_KEY".equals(env.getName())) {
                    globalAccessKey = env.getValue();
                } else if ("KAFKA_S3_SECRET_KEY".equals(env.getName())) {
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
                PingS3Helper.builder().bucket(bucket).build().pingS3();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static String genServerStartupCmd(ClusterTopology topo, Node node) {
        StringBuilder sb = new StringBuilder();
        appendEnvs(sb, topo);
        sb.append("./bin/kafka-server-start.sh config/kraft/server.properties ");
        appendCommonConfigsOverride(sb, topo, node);
        appendExtConfigsOverride(sb, topo.getGlobal().getConfigs());
        return sb.toString();
    }

    private static String genBrokerStartupCmd(ClusterTopology topo, Node node) {
        StringBuilder sb = new StringBuilder();
        appendEnvs(sb, topo);
        sb.append("./bin/kafka-server-start.sh config/kraft/broker.properties ");
        appendCommonConfigsOverride(sb, topo, node);
        appendExtConfigsOverride(sb, topo.getGlobal().getConfigs());
        return sb.toString();
    }

    private static void appendEnvs(StringBuilder sb, ClusterTopology topo) {
        topo.getGlobal().getEnvs().forEach(env -> sb.append(env.getName()).append("=").append(env.getValue()).append(" "));
    }

    private static void appendCommonConfigsOverride(StringBuilder sb, ClusterTopology topo, Node node) {
        if (node.getNodeId() == Constants.NOOP_NODE_ID) {
            throw new IllegalArgumentException(String.format("The host[%s]'s nodeId is required", node.getHost()));
        }
        sb.append("--override cluster.id=").append(topo.getGlobal().getClusterId()).append(" ");
        sb.append("--override node.id=").append(node.getNodeId()).append(" ");
        sb.append("--override controller.quorum.voters=").append(getQuorumVoters(topo)).append(" ");
        sb.append("--override advertised.listener=").append(node.getHost()).append(":9092").append(" ");
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
}
