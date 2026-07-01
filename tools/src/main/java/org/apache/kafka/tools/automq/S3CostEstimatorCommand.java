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

package org.apache.kafka.tools.automq;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.tools.automq.cost.ClusterStorageInfo;
import org.apache.kafka.tools.automq.cost.CostCalculator;
import org.apache.kafka.tools.automq.cost.CostEstimatorConfig;
import org.apache.kafka.tools.automq.cost.CostReportPrinter;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * AutoMQ S3 Cost Estimator CLI tool.
 * <p>
 * Connects to a running AutoMQ cluster via the Admin API, collects topic and
 * storage metadata, and prints an estimated monthly cost breakdown for:
 * <ul>
 *   <li>S3 storage</li>
 *   <li>S3 PUT requests (WAL object uploads)</li>
 *   <li>S3 GET requests (consumer fetches)</li>
 * </ul>
 * It also computes the equivalent Apache Kafka (EBS-backed) monthly cost so
 * users can immediately see their cloud cost savings.
 * <p>
 * Entry point: {@code bin/automq-cost-estimator.sh}
 */
public class S3CostEstimatorCommand {

    public static void main(String[] args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (ArgumentParserException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = buildParser();
        Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            Exit.exit(0);
            return;
        }

        CostEstimatorConfig config = buildConfig(ns);
        Properties adminProps = buildAdminProperties(config);

        try (Admin admin = Admin.create(adminProps)) {
            ClusterStorageInfo info = collectClusterInfo(admin, config);
            CostCalculator.CostReport report = new CostCalculator().calculate(info, config);
            new CostReportPrinter(System.out).print(info, report, config);
        }
    }

    // -----------------------------------------------------------------------
    // Cluster data collection
    // -----------------------------------------------------------------------

    /**
     * Collects cluster metadata and storage information from the Admin API.
     *
     * @param admin  a connected Admin client
     * @param config tool configuration controlling which data to collect
     * @return an immutable {@link ClusterStorageInfo} snapshot
     */
    static ClusterStorageInfo collectClusterInfo(Admin admin,
        CostEstimatorConfig config) throws Exception {

        // 1. Cluster identity and broker list
        String clusterId = admin.describeCluster().clusterId().get();
        Set<Integer> brokerIds = admin.describeCluster().nodes().get()
            .stream().map(Node::id).collect(Collectors.toSet());

        // 2. Topic list (exclude internal topics like __consumer_offsets)
        Set<String> topicNames = admin.listTopics().names().get();

        // 3. Describe topics to count total partitions
        DescribeTopicsResult describeTopics = admin.describeTopics(new ArrayList<>(topicNames));
        Map<String, TopicDescription> topicDescriptions = describeTopics.allTopicNames().get();

        int partitionCount = topicDescriptions.values().stream()
            .mapToInt(td -> td.partitions().size())
            .sum();

        // 4. Describe log dirs to estimate storage sizes
        DescribeLogDirsResult logDirsResult = admin.describeLogDirs(brokerIds);
        Map<Integer, Map<String, LogDirDescription>> logDirsByBroker =
            logDirsResult.allDescriptions().get();

        // Aggregate per-topic bytes (de-duplicate replicas by keeping the first seen per partition)
        Map<TopicPartition, Long> partitionBytesMap = new HashMap<>();
        for (Map<String, LogDirDescription> logDirs : logDirsByBroker.values()) {
            for (LogDirDescription logDir : logDirs.values()) {
                for (Map.Entry<TopicPartition, ReplicaInfo> entry
                    : logDir.replicaInfos().entrySet()) {
                    // Only count each partition once (avoid triple-counting replicas)
                    partitionBytesMap.putIfAbsent(entry.getKey(), entry.getValue().size());
                }
            }
        }

        long totalLogBytes = partitionBytesMap.values().stream()
            .mapToLong(Long::longValue).sum();

        // Per-topic aggregation (only computed when --per-topic is set)
        Map<String, Long> topicBytes = Collections.emptyMap();
        if (config.perTopic) {
            Map<String, Long> topicBytesMap = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : partitionBytesMap.entrySet()) {
                topicBytesMap.merge(entry.getKey().topic(), entry.getValue(), Long::sum);
            }
            topicBytes = topicBytesMap;
        }

        return ClusterStorageInfo.builder()
            .clusterId(clusterId)
            .brokerCount(brokerIds.size())
            .topicCount(topicNames.size())
            .partitionCount(partitionCount)
            .totalLogBytes(totalLogBytes)
            .topicBytes(topicBytes)
            .build();
    }

    // -----------------------------------------------------------------------
    // CLI argument parsing
    // -----------------------------------------------------------------------

    private static ArgumentParser buildParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("automq-cost-estimator")
            .defaultHelp(true)
            .description("Estimates the monthly S3 cost of your AutoMQ cluster and compares it\n"
                + "to the equivalent Apache Kafka (EBS-backed) deployment cost.\n\n"
                + "Storage sizes are approximated from WAL log-dir data reported by each broker.\n"
                + "In a fully diskless deployment, bulk data lives in S3; treat the output as\n"
                + "a lower-bound estimate and see the disclaimer in the report footer.");

        // Connection
        parser.addArgument("-B", "--bootstrap-server")
            .action(store())
            .required(true)
            .dest("bootstrapServer")
            .metavar("HOST:PORT")
            .help("Bootstrap server address of the AutoMQ cluster.");

        parser.addArgument("--config", "-c")
            .action(store())
            .dest("configFile")
            .metavar("FILE")
            .help("Path to a properties file with additional Admin client configuration.");

        // S3 pricing
        parser.addArgument("--storage-price")
            .action(store())
            .type(Double.class)
            .setDefault(0.023)
            .dest("storagePricePerGib")
            .metavar("USD")
            .help("S3 storage price per GiB per month (default: $0.023, AWS us-east-1).");

        parser.addArgument("--put-price")
            .action(store())
            .type(Double.class)
            .setDefault(0.000005)
            .dest("putPricePerRequest")
            .metavar("USD")
            .help("S3 PUT request price per request (default: $0.000005).");

        parser.addArgument("--get-price")
            .action(store())
            .type(Double.class)
            .setDefault(0.0000004)
            .dest("getPricePerRequest")
            .metavar("USD")
            .help("S3 GET request price per request (default: $0.0000004).");

        // Kafka comparison
        parser.addArgument("--ebs-price")
            .action(store())
            .type(Double.class)
            .setDefault(0.08)
            .dest("ebsPricePerGib")
            .metavar("USD")
            .help("EBS gp3 price per GiB per month used for Kafka comparison (default: $0.08).");

        parser.addArgument("--replication-factor")
            .action(store())
            .type(Integer.class)
            .setDefault(3)
            .dest("kafkaReplicationFactor")
            .metavar("N")
            .help("Replication factor to simulate for the Kafka EBS cost (default: 3).");

        parser.addArgument("--overprovision-factor")
            .action(store())
            .type(Double.class)
            .setDefault(2.0)
            .dest("overprovisionFactor")
            .metavar("MULTIPLIER")
            .help("Over-provisioning multiplier for the Kafka EBS size (default: 2.0).");

        // Output
        parser.addArgument("--output")
            .action(store())
            .choices("text", "json")
            .setDefault("text")
            .dest("outputFormat")
            .help("Output format: 'text' (default) or 'json'.");

        parser.addArgument("--per-topic")
            .action(storeTrue())
            .dest("perTopic")
            .help("Print a per-topic storage cost breakdown in addition to the cluster total.");

        return parser;
    }

    private static CostEstimatorConfig buildConfig(Namespace ns) {
        return CostEstimatorConfig.builder(ns.getString("bootstrapServer"))
            .configFile(ns.getString("configFile"))
            .storagePricePerGib(ns.getDouble("storagePricePerGib"))
            .putPricePerRequest(ns.getDouble("putPricePerRequest"))
            .getPricePerRequest(ns.getDouble("getPricePerRequest"))
            .ebsPricePerGib(ns.getDouble("ebsPricePerGib"))
            .kafkaReplicationFactor(ns.getInt("kafkaReplicationFactor"))
            .overprovisionFactor(ns.getDouble("overprovisionFactor"))
            .outputFormat(ns.getString("outputFormat"))
            .perTopic(ns.getBoolean("perTopic"))
            .build();
    }

    private static Properties buildAdminProperties(CostEstimatorConfig config) throws IOException {
        Properties props = new Properties();
        if (config.configFile != null) {
            props.putAll(Utils.loadProps(config.configFile));
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer);
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "automq-cost-estimator");
        props.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        return props;
    }

    // Expose for testing
    static List<String> filterInternalTopics(Set<String> topics) {
        return topics.stream()
            .filter(t -> !t.startsWith("__"))
            .collect(Collectors.toList());
    }
}
