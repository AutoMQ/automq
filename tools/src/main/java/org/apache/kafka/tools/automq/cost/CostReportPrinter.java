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

package org.apache.kafka.tools.automq.cost;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Formats and prints a {@link CostCalculator.CostReport} to a {@link PrintStream}.
 * <p>
 * Two output modes are supported:
 * <ul>
 *   <li><b>text</b> – human-readable table (default)</li>
 *   <li><b>json</b> – machine-readable JSON object</li>
 * </ul>
 */
public class CostReportPrinter {

    private static final String SEPARATOR = "─".repeat(60);
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);

    private final PrintStream out;

    /**
     * Creates a printer that writes to the given stream.
     *
     * @param out the target print stream (e.g. {@code System.out})
     */
    public CostReportPrinter(PrintStream out) {
        this.out = out;
    }

    /**
     * Prints the cost report in the format specified by {@code config.outputFormat}.
     *
     * @param info   cluster facts used as report header context
     * @param report computed cost figures
     * @param config tool configuration (pricing and output format)
     */
    public void print(ClusterStorageInfo info, CostCalculator.CostReport report,
        CostEstimatorConfig config) {
        if ("json".equalsIgnoreCase(config.outputFormat)) {
            printJson(info, report, config);
        } else {
            printText(info, report, config);
        }
    }

    private void printText(ClusterStorageInfo info, CostCalculator.CostReport report,
        CostEstimatorConfig config) {
        out.println();
        out.println("AutoMQ S3 Cost Estimator");
        out.println(SEPARATOR);
        out.printf("  Cluster ID   : %s%n", info.clusterId);
        out.printf("  Broker count : %d%n", info.brokerCount);
        out.printf("  Topics       : %d%n", info.topicCount);
        out.printf("  Partitions   : %d%n", info.partitionCount);
        out.println();

        out.println("Storage Breakdown");
        out.println(SEPARATOR);
        out.printf("  Total data (approx.)  : %s%n", formatGib(report.totalGib));
        out.printf("  S3 storage cost       : %s/month  (@ $%.4f/GiB)%n",
            formatUsd(report.storageCostUsd), config.storagePricePerGib);
        out.printf("  S3 PUT cost           : %s/month  (~%,d PUTs/day, $%.7f/req)%n",
            formatUsd(report.putCostUsd),
            (long) info.partitionCount * config.estimatedPutsPerPartitionPerDay,
            config.putPricePerRequest);
        out.printf("  S3 GET cost           : %s/month  (~%,d GETs/day, $%.7f/req)%n",
            formatUsd(report.getCostUsd),
            (long) info.partitionCount * config.estimatedGetsPerPartitionPerDay,
            config.getPricePerRequest);
        out.println();

        out.printf("  AutoMQ Estimated Total: %s / month%n", formatUsd(report.automqTotalUsd));
        out.println();

        out.println("Equivalent Apache Kafka (EBS) Cost");
        out.println(SEPARATOR);
        out.printf("  EBS size (RF=%d, %.1fx over-provision): %s%n",
            config.kafkaReplicationFactor, config.overprovisionFactor,
            formatGib(report.kafkaEbsGib));
        out.printf("  EBS cost              : %s/month  (@ $%.4f/GiB)%n",
            formatUsd(report.kafkaEbsCostUsd), config.ebsPricePerGib);
        out.println();

        out.println(SEPARATOR);
        out.printf("  Savings vs Kafka      : %s/month  (%.1f%% cheaper)%n",
            formatUsd(report.savingsUsd), report.savingsPct);
        out.println(SEPARATOR);
        out.println();

        if (config.perTopic && !report.perTopicStorageCostUsd.isEmpty()) {
            out.println("Per-Topic Storage Cost Breakdown");
            out.println(SEPARATOR);
            out.printf("  %-45s %12s%n", "Topic", "Cost/month");
            out.printf("  %-45s %12s%n", "-".repeat(45), "-".repeat(12));
            report.perTopicStorageCostUsd.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .forEach(e -> out.printf("  %-45s %12s%n",
                    truncate(e.getKey(), 45), formatUsd(e.getValue())));
            out.println();
        }

        out.println("Note: Prices estimated using AWS us-east-1 defaults.");
        out.println("      Storage size is approximated from WAL log-dir data.");
        out.println("      Use --storage-price, --put-price, --get-price, --ebs-price to override.");
        out.println();
    }

    private void printJson(ClusterStorageInfo info, CostCalculator.CostReport report,
        CostEstimatorConfig config) {
        Map<String, Object> root = new LinkedHashMap<>();

        Map<String, Object> cluster = new LinkedHashMap<>();
        cluster.put("clusterId", info.clusterId);
        cluster.put("brokerCount", info.brokerCount);
        cluster.put("topicCount", info.topicCount);
        cluster.put("partitionCount", info.partitionCount);
        root.put("cluster", cluster);

        Map<String, Object> automq = new LinkedHashMap<>();
        automq.put("totalGib", round(report.totalGib));
        automq.put("storageCostUsd", round(report.storageCostUsd));
        automq.put("putCostUsd", round(report.putCostUsd));
        automq.put("getCostUsd", round(report.getCostUsd));
        automq.put("totalCostUsd", round(report.automqTotalUsd));
        root.put("automq", automq);

        Map<String, Object> kafka = new LinkedHashMap<>();
        kafka.put("ebsGib", round(report.kafkaEbsGib));
        kafka.put("ebsCostUsd", round(report.kafkaEbsCostUsd));
        kafka.put("replicationFactor", config.kafkaReplicationFactor);
        kafka.put("overprovisionFactor", config.overprovisionFactor);
        root.put("kafka", kafka);

        Map<String, Object> savings = new LinkedHashMap<>();
        savings.put("savingsUsd", round(report.savingsUsd));
        savings.put("savingsPct", round(report.savingsPct));
        root.put("savings", savings);

        if (config.perTopic && !report.perTopicStorageCostUsd.isEmpty()) {
            Map<String, Double> perTopic = new LinkedHashMap<>();
            report.perTopicStorageCostUsd.forEach((topic, cost) -> perTopic.put(topic, round(cost)));
            root.put("perTopicStorageCostUsd", perTopic);
        }

        try {
            out.println(MAPPER.writeValueAsString(root));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize cost report to JSON", e);
        }
    }

    // --- formatting helpers ---

    private static String formatGib(double gib) {
        return String.format("%.2f GiB", gib);
    }

    private static String formatUsd(double usd) {
        return String.format("$%,.2f", usd);
    }

    private static double round(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    private static String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) {
            return s;
        }
        return s.substring(0, maxLen - 1) + "…";
    }
}
