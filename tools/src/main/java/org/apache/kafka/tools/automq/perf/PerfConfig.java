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

package org.apache.kafka.tools.automq.perf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class PerfConfig {
    final String bootstrapServer;
    final Map<String, String> topicConfigs;
    final Map<String, String> producerConfigs;
    final Map<String, String> consumerConfigs;
    final String topicPrefix;
    final int topics;
    final int partitionsPerTopic;
    final int producersPerTopic;
    final int groupsPerTopic;
    final int consumersPerGroup;
    final int recordSize;
    final int sendRate;
    final int warmupDurationMinutes;
    final int testDurationMinutes;
    final int reportingIntervalSeconds;

    public PerfConfig(String[] args) {
        ArgumentParser parser = parser();

        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        bootstrapServer = ns.getString("bootstrapServer");
        topicConfigs = parseConfigs(ns.getList("topicConfigs"));
        producerConfigs = parseConfigs(ns.getList("producerConfigs"));
        consumerConfigs = parseConfigs(ns.getList("consumerConfigs"));
        topicPrefix = ns.getString("topicPrefix");
        topics = ns.getInt("topics");
        partitionsPerTopic = ns.getInt("partitionsPerTopic");
        producersPerTopic = ns.getInt("producersPerTopic");
        groupsPerTopic = ns.getInt("groupsPerTopic");
        consumersPerGroup = ns.getInt("consumersPerGroup");
        recordSize = ns.getInt("recordSize");
        sendRate = ns.getInt("sendRate");
        warmupDurationMinutes = ns.getInt("warmupDurationMinutes");
        testDurationMinutes = ns.getInt("testDurationMinutes");
        reportingIntervalSeconds = ns.getInt("reportingIntervalSeconds");
    }

    public static ArgumentParser parser() {
        ArgumentParser parser = ArgumentParsers
            .newFor("performance-test")
            .build()
            .defaultHelp(true)
            .description("This tool is used to run performance tests.");
        parser.addArgument("-B", "--bootstrap-server")
            .setDefault("localhost:9092")
            .type(String.class)
            .dest("bootstrapServer")
            .metavar("BOOTSTRAP_SERVER")
            .help("The AutoMQ bootstrap server.");
        parser.addArgument("-T", "--topic-configs")
            .nargs("*")
            .type(String.class)
            .dest("topicConfigs")
            .metavar("TOPIC_CONFIG")
            .help("The topic configurations.");
        parser.addArgument("-P", "--producer-configs")
            .nargs("*")
            .type(String.class)
            .dest("producerConfigs")
            .metavar("PRODUCER_CONFIG")
            .help("The producer configurations.");
        parser.addArgument("-C", "--consumer-configs")
            .nargs("*")
            .type(String.class)
            .dest("consumerConfigs")
            .metavar("CONSUMER_CONFIG")
            .help("The consumer configurations.");
        parser.addArgument("-X", "--topic-prefix")
            .setDefault("test-topic")
            .type(String.class)
            .dest("topicPrefix")
            .metavar("TOPIC_PREFIX")
            .help("The topic prefix.");
        parser.addArgument("-t", "--topics")
            .setDefault(1)
            .type(Integer.class)
            .dest("topics")
            .metavar("TOPICS")
            .help("The number of topics.");
        parser.addArgument("-n", "--partitions-per-topic")
            .setDefault(1)
            .type(Integer.class)
            .dest("partitionsPerTopic")
            .metavar("PARTITIONS_PER_TOPIC")
            .help("The number of partitions per topic.");
        parser.addArgument("-p", "--producers-per-topic")
            .setDefault(1)
            .type(Integer.class)
            .dest("producersPerTopic")
            .metavar("PRODUCERS_PER_TOPIC")
            .help("The number of producers per topic.");
        parser.addArgument("-g", "--groups-per-topic")
            .setDefault(1)
            .type(Integer.class)
            .dest("groupsPerTopic")
            .metavar("GROUPS_PER_TOPIC")
            .help("The number of consumer groups per topic.");
        parser.addArgument("-c", "--consumers-per-group")
            .setDefault(1)
            .type(Integer.class)
            .dest("consumersPerGroup")
            .metavar("CONSUMERS_PER_GROUP")
            .help("The number of consumers per group.");
        parser.addArgument("-s", "--record-size")
            .setDefault(1024)
            .type(Integer.class)
            .dest("recordSize")
            .metavar("RECORD_SIZE")
            .help("The record size in bytes.");
        parser.addArgument("-r", "--send-rate")
            .setDefault(1000)
            .type(Integer.class)
            .dest("sendRate")
            .metavar("SEND_RATE")
            .help("The send rate in messages per second.");
        parser.addArgument("-w", "--warmup-duration")
            .setDefault(1)
            .type(Integer.class)
            .dest("warmupDurationMinutes")
            .metavar("WARMUP_DURATION_MINUTES")
            .help("The warmup duration in minutes.");
        parser.addArgument("-d", "--test-duration")
            .setDefault(5)
            .type(Integer.class)
            .dest("testDurationMinutes")
            .metavar("TEST_DURATION_MINUTES")
            .help("The test duration in minutes.");
        parser.addArgument("-i", "--reporting-interval")
            .setDefault(1)
            .type(Integer.class)
            .dest("reportingIntervalSeconds")
            .metavar("REPORTING_INTERVAL_SECONDS")
            .help("The reporting interval in seconds.");
        return parser;
    }

    public String bootstrapServer() {
        return bootstrapServer;
    }

    public TopicService.TopicsConfig topicsConfig() {
        return new TopicService.TopicsConfig(
            topicPrefix,
            topics,
            partitionsPerTopic,
            topicConfigs
        );
    }

    private Map<String, String> parseConfigs(List<String> configs) {
        Map<String, String> map = new HashMap<>();
        for (String config : configs) {
            String[] parts = config.split("=");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid config: " + config);
            }
            map.put(parts[0], parts[1]);
        }
        return map;
    }
}
