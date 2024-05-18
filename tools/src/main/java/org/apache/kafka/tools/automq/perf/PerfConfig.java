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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.tools.automq.perf.ConsumerService.ConsumersConfig;
import org.apache.kafka.tools.automq.perf.ProducerService.ProducersConfig;
import org.apache.kafka.tools.automq.perf.TopicService.TopicsConfig;

public class PerfConfig {
    public final String bootstrapServer;
    public final Map<String, String> topicConfigs;
    public final Map<String, String> producerConfigs;
    public final Map<String, String> consumerConfigs;
    public final String topicPrefix;
    public final int topics;
    public final int partitionsPerTopic;
    public final int producersPerTopic;
    public final int groupsPerTopic;
    public final int consumersPerGroup;
    public final int recordSize;
    public final double randomRatio;
    public final int randomPoolSize;
    public final int sendRate;
    public final int backlogDurationSeconds;
    public final int groupStartDelaySeconds;
    public final int warmupDurationMinutes;
    public final int testDurationMinutes;
    public final int reportingIntervalSeconds;

    public PerfConfig(String[] args) {
        ArgumentParser parser = parser();

        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            Exit.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
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
        randomRatio = ns.getDouble("randomRatio");
        randomPoolSize = ns.getInt("randomPoolSize");
        sendRate = ns.getInt("sendRate");
        backlogDurationSeconds = ns.getInt("backlogDurationSeconds");
        groupStartDelaySeconds = ns.getInt("groupStartDelaySeconds");
        warmupDurationMinutes = ns.getInt("warmupDurationMinutes");
        testDurationMinutes = ns.getInt("testDurationMinutes");
        reportingIntervalSeconds = ns.getInt("reportingIntervalSeconds");

        // TODO add more checker

        if (backlogDurationSeconds <= producersPerTopic * groupStartDelaySeconds) {
            throw new IllegalArgumentException(String.format("BACKLOG_DURATION_SECONDS(%d) should be greater than PRODUCERS_PER_TOPIC(%d) * GROUP_START_DELAY_SECONDS(%d)",
                backlogDurationSeconds, producersPerTopic, groupStartDelaySeconds));
        }
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
        parser.addArgument("-R", "--random-ratio")
            .setDefault(0.0)
            .type(Double.class)
            .dest("randomRatio")
            .metavar("RANDOM_RATIO")
            .help("The ratio of random payloads. The value should be between 0.0 and 1.0.");
        parser.addArgument("-S", "--random-pool-size")
            .setDefault(1000)
            .type(Integer.class)
            .dest("randomPoolSize")
            .metavar("RANDOM_POOL_SIZE")
            .help("The count of random payloads. Only used when random ratio is greater than 0.0.");
        parser.addArgument("-r", "--send-rate")
            .setDefault(1000)
            .type(Integer.class)
            .dest("sendRate")
            .metavar("SEND_RATE")
            .help("The send rate in messages per second.");
        parser.addArgument("-b", "--backlog-duration")
            .setDefault(500)
            .type(Integer.class)
            .dest("backlogDurationSeconds")
            .metavar("BACKLOG_DURATION_SECONDS")
            .help("The backlog duration in seconds. Should be greater than PRODUCERS_PER_TOPIC * GROUP_START_DELAY_SECONDS.");
        parser.addArgument("-G", "--group-start-delay")
            .setDefault(0)
            .type(Integer.class)
            .dest("groupStartDelaySeconds")
            .metavar("GROUP_START_DELAY_SECONDS")
            .help("The group start delay in seconds.");
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

    public TopicsConfig topicsConfig() {
        return new TopicsConfig(
            topicPrefix,
            topics,
            partitionsPerTopic,
            topicConfigs
        );
    }

    public ProducersConfig producersConfig() {
        return new ProducersConfig(
            bootstrapServer,
            producersPerTopic,
            producerConfigs
        );
    }

    public ConsumersConfig consumersConfig() {
        return new ConsumersConfig(
            bootstrapServer,
            groupsPerTopic,
            consumersPerGroup,
            consumerConfigs
        );
    }

    private Map<String, String> parseConfigs(List<String> configs) {
        if (configs == null) {
            return new HashMap<>();
        }
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
