/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.tools.automq.perf;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.tools.automq.perf.ConsumerService.ConsumersConfig;
import org.apache.kafka.tools.automq.perf.ProducerService.ProducersConfig;
import org.apache.kafka.tools.automq.perf.TopicService.TopicsConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.ReflectArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;
import static org.apache.kafka.tools.automq.perf.PerfConfig.IntegerArgumentType.between;
import static org.apache.kafka.tools.automq.perf.PerfConfig.IntegerArgumentType.nonNegativeInteger;
import static org.apache.kafka.tools.automq.perf.PerfConfig.IntegerArgumentType.notLessThan;
import static org.apache.kafka.tools.automq.perf.PerfConfig.IntegerArgumentType.positiveInteger;

public class PerfConfig {
    public final String bootstrapServer;
    public final Properties commonConfigs;
    public final Map<String, String> adminConfigs;
    public final Map<String, String> topicConfigs;
    public final Map<String, String> producerConfigs;
    public final Map<String, String> consumerConfigs;
    public final boolean reset;
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
    public final int sendRateDuringCatchup;
    public final int maxConsumeRecordRate;
    public final int backlogDurationSeconds;
    public final int groupStartDelaySeconds;
    public final int warmupDurationMinutes;
    public final int testDurationMinutes;
    public final int reportingIntervalSeconds;
    public final String valueSchema;
    public final String valuesFile;

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
        assert ns != null;

        bootstrapServer = ns.getString("bootstrapServer");
        commonConfigs = ns.getString("commonConfigFile") == null ? new Properties() : loadProperties(ns.getString("commonConfigFile"));
        adminConfigs = parseConfigs(ns.getList("adminConfigs"));
        topicConfigs = parseConfigs(ns.getList("topicConfigs"));
        producerConfigs = parseConfigs(ns.getList("producerConfigs"));
        consumerConfigs = parseConfigs(ns.getList("consumerConfigs"));
        reset = ns.getBoolean("reset");
        topicPrefix = ns.getString("topicPrefix") == null ? randomTopicPrefix() : ns.getString("topicPrefix");
        topics = ns.getInt("topics");
        partitionsPerTopic = ns.getInt("partitionsPerTopic");
        producersPerTopic = ns.getInt("producersPerTopic");
        groupsPerTopic = ns.getInt("groupsPerTopic");
        consumersPerGroup = ns.getInt("consumersPerGroup");
        recordSize = ns.getInt("recordSize");
        randomRatio = ns.getDouble("randomRatio");
        randomPoolSize = ns.getInt("randomPoolSize");
        sendRate = ns.getInt("sendRate");
        sendRateDuringCatchup = ns.getInt("sendRateDuringCatchup") == null ? sendRate : ns.getInt("sendRateDuringCatchup");
        maxConsumeRecordRate = ns.getInt("maxConsumeRecordRate");
        backlogDurationSeconds = ns.getInt("backlogDurationSeconds");
        groupStartDelaySeconds = ns.getInt("groupStartDelaySeconds");
        warmupDurationMinutes = ns.getInt("warmupDurationMinutes");
        testDurationMinutes = ns.getInt("testDurationMinutes");
        reportingIntervalSeconds = ns.getInt("reportingIntervalSeconds");
        valueSchema = ns.getString("valueSchema");
        valuesFile = ns.get("valuesFile");

        if (backlogDurationSeconds < groupsPerTopic * groupStartDelaySeconds) {
            throw new IllegalArgumentException(String.format("BACKLOG_DURATION_SECONDS(%d) should not be less than GROUPS_PER_TOPIC(%d) * GROUP_START_DELAY_SECONDS(%d)",
                backlogDurationSeconds, groupsPerTopic, groupStartDelaySeconds));
        }
    }

    public static ArgumentParser parser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("performance-test")
            .defaultHelp(true)
            .description("This tool is used to run performance tests.");
        parser.addArgument("-B", "--bootstrap-server")
            .setDefault("localhost:9092")
            .type(String.class)
            .dest("bootstrapServer")
            .metavar("BOOTSTRAP_SERVER")
            .help("The AutoMQ bootstrap server.");
        parser.addArgument("-F", "--common-config-file")
            .type(String.class)
            .dest("commonConfigFile")
            .metavar("COMMON_CONFIG_FILE")
            .help("The property file containing common configurations to be passed to all clients —— producer, consumer, and admin.");
        parser.addArgument("-A", "--admin-configs")
            .nargs("*")
            .type(String.class)
            .dest("adminConfigs")
            .metavar("ADMIN_CONFIG")
            .help("The admin client configurations.");
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
        parser.addArgument("--reset")
            .action(storeTrue())
            .dest("reset")
            .help("delete all topics before running the test.");
        parser.addArgument("-X", "--topic-prefix")
            .type(String.class)
            .dest("topicPrefix")
            .metavar("TOPIC_PREFIX")
            .help("The topic prefix.");
        parser.addArgument("-t", "--topics")
            .setDefault(1)
            .type(positiveInteger())
            .dest("topics")
            .metavar("TOPICS")
            .help("The number of topics.");
        parser.addArgument("-n", "--partitions-per-topic")
            .setDefault(1)
            .type(positiveInteger())
            .dest("partitionsPerTopic")
            .metavar("PARTITIONS_PER_TOPIC")
            .help("The number of partitions per topic.");
        parser.addArgument("-p", "--producers-per-topic")
            .setDefault(1)
            .type(positiveInteger())
            .dest("producersPerTopic")
            .metavar("PRODUCERS_PER_TOPIC")
            .help("The number of producers per topic.");
        parser.addArgument("-g", "--groups-per-topic")
            .setDefault(1)
            .type(nonNegativeInteger())
            .dest("groupsPerTopic")
            .metavar("GROUPS_PER_TOPIC")
            .help("The number of consumer groups per topic.");
        parser.addArgument("-c", "--consumers-per-group")
            .setDefault(1)
            .type(positiveInteger())
            .dest("consumersPerGroup")
            .metavar("CONSUMERS_PER_GROUP")
            .help("The number of consumers per group.");
        parser.addArgument("-s", "--record-size")
            .setDefault(1024)
            .type(positiveInteger())
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
            .type(positiveInteger())
            .dest("randomPoolSize")
            .metavar("RANDOM_POOL_SIZE")
            .help("The count of random payloads. Only used when random ratio is greater than 0.0.");
        parser.addArgument("-r", "--send-rate")
            .setDefault(1000)
            .type(positiveInteger())
            .dest("sendRate")
            .metavar("SEND_RATE")
            .help("The send rate in messages per second.");
        parser.addArgument("-a", "--send-rate-during-catchup")
            .type(positiveInteger())
            .dest("sendRateDuringCatchup")
            .metavar("SEND_RATE_DURING_CATCHUP")
            .help("The send rate in messages per second during catchup. If not set, the send rate will be used.");
        parser.addArgument("-m", "--max-consume-record-rate")
            .setDefault(1_000_000_000)
            .type(between(0, 1_000_000_000))
            .dest("maxConsumeRecordRate")
            .metavar("MAX_CONSUME_RECORD_RATE")
            .help("The max rate of consuming records per second.");
        parser.addArgument("-b", "--backlog-duration")
            .setDefault(0)
            .type(notLessThan(300))
            .dest("backlogDurationSeconds")
            .metavar("BACKLOG_DURATION_SECONDS")
            .help("The backlog duration in seconds, and zero means no backlog. Should not be less than GROUPS_PER_TOPIC * GROUP_START_DELAY_SECONDS.");
        parser.addArgument("-G", "--group-start-delay")
            .setDefault(0)
            .type(nonNegativeInteger())
            .dest("groupStartDelaySeconds")
            .metavar("GROUP_START_DELAY_SECONDS")
            .help("The group start delay in seconds.");
        parser.addArgument("-w", "--warmup-duration")
            .setDefault(1)
            .type(nonNegativeInteger())
            .dest("warmupDurationMinutes")
            .metavar("WARMUP_DURATION_MINUTES")
            .help("The warmup duration in minutes.");
        parser.addArgument("-d", "--test-duration")
            .setDefault(5)
            .type(positiveInteger())
            .dest("testDurationMinutes")
            .metavar("TEST_DURATION_MINUTES")
            .help("The test duration in minutes.");
        parser.addArgument("-i", "--reporting-interval")
            .setDefault(1)
            .type(positiveInteger())
            .dest("reportingIntervalSeconds")
            .metavar("REPORTING_INTERVAL_SECONDS")
            .help("The reporting interval in seconds.");
        parser.addArgument("--value-schema")
            .type(String.class)
            .dest("valueSchema")
            .metavar("VALUE_SCHEMA")
            .help("The schema of the values ex. {\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}");
        parser.addArgument("--values-file")
            .type(String.class)
            .dest("valuesFile")
            .metavar("VALUES_FILE")
            .help("The avro value file. Example file content {\"f1\": \"value1\"}");
        return parser;
    }

    public String bootstrapServer() {
        return bootstrapServer;
    }

    public Map<String, String> adminConfig() {
        return adminConfigs;
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

    private Properties loadProperties(String filename) {
        try {
            return Utils.loadProps(filename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> parseConfigs(List<String> configs) {
        if (configs == null) {
            return new HashMap<>();
        }
        Map<String, String> map = new HashMap<>();
        for (String config : configs) {
            String[] parts = config.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid config: " + config);
            }
            map.put(parts[0], parts[1]);
        }
        return map;
    }

    private String randomTopicPrefix() {
        return String.format("topic_%d_%s", System.currentTimeMillis(), randomString(4));
    }

    private String randomString(int length) {
        final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random r = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(r.nextInt(characters.length())));
        }
        return sb.toString();
    }

    static class IntegerArgumentType extends ReflectArgumentType<Integer> {

        private final IntegerValidator validator;

        public IntegerArgumentType(IntegerValidator validator) {
            super(Integer.class);
            this.validator = validator;
        }

        @Override
        public Integer convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            Integer result = super.convert(parser, arg, value);
            String message = validator.validate(result);
            if (message != null) {
                throw new ArgumentParserException(message, parser, arg);
            }
            return result;
        }

        public static IntegerArgumentType nonNegativeInteger() {
            return new IntegerArgumentType(value -> value < 0 ? "expected a non-negative integer, but got " + value : null);
        }

        public static IntegerArgumentType positiveInteger() {
            return new IntegerArgumentType(value -> value <= 0 ? "expected a positive integer, but got " + value : null);
        }

        public static IntegerArgumentType notLessThan(int min) {
            return new IntegerArgumentType(value -> value < min ? "expected an integer not less than " + min + ", but got " + value : null);
        }

        public static IntegerArgumentType between(int min, int max) {
            return new IntegerArgumentType(value -> value < min || value > max ? "expected an integer between " + min + " and " + max + ", but got " + value : null);
        }
    }

    @FunctionalInterface
    interface IntegerValidator {
        /**
         * Validate the given value. Return an error message if the value is invalid, otherwise return null.
         */
        String validate(Integer value);
    }
}
