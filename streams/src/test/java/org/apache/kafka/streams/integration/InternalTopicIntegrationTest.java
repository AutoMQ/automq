/*
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
package org.apache.kafka.streams.integration;

import kafka.log.LogConfig;
import kafka.utils.MockTime;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to internal topics in streams
 */
@Category({IntegrationTest.class})
public class InternalTopicIntegrationTest {
    private static final int NUM_BROKERS = 1;
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;
    private Properties streamsConfiguration;
    private String applicationId = "compact-topics-integration-test";

    @BeforeClass
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopics(DEFAULT_INPUT_TOPIC, DEFAULT_OUTPUT_TOPIC);
    }

    @Before
    public void before() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    private Properties getTopicConfigProperties(final String changelog) {
        final KafkaZkClient kafkaZkClient = KafkaZkClient.apply(CLUSTER.zKConnectString(), false,
                DEFAULT_ZK_SESSION_TIMEOUT_MS, DEFAULT_ZK_CONNECTION_TIMEOUT_MS, Integer.MAX_VALUE, Time.SYSTEM,
                "testMetricGroup", "testMetricType");
        try {
            final AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);

            final Map<String, Properties> topicConfigs = adminZkClient.getAllTopicConfigs();
            final Iterator it = topicConfigs.iterator();
            while (it.hasNext()) {
                final Tuple2<String, Properties> topicConfig = (Tuple2<String, Properties>) it.next();
                final String topic = topicConfig._1;
                final Properties prop = topicConfig._2;
                if (topic.equals(changelog)) {
                    return prop;
                }
            }
            return new Properties();
        } finally {
            kafkaZkClient.close();
        }
    }

    @Test
    public void shouldCompactTopicsForStateChangelogs() throws Exception {
        //
        // Step 1: Configure and start a simple word count topology
        //
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "compact-topics-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        final KStream<String, Long> wordCounts = textLines
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(final String value) {
                        return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                    }
                }).groupBy(MockKeyValueMapper.<String, String>SelectValueMapper())
                .count("Counts").toStream();

        wordCounts.to(stringSerde, longSerde, DEFAULT_OUTPUT_TOPIC);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        streams.close();
        final Properties properties = getTopicConfigProperties(ProcessorStateManager.storeChangelogTopic(applicationId, "Counts"));
        assertEquals(LogConfig.Compact(), properties.getProperty(LogConfig.CleanupPolicyProp()));
    }

    private void produceData(final List<String> inputValues) throws Exception {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerConfig, mockTime);
    }

    @Test
    public void shouldUseCompactAndDeleteForWindowStoreChangelogs() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        final int durationMs = 2000;
        textLines
                .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(String value) {
                        return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                    }
                }).groupBy(MockKeyValueMapper.<String, String>SelectValueMapper())
                .count(TimeWindows.of(1000).until(durationMs), "CountWindows").toStream();


        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        streams.close();
        final Properties properties = getTopicConfigProperties(ProcessorStateManager.storeChangelogTopic(applicationId, "CountWindows"));
        final List<String> policies = Arrays.asList(properties.getProperty(LogConfig.CleanupPolicyProp()).split(","));
        assertEquals(2, policies.size());
        assertTrue(policies.contains(LogConfig.Compact()));
        assertTrue(policies.contains(LogConfig.Delete()));
        // retention should be 1 day + the window duration
        final long retention = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) + durationMs;
        assertEquals(retention, Long.parseLong(properties.getProperty(LogConfig.RetentionMsProp())));
    }
}
