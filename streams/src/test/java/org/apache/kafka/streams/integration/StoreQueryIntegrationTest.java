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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


@Category({IntegrationTest.class})
public class StoreQueryIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";

    @Rule
    public final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private final MockTime mockTime = cluster.time;

    @Before
    public void before() throws InterruptedException {
        cluster.createTopic(INPUT_TOPIC_NAME, 2, 1);
    }

    @After
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close();
        }
    }

    @Test
    public void shouldQueryAllActivePartitionStoresByDefault() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                        Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                                .withCachingDisabled())
                .toStream()
                .peek((k, v) -> semaphore.release());

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> 0);

        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = QueryableStoreTypes.keyValueStore();
        final ReadOnlyKeyValueStore<Integer, Integer> store1 = kafkaStreams1
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType));

        final ReadOnlyKeyValueStore<Integer, Integer> store2 = kafkaStreams2
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType));

        final boolean kafkaStreams1IsActive;
        if ((keyQueryMetadata.getActiveHost().port() % 2) == 1) {
            kafkaStreams1IsActive = true;
        } else {
            kafkaStreams1IsActive = false;
        }

        // Assert that only active is able to query for a key by default
        assertThat(kafkaStreams1IsActive ? store1.get(key) : store2.get(key), is(notNullValue()));
        assertThat(kafkaStreams1IsActive ? store2.get(key) : store1.get(key), is(nullValue()));
    }

    @Test
    public void shouldQuerySpecificActivePartitionStores() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                        Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                                .withCachingDisabled())
                .toStream()
                .peek((k, v) -> semaphore.release());

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> 0);

        //key belongs to this partition
        final int keyPartition = keyQueryMetadata.getPartition();

        //key doesn't belongs to this partition
        final int keyDontBelongPartition = (keyPartition == 0) ? 1 : 0;
        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = QueryableStoreTypes.keyValueStore();
        ReadOnlyKeyValueStore<Integer, Integer> store1 = null;
        ReadOnlyKeyValueStore<Integer, Integer> store2 = null;
        try {
            store1 = kafkaStreams1
                    .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).withPartition(keyPartition));
        } catch (final InvalidStateStoreException exception) {
        //Only one among kafkaStreams1 and kafkaStreams2 will contain the specific active store requested. The other will throw exception
        }
        try {
            store2 = kafkaStreams2
                    .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).withPartition(keyPartition));
        } catch (final InvalidStateStoreException exception) {
            //Only one among kafkaStreams1 and kafkaStreams2 will contain the specific active store requested. The other will throw exception
        }
        final boolean kafkaStreams1IsActive;
        if ((keyQueryMetadata.getActiveHost().port() % 2) == 1) {
            kafkaStreams1IsActive = true;
            assertThat(store1, is(notNullValue()));
            assertThat(store2, is(nullValue()));
        } else {
            kafkaStreams1IsActive = false;
            assertThat(store2, is(notNullValue()));
            assertThat(store1, is(nullValue()));
        }

        // Assert that only active for a specific requested partition serves key if stale stores and not enabled
        assertThat(kafkaStreams1IsActive ? store1.get(key) : store2.get(key), is(notNullValue()));

        ReadOnlyKeyValueStore<Integer, Integer> store3 = null;
        ReadOnlyKeyValueStore<Integer, Integer> store4 = null;
        try {
            store3 = kafkaStreams1
                    .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).withPartition(keyDontBelongPartition));
        } catch (final InvalidStateStoreException exception) {
            //Only one among kafkaStreams1 and kafkaStreams2 will contain the specific active store requested. The other will throw exception
        }
        try {
            store4 = kafkaStreams2
                    .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).withPartition(keyDontBelongPartition));
        } catch (final InvalidStateStoreException exception) {
            //Only one among kafkaStreams1 and kafkaStreams2 will contain the specific active store requested. The other will throw exception
        }

        // Assert that key is not served when wrong specific partition is requested
        // If kafkaStreams1 is active for keyPartition, kafkaStreams2 would be active for keyDontBelongPartition
        // So, in that case, store3 would be null and the store4 would not return the value for key as wrong partition was requested
        assertThat(kafkaStreams1IsActive ? store4.get(key) : store3.get(key), is(nullValue()));
    }

    @Test
    public void shouldQueryAllStalePartitionStores() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                        Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                                .withCachingDisabled())
                .toStream()
                .peek((k, v) -> semaphore.release());

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));

        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = QueryableStoreTypes.keyValueStore();

        // Assert that both active and standby are able to query for a key
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = kafkaStreams1
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).enableStaleStores());
            return store1.get(key) != null;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = kafkaStreams2
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).enableStaleStores());
            return store2.get(key) != null;
        }, "store2 cannot find results for key");
    }

    @Test
    public void shouldQuerySpecificStalePartitionStores() throws Exception {
        final int batch1NumMessages = 100;
        final int key = 1;
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                        Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                                .withCachingDisabled())
                .toStream()
                .peek((k, v) -> semaphore.release());

        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration());
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration());
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        startApplicationAndWaitUntilRunning(kafkaStreamsList, Duration.ofSeconds(60));

        produceValueRange(key, 0, batch1NumMessages);

        // Assert that all messages in the first batch were processed in a timely manner
        assertThat(semaphore.tryAcquire(batch1NumMessages, 60, TimeUnit.SECONDS), is(equalTo(true)));
        final KeyQueryMetadata keyQueryMetadata = kafkaStreams1.queryMetadataForKey(TABLE_NAME, key, (topic, somekey, value, numPartitions) -> 0);

        //key belongs to this partition
        final int keyPartition = keyQueryMetadata.getPartition();

        //key doesn't belongs to this partition
        final int keyDontBelongPartition = (keyPartition == 0) ? 1 : 0;
        final QueryableStoreType<ReadOnlyKeyValueStore<Integer, Integer>> queryableStoreType = QueryableStoreTypes.keyValueStore();

        // Assert that both active and standby are able to query for a key
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store1 = kafkaStreams1
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).enableStaleStores().withPartition(keyPartition));
            return store1.get(key) != null;
        }, "store1 cannot find results for key");
        TestUtils.waitForCondition(() -> {
            final ReadOnlyKeyValueStore<Integer, Integer> store2 = kafkaStreams2
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).enableStaleStores().withPartition(keyPartition));
            return store2.get(key) != null;
        }, "store2 cannot find results for key");

        final ReadOnlyKeyValueStore<Integer, Integer> store3 = kafkaStreams1
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).enableStaleStores().withPartition(keyDontBelongPartition));

        final ReadOnlyKeyValueStore<Integer, Integer> store4 = kafkaStreams2
                .store(StoreQueryParams.fromNameAndType(TABLE_NAME, queryableStoreType).enableStaleStores().withPartition(keyDontBelongPartition));

        // Assert that
        assertThat(store3.get(key), is(nullValue()));
        assertThat(store4.get(key), is(nullValue()));
    }

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private void produceValueRange(final int key, final int start, final int endExclusive) throws Exception {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC_NAME,
                IntStream.range(start, endExclusive)
                        .mapToObj(i -> KeyValue.pair(key, i))
                        .collect(Collectors.toList()),
                producerProps,
                mockTime);
    }

    private Properties streamsConfiguration() {
        final String applicationId = "streamsApp";
        final Properties config = new Properties();
        config.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + String.valueOf(++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(applicationId).getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        return config;
    }
}
