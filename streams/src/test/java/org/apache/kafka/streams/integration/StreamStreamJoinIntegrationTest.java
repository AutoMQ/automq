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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertThrows;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public class StreamStreamJoinIntegrationTest extends AbstractJoinIntegrationTest {
    private KStream<Long, String> leftStream;
    private KStream<Long, String> rightStream;

    public StreamStreamJoinIntegrationTest(final boolean cacheEnabled) {
        super(cacheEnabled);
    }

    @Before
    public void prepareTopology() throws InterruptedException {
        super.prepareEnvironment();

        appID = "stream-stream-join-integration-test";

        builder = new StreamsBuilder();
        leftStream = builder.stream(INPUT_TOPIC_LEFT);
        rightStream = builder.stream(INPUT_TOPIC_RIGHT);
    }

    @Test
    public void shouldNotAccessJoinStoresWhenGivingName() throws InterruptedException {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-no-store-access");
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> left = builder.stream(INPUT_TOPIC_LEFT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right = builder.stream(INPUT_TOPIC_RIGHT, Consumed.with(Serdes.String(), Serdes.Integer()));
        final CountDownLatch latch = new CountDownLatch(1);

        left.join(
            right,
            (value1, value2) -> value1 + value2,
            JoinWindows.of(ofMillis(100)),
            StreamJoined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer()).withStoreName("join-store"));

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), STREAMS_CONFIG)) {
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            kafkaStreams.start();
            latch.await();
            assertThrows(InvalidStateStoreException.class, () -> kafkaStreams.store(StoreQueryParameters.fromNameAndType("join-store", QueryableStoreTypes.keyValueStore())));
        }
    }

    @Test
    public void testInner() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }

    @Test
    public void testInnerRepartitioned() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-inner-repartitioned");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
                .join(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                                 .selectKey(MockMapper.selectKeyKeyValueMapper()),
                       valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }

    @Test
    public void testLeft() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.leftJoin(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }

    @Test
    public void testLeftRepartitioned() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left-repartitioned");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
                .leftJoin(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                                     .selectKey(MockMapper.selectKeyKeyValueMapper()),
                        valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }

    @Test
    public void testOuter() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.outerJoin(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }

    @Test
    public void testOuterRepartitioned() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-outer");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-null", null, 3L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d", null, 15L))
        );

        leftStream.map(MockMapper.noOpKeyValueMapper())
                .outerJoin(rightStream.flatMap(MockMapper.noOpFlatKeyValueMapper())
                                .selectKey(MockMapper.selectKeyKeyValueMapper()),
                        valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }

    @Test
    public void testMultiInner() {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-multi-inner");

        final List<List<TestRecord<Long, String>>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "A-a-a", null, 4L)),
            Collections.singletonList(new TestRecord<>(ANY_UNIQUE_KEY, "B-a-a", null, 5L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-a", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-a", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-b", null, 6L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-b", null, 6L)),
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-b", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-a", null, 9L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-b", null, 9L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-a", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-b", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-c", null, 10L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-c", null, 10L)),
            null,
            null,
            null,
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-a", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-b", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-c", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-a-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-b-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-c-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "A-d-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "B-d-d", null, 14L),
                new TestRecord<>(ANY_UNIQUE_KEY, "C-d-d", null, 14L)),
            Arrays.asList(
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-a-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-b-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-c-d", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-a", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-b", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-c", null, 15L),
                new TestRecord<>(ANY_UNIQUE_KEY, "D-d-d", null, 15L))
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10)))
                .join(rightStream, valueJoiner, JoinWindows.of(ofSeconds(10))).to(OUTPUT_TOPIC);

        runTestWithDriver(expectedResult);
    }
}
