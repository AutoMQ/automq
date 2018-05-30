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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.SourceNode;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class KStreamImplTest {

    private final Consumed<String, String> stringConsumed = Consumed.with(Serdes.String(), Serdes.String());
    private final MockProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();

    private KStream<String, String> testStream;
    private StreamsBuilder builder;

    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.topologyTestConfig(Serdes.String(), Serdes.String());

    @Before
    public void before() {
        builder = new StreamsBuilder();
        testStream = builder.stream("source");
    }

    @Test
    public void testNumProcesses() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source1 = builder.stream(Arrays.asList("topic-1", "topic-2"), stringConsumed);

        KStream<String, String> source2 = builder.stream(Arrays.asList("topic-3", "topic-4"), stringConsumed);

        KStream<String, String> stream1 =
            source1.filter(new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return true;
                }
            }).filterNot(new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return false;
                }
            });

        KStream<String, Integer> stream2 = stream1.mapValues(new ValueMapper<String, Integer>() {
            @Override
            public Integer apply(String value) {
                return new Integer(value);
            }
        });

        KStream<String, Integer> stream3 = source2.flatMapValues(new ValueMapper<String, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> apply(String value) {
                return Collections.singletonList(new Integer(value));
            }
        });

        KStream<String, Integer>[] streams2 = stream2.branch(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                },
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return true;
                    }
                }
        );

        KStream<String, Integer>[] streams3 = stream3.branch(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return (value % 2) == 0;
                    }
                },
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(String key, Integer value) {
                        return true;
                    }
                }
        );

        final int anyWindowSize = 1;
        final Joined<String, Integer, Integer> joined = Joined.with(Serdes.String(), Serdes.Integer(), Serdes.Integer());
        KStream<String, Integer> stream4 = streams2[0].join(streams3[0], new ValueJoiner<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer value1, Integer value2) {
                return value1 + value2;
            }
        }, JoinWindows.of(anyWindowSize), joined);

        streams2[1].join(streams3[1], new ValueJoiner<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer value1, Integer value2) {
                return value1 + value2;
            }
        }, JoinWindows.of(anyWindowSize), joined);

        stream4.to("topic-5");

        streams2[1].through("topic-6").process(new MockProcessorSupplier<String, Integer>());

        assertEquals(2 + // sources
            2 + // stream1
            1 + // stream2
            1 + // stream3
            1 + 2 + // streams2
            1 + 2 + // streams3
            5 * 2 + // stream2-stream3 joins
            1 + // to
            2 + // through
            1, // process
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build(null).processors().size());
    }

    @Test
    public void shouldUseRecordMetadataTimestampExtractorWithThrough() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream1 = builder.stream(Arrays.asList("topic-1", "topic-2"), stringConsumed);
        KStream<String, String> stream2 = builder.stream(Arrays.asList("topic-3", "topic-4"), stringConsumed);

        stream1.to("topic-5");
        stream2.through("topic-6");

        ProcessorTopology processorTopology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build(null);
        assertThat(processorTopology.source("topic-6").getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
        assertEquals(processorTopology.source("topic-4").getTimestampExtractor(), null);
        assertEquals(processorTopology.source("topic-3").getTimestampExtractor(), null);
        assertEquals(processorTopology.source("topic-2").getTimestampExtractor(), null);
        assertEquals(processorTopology.source("topic-1").getTimestampExtractor(), null);
    }

    @Test
    public void shouldSendDataThroughTopicUsingProduced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.through("through-topic", Produced.with(Serdes.String(), Serdes.String())).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(input, "a", "b"));
        }
        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList("a:b")));
    }

    @Test
    public void shouldSendDataToTopicUsingProduced() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.to("to-topic", Produced.with(Serdes.String(), Serdes.String()));
        builder.stream("to-topic", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(input, "e", "f"));
        }
        assertThat(processorSupplier.theCapturedProcessor().processed, equalTo(Collections.singletonList("e:f")));
    }

    @Test
    public void shouldSendDataToDynamicTopics() {
        final StreamsBuilder builder = new StreamsBuilder();
        final String input = "topic";
        final KStream<String, String> stream = builder.stream(input, stringConsumed);
        stream.to((key, value, context) -> context.topic() + "-" + key + "-" + value.substring(0, 1),
                  Produced.with(Serdes.String(), Serdes.String()));
        builder.stream(input + "-a-v", stringConsumed).process(processorSupplier);
        builder.stream(input + "-b-v", stringConsumed).process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(input, "a", "v1"));
            driver.pipeInput(recordFactory.create(input, "a", "v2"));
            driver.pipeInput(recordFactory.create(input, "b", "v1"));
        }
        List<MockProcessor<String, String>> mockProcessors = processorSupplier.capturedProcessors(2);
        assertThat(mockProcessors.get(0).processed, equalTo(Utils.mkList("a:v1", "a:v2")));
        assertThat(mockProcessors.get(1).processed, equalTo(Collections.singletonList("b:v1")));
    }

    @Test
    public void shouldUseRecordMetadataTimestampExtractorWhenInternalRepartitioningTopicCreated() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> kStream = builder.stream("topic-1", stringConsumed);
        final ValueJoiner<String, String, String> valueJoiner = MockValueJoiner.instance(":");
        final long windowSize = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final KStream<String, String> stream = kStream
                        .map(new KeyValueMapper<String, String, KeyValue<? extends String, ? extends String>>() {
                            @Override
                            public KeyValue<? extends String, ? extends String> apply(String key, String value) {
                                return KeyValue.pair(value, value);
                            }
                        });
        stream.join(kStream,
                    valueJoiner,
                    JoinWindows.of(windowSize).until(3 * windowSize),
                    Joined.with(Serdes.String(),
                                Serdes.String(),
                                Serdes.String()))
                .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        final ProcessorTopology topology = TopologyWrapper.getInternalTopologyBuilder(builder.build()).setApplicationId("X").build();

        final SourceNode originalSourceNode = topology.source("topic-1");

        for (SourceNode sourceNode: topology.sources()) {
            if (sourceNode.name().equals(originalSourceNode.name())) {
                assertEquals(sourceNode.getTimestampExtractor(), null);
            } else {
                assertThat(sourceNode.getTimestampExtractor(), instanceOf(FailOnInvalidTimestamp.class));
            }
        }
    }
    
    @Test
    public void testToWithNullValueSerdeDoesntNPE() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
        final KStream<String, String> inputStream = builder.stream(Collections.singleton("input"), consumed);

        inputStream.to("output", Produced.with(Serdes.String(), Serdes.String()));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilter() {
        testStream.filter(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullPredicateOnFilterNot() {
        testStream.filterNot(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnSelectKey() {
        testStream.selectKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMap() {
        testStream.map(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValues() {
        testStream.mapValues((ValueMapper) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnMapValuesWithKey() {
        testStream.mapValues((ValueMapperWithKey) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMap() {
        testStream.flatMap(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMapValues() {
        testStream.flatMapValues((ValueMapper<? super String, ? extends Iterable<? extends String>>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnFlatMapValuesWithKey() {
        testStream.flatMapValues((ValueMapperWithKey<? super String, ? super String, ? extends Iterable<? extends String>>) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldHaveAtLeastOnPredicateWhenBranching() {
        testStream.branch();
    }

    @Test(expected = NullPointerException.class)
    public void shouldCantHaveNullPredicate() {
        testStream.branch((Predicate) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnThrough() {
        testStream.through(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicOnTo() {
        testStream.to((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTopicChooserOnTo() {
        testStream.to((TopicNameExtractor<String, String>) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTransformSupplierOnTransform() {
        testStream.transform(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTransformSupplierOnTransformValues() {
        testStream.transformValues((ValueTransformerSupplier) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTransformSupplierOnTransformValuesWithKey() {
        testStream.transformValues((ValueTransformerWithKeySupplier) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullProcessSupplier() {
        testStream.process(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullOtherStreamOnJoin() {
        testStream.join(null, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(10));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullValueJoinerOnJoin() {
        testStream.join(testStream, null, JoinWindows.of(10));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinWindowsOnJoin() {
        testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnTableJoin() {
        testStream.leftJoin((KTable) null, MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullValueMapperOnTableJoin() {
        testStream.leftJoin(builder.table("topic", stringConsumed), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullSelectorOnGroupBy() {
        testStream.groupBy(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullActionOnForEach() {
        testStream.foreach(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnJoinWithGlobalTable() {
        testStream.join((GlobalKTable) null,
                        MockMapper.<String, String>selectValueMapper(),
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnJoinWithGlobalTable() {
        testStream.join(builder.globalTable("global", stringConsumed),
                        null,
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnJoinWithGlobalTable() {
        testStream.join(builder.globalTable("global", stringConsumed),
                        MockMapper.<String, String>selectValueMapper(),
                        null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullTableOnJLeftJoinWithGlobalTable() {
        testStream.leftJoin((GlobalKTable) null,
                        MockMapper.<String, String>selectValueMapper(),
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullMapperOnLeftJoinWithGlobalTable() {
        testStream.leftJoin(builder.globalTable("global", stringConsumed),
                        null,
                        MockValueJoiner.TOSTRING_JOINER);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowNullJoinerOnLeftJoinWithGlobalTable() {
        testStream.leftJoin(builder.globalTable("global", stringConsumed),
                        MockMapper.<String, String>selectValueMapper(),
                        null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnPrintIfPrintedIsNull() {
        testStream.print(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnThroughWhenProducedIsNull() {
        testStream.through("topic", null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnToWhenProducedIsNull() {
        testStream.to("topic", null);
    }

    @Test
    public void shouldThrowNullPointerOnLeftJoinWithTableWhenJoinedIsNull() {
        final KTable<String, String> table = builder.table("blah", stringConsumed);
        try {
            testStream.leftJoin(table,
                                MockValueJoiner.TOSTRING_JOINER,
                                null);
            fail("Should have thrown NullPointerException");
        } catch (final NullPointerException e) {
            // ok
        }
    }

    @Test
    public void shouldThrowNullPointerOnJoinWithTableWhenJoinedIsNull() {
        final KTable<String, String> table = builder.table("blah", stringConsumed);
        try {
            testStream.join(table,
                            MockValueJoiner.TOSTRING_JOINER,
                            null);
            fail("Should have thrown NullPointerException");
        } catch (final NullPointerException e) {
            // ok
        }
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnJoinWithStreamWhenJoinedIsNull() {
        testStream.join(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(10), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnOuterJoinJoinedIsNull() {
        testStream.outerJoin(testStream, MockValueJoiner.TOSTRING_JOINER, JoinWindows.of(10), null);
    }
    
    @Test
    public void shouldMergeTwoStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> merged = source1.merge(source2);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "aa"));
            driver.pipeInput(recordFactory.create(topic2, "B", "bb"));
            driver.pipeInput(recordFactory.create(topic2, "C", "cc"));
            driver.pipeInput(recordFactory.create(topic1, "D", "dd"));
        }

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd"), processorSupplier.theCapturedProcessor().processed);
    }
    
    @Test
    public void shouldMergeMultipleStreams() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final String topic4 = "topic-4";

        final KStream<String, String> source1 = builder.stream(topic1);
        final KStream<String, String> source2 = builder.stream(topic2);
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> source4 = builder.stream(topic4);
        final KStream<String, String> merged = source1.merge(source2).merge(source3).merge(source4);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "aa"));
            driver.pipeInput(recordFactory.create(topic2, "B", "bb"));
            driver.pipeInput(recordFactory.create(topic3, "C", "cc"));
            driver.pipeInput(recordFactory.create(topic4, "D", "dd"));
            driver.pipeInput(recordFactory.create(topic4, "E", "ee"));
            driver.pipeInput(recordFactory.create(topic3, "F", "ff"));
            driver.pipeInput(recordFactory.create(topic2, "G", "gg"));
            driver.pipeInput(recordFactory.create(topic1, "H", "hh"));
        }

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd", "E:ee", "F:ff", "G:gg", "H:hh"),
                     processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessFromSourceThatMatchPattern() {
        final KStream<String, String> pattern2Source = builder.stream(Pattern.compile("topic-\\d"));

        pattern2Source.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("topic-3", "A", "aa"));
            driver.pipeInput(recordFactory.create("topic-4", "B", "bb"));
            driver.pipeInput(recordFactory.create("topic-5", "C", "cc"));
            driver.pipeInput(recordFactory.create("topic-6", "D", "dd"));
            driver.pipeInput(recordFactory.create("topic-7", "E", "ee"));
        }

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd", "E:ee"),
                processorSupplier.theCapturedProcessor().processed);
    }

    @Test
    public void shouldProcessFromSourcesThatMatchMultiplePattern() {
        final String topic3 = "topic-without-pattern";

        final KStream<String, String> pattern2Source1 = builder.stream(Pattern.compile("topic-\\d"));
        final KStream<String, String> pattern2Source2 = builder.stream(Pattern.compile("topic-[A-Z]"));
        final KStream<String, String> source3 = builder.stream(topic3);
        final KStream<String, String> merged = pattern2Source1.merge(pattern2Source2).merge(source3);

        merged.process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create("topic-3", "A", "aa"));
            driver.pipeInput(recordFactory.create("topic-4", "B", "bb"));
            driver.pipeInput(recordFactory.create("topic-A", "C", "cc"));
            driver.pipeInput(recordFactory.create("topic-Z", "D", "dd"));
            driver.pipeInput(recordFactory.create(topic3, "E", "ee"));
        }

        assertEquals(Utils.mkList("A:aa", "B:bb", "C:cc", "D:dd", "E:ee"),
                processorSupplier.theCapturedProcessor().processed);
    }
}
