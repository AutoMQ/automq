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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockReducer;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Collections;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class TimeWindowedKStreamImplTest {
    private static final String TOPIC = "input";
    private static final Windowed<String> KEY_1_WINDOW_0 = new Windowed<>("1", new TimeWindow(0L, 500L));
    private static final Windowed<String> KEY_1_WINDOW_1 = new Windowed<>("1", new TimeWindow(500L, 1000L));
    private static final Windowed<String> KEY_2_WINDOW_1 = new Windowed<>("2", new TimeWindow(500L, 1000L));
    private static final Windowed<String> KEY_2_WINDOW_2 = new Windowed<>("2", new TimeWindow(1000L, 1500L));

    private final StreamsBuilder builder = new StreamsBuilder();
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());
    private TimeWindowedKStream<String, String> windowedStream;

    @Parameter
    public StrategyType type;

    @Parameter(1)
    public boolean withCache;

    private EmitStrategy emitStrategy;
    private boolean emitFinal;

    @Parameterized.Parameters(name = "{0}_cache:{1}")
    public static Collection<Object[]> data() {
        return asList(new Object[][] {
            {StrategyType.ON_WINDOW_UPDATE, true},
            {StrategyType.ON_WINDOW_UPDATE, false},
            {StrategyType.ON_WINDOW_CLOSE, true},
            {StrategyType.ON_WINDOW_CLOSE, false}
        });
    }

    @Before
    public void before() {
        emitFinal = type.equals(StrategyType.ON_WINDOW_CLOSE);
        emitStrategy = StrategyType.forType(type);
        // Set interval to 0 so that it always tries to emit
        props.setProperty(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, "0");
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        windowedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(500L)));
    }

    @Test
    public void shouldCountWindowed() {
        final MockApiProcessorSupplier<Windowed<String>, Long, Void, Void> supplier = new MockApiProcessorSupplier<>();
        windowedStream
            .emitStrategy(emitStrategy)
            .count()
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }
        final ArrayList<KeyValueTimestamp<Windowed<String>, Long>> processed = supplier.theCapturedProcessor().processed();

        if (emitFinal) {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, 2L, 15L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_1, 1L, 500L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, 2L, 550L)
                ),
                processed
            );
        } else {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, 1L, 10L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, 2L, 15L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_1, 1L, 500L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, 1L, 550L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, 2L, 550L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_2, 1L, 1000L)
                ),
                processed
            );
        }
    }

    @Test
    public void shouldReduceWindowed() {
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        windowedStream
            .emitStrategy(emitStrategy)
            .reduce(MockReducer.STRING_ADDER)
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> processed = supplier.theCapturedProcessor().processed();
        if (emitFinal) {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, "1+2", 15L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_1, "3", 500L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, "10+20", 550L)
                ),
                processed
            );
        } else {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, "1", 10L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, "1+2", 15L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_1, "3", 500L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, "10", 550L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, "10+20", 550L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_2, "30", 1000L)
                ),
                processed
            );
        }
    }

    @Test
    public void shouldAggregateWindowed() {
        final MockApiProcessorSupplier<Windowed<String>, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        windowedStream
            .emitStrategy(emitStrategy)
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                setMaterializedCache(Materialized.with(Serdes.String(), Serdes.String())))
            .toStream()
            .process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
        }

        final ArrayList<KeyValueTimestamp<Windowed<String>, String>> processed = supplier.theCapturedProcessor().processed();

        if (emitFinal) {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, "0+1+2", 15L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_1, "0+3", 500L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, "0+10+20", 550L)
                ),
                processed
            );
        } else {
            assertEquals(
                asList(
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, "0+1", 10L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_0, "0+1+2", 15L),
                    new KeyValueTimestamp<>(KEY_1_WINDOW_1, "0+3", 500L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, "0+10", 550L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_1, "0+10+20", 550L),
                    new KeyValueTimestamp<>(KEY_2_WINDOW_2, "0+30", 1000L)
                ),
                processed
            );
        }
    }

    @Test
    public void shouldMaterializeCount() {
        windowedStream
            .emitStrategy(emitStrategy)
            .count(
                setMaterializedCache(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("count-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            {
                final WindowStore<String, Long> windowStore = driver.getWindowStore("count-store");
                final List<KeyValue<Windowed<String>, Long>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

                if (withCache) {
                    // with cache returns all records (expired from underneath as well) as part of
                    // the merge process
                    assertThat(data, equalTo(asList(
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), 2L),
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 2L),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), 1L))));
                } else {
                    // without cache, we get only non-expired record from underlying store.
                    if (!emitFinal) {
                        assertThat(data, equalTo(Collections.singletonList(
                                KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), 1L))));
                    } else {
                        assertThat(data, equalTo(asList(
                                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), 1L),
                                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), 2L),
                                KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), 1L))));
                    }
                }
            }
            {
                final WindowStore<String, ValueAndTimestamp<Long>> windowStore =
                    driver.getTimestampedWindowStore("count-store");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<Long>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

                // the same values and logic described above applies here as well.
                if (withCache) {
                    assertThat(data, equalTo(asList(
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), ValueAndTimestamp.make(2L, 15L)),
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make(1L, 500L)),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make(2L, 550L)),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make(1L, 1000L)))));
                } else {
                    if (!emitFinal) {
                        assertThat(data, equalTo(Collections.singletonList(
                                KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make(1L, 1000L)))));
                    } else {
                        assertThat(data, equalTo(asList(
                                KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make(1L, 500L)),
                                KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make(2L, 550L)),
                                KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make(1L, 1000L)))));
                    }
                }
            }
        }
    }

    @Test
    public void shouldMaterializeReduced() {
        windowedStream.reduce(
            MockReducer.STRING_ADDER,
            setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("reduced")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            {
                final WindowStore<String, String> windowStore = driver.getWindowStore("reduced");
                final List<KeyValue<Windowed<String>, String>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

                if (withCache) {
                    // with cache returns all records (expired from underneath as well) as part of
                    // the merge process
                    assertThat(data, equalTo(asList(
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), "1+2"),
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), "3"),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), "10+20"),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), "30"))));
                } else {
                    // without cache, we get only non-expired record from underlying store.
                    // actualFrom = observedStreamTime(1500) - retentionPeriod(1000) + 1 = 501.
                    // only 1 record is non expired and would be returned.
                    assertThat(data, equalTo(Collections.singletonList(KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), "30"))));
                }
            }
            {
                final WindowStore<String, ValueAndTimestamp<String>> windowStore = driver.getTimestampedWindowStore("reduced");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<String>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

                // same logic/data as explained above.
                if (withCache) {
                    assertThat(data, equalTo(asList(
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), ValueAndTimestamp.make("1+2", 15L)),
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make("3", 500L)),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make("10+20", 550L)),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make("30", 1000L)))));
                } else {
                    assertThat(data, equalTo(Collections.singletonList(
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make("30", 1000L)))));
                }
            }
        }
    }

    @Test
    public void shouldMaterializeAggregated() {
        windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            setMaterializedCache(Materialized.<String, String, WindowStore<Bytes, byte[]>>as("aggregated")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())));

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            processData(driver);
            {
                final WindowStore<String, String> windowStore = driver.getWindowStore("aggregated");
                final List<KeyValue<Windowed<String>, String>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));

                if (withCache) {
                    // with cache returns all records (expired from underneath as well) as part of
                    // the merge process
                    assertThat(data, equalTo(asList(
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), "0+1+2"),
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), "0+3"),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), "0+10+20"),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), "0+30"))));
                } else {
                    // without cache, we get only non-expired record from underlying store.
                    // actualFrom = observedStreamTime(1500) - retentionPeriod(1000) + 1 = 501.
                    // only 1 record is non expired and would be returned.
                    assertThat(data, equalTo(Collections
                            .singletonList(KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), "0+30"))));
                }
            }
            {
                final WindowStore<String, ValueAndTimestamp<String>> windowStore = driver.getTimestampedWindowStore("aggregated");
                final List<KeyValue<Windowed<String>, ValueAndTimestamp<String>>> data =
                    StreamsTestUtils.toList(windowStore.fetch("1", "2", ofEpochMilli(0), ofEpochMilli(1000L)));
                if (withCache) {
                    assertThat(data, equalTo(asList(
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(0, 500)), ValueAndTimestamp.make("0+1+2", 15L)),
                            KeyValue.pair(new Windowed<>("1", new TimeWindow(500, 1000)), ValueAndTimestamp.make("0+3", 500L)),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(500, 1000)), ValueAndTimestamp.make("0+10+20", 550L)),
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make("0+30", 1000L)))));
                } else {
                    assertThat(data, equalTo(Collections.singletonList(
                            KeyValue.pair(new Windowed<>("2", new TimeWindow(1000, 1500)), ValueAndTimestamp.make("0+30", 1000L)))));
                }
            }
        }
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfInitializerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(null, MockAggregator.TOSTRING_ADDER));
    }

    @Test
    public void shouldThrowNullPointerOnAggregateIfAggregatorIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(MockInitializer.STRING_INIT, null));
    }

    @Test
    public void shouldThrowNullPointerOnReduceIfReducerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfInitializerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(
            null,
            MockAggregator.TOSTRING_ADDER,
            setMaterializedCache(Materialized.as("store"))));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfAggregatorIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            null,
            setMaterializedCache(Materialized.as("store"))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldThrowNullPointerOnMaterializedAggregateIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.aggregate(
            MockInitializer.STRING_INIT,
            MockAggregator.TOSTRING_ADDER,
            (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfReducerIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(
            null,
            setMaterializedCache(Materialized.as("store"))));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowNullPointerOnMaterializedReduceIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(
            MockReducer.STRING_ADDER,
            (Materialized) null));
    }

    @Test
    public void shouldThrowNullPointerOnMaterializedReduceIfNamedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.reduce(
            MockReducer.STRING_ADDER,
            (Named) null));
    }

    @Test
    public void shouldThrowNullPointerOnCountIfMaterializedIsNull() {
        assertThrows(NullPointerException.class, () -> windowedStream.count((Materialized<String, Long, WindowStore<Bytes, byte[]>>) null));
    }

    private void processData(final TopologyTestDriver driver) {
        final TestInputTopic<String, String> inputTopic =
                driver.createInputTopic(TOPIC, new StringSerializer(), new StringSerializer());
        inputTopic.pipeInput("1", "1", 10L);
        inputTopic.pipeInput("1", "2", 15L);
        inputTopic.pipeInput("1", "3", 500L);
        inputTopic.pipeInput("2", "10", 550L);
        inputTopic.pipeInput("2", "20", 500L);
        inputTopic.pipeInput("2", "30", 1000L);
    }

    private <K, V, S extends StateStore> Materialized<K, V, S> setMaterializedCache(final Materialized<K, V, S> materialized) {
        if (withCache) {
            return materialized.withCachingEnabled();
        }
        return materialized.withCachingDisabled();
    }
}
