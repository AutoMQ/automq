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
package org.apache.kafka.streams.examples.docs;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * This is code sample in docs/streams/developer-guide/testing.html
 */

public class DeveloperGuideTesting {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private KeyValueStore<String, Long> store;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();

    @Before
    public void setup() {
        final Topology topology = new Topology();
        topology.addSource("sourceProcessor", "input-topic");
        topology.addProcessor("aggregator", new CustomMaxAggregatorSupplier(), "sourceProcessor");
        topology.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("aggStore"),
                        Serdes.String(),
                        Serdes.Long()).withLoggingDisabled(), // need to disable logging to allow store pre-populating
                "aggregator");
        topology.addSink("sinkProcessor", "result-topic", "aggregator");

        // setup test driver
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

        // pre-populate store
        store = testDriver.getKeyValueStore("aggStore");
        store.put("a", 21L);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }


    @Test
    public void shouldFlushStoreForFirstInput() {
        inputTopic.pipeInput("a", 1L);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        inputTopic.pipeInput("a", 1L);
        assertThat(store.get("a"), equalTo(21L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldNotUpdateStoreForLargerValue() {
        inputTopic.pipeInput("a", 42L);
        assertThat(store.get("a"), equalTo(42L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 42L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldUpdateStoreForNewKey() {
        inputTopic.pipeInput("b", 21L);
        assertThat(store.get("b"), equalTo(21L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("b", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldPunctuateIfEvenTimeAdvances() {
        final Instant recordTime = Instant.now();
        inputTopic.pipeInput("a", 1L,  recordTime);
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));

        inputTopic.pipeInput("a", 1L,  recordTime);
        assertThat(outputTopic.isEmpty(), is(true));

        inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(10L));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void shouldPunctuateIfWallClockTimeAdvances() {
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>("a", 21L)));
        assertThat(outputTopic.isEmpty(), is(true));
    }

    public static class CustomMaxAggregatorSupplier implements ProcessorSupplier<String, Long> {
        @Override
        public Processor<String, Long> get() {
            return new CustomMaxAggregator();
        }
    }

    public static class CustomMaxAggregator implements Processor<String, Long> {
        ProcessorContext context;
        private KeyValueStore<String, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
            context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
            context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore());
            store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
        }

        @Override
        public void process(final String key, final Long value) {
            final Long oldValue = store.get(key);
            if (oldValue == null || value > oldValue) {
                store.put(key, value);
            }
        }

        private void flushStore() {
            final KeyValueIterator<String, Long> it = store.all();
            while (it.hasNext()) {
                final KeyValue<String, Long> next = it.next();
                context.forward(next.key, next.value);
            }
        }

        @Override
        public void close() {}
    }
}
