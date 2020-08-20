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

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertTrue;

public class AbstractStreamTest {

    @Test
    public void testToInternalValueTransformerSupplierSuppliesNewTransformers() {
        final ValueTransformerSupplier<?, ?> valueTransformerSupplier = createMock(ValueTransformerSupplier.class);
        expect(valueTransformerSupplier.get()).andReturn(null).times(3);
        final ValueTransformerWithKeySupplier<?, ?, ?> valueTransformerWithKeySupplier =
            AbstractStream.toValueTransformerWithKeySupplier(valueTransformerSupplier);
        replay(valueTransformerSupplier);
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
        verify(valueTransformerSupplier);
    }

    @Test
    public void testToInternalValueTransformerWithKeySupplierSuppliesNewTransformers() {
        final ValueTransformerWithKeySupplier<?, ?, ?> valueTransformerWithKeySupplier =
            createMock(ValueTransformerWithKeySupplier.class);
        expect(valueTransformerWithKeySupplier.get()).andReturn(null).times(3);
        replay(valueTransformerWithKeySupplier);
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
        valueTransformerWithKeySupplier.get();
        verify(valueTransformerWithKeySupplier);
    }

    @Test
    public void testShouldBeExtensible() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{1, 2, 3, 4, 5, 6, 7};
        final MockProcessorSupplier<Integer, String> supplier = new MockProcessorSupplier<>();
        final String topicName = "topic";

        final ExtendedKStream<Integer, String> stream = new ExtendedKStream<>(builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String())));

        stream.randomFilter().process(supplier);

        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "abstract-stream-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props);
        final TestInputTopic<Integer, String> inputTopic = driver.createInputTopic(topicName, new IntegerSerializer(), new StringSerializer());
        for (final int expectedKey : expectedKeys) {
            inputTopic.pipeInput(expectedKey, "V" + expectedKey);
        }

        assertTrue(supplier.theCapturedProcessor().processed().size() <= expectedKeys.length);
    }

    private static class ExtendedKStream<K, V> extends AbstractStream<K, V> {

        ExtendedKStream(final KStream<K, V> stream) {
            super((KStreamImpl<K, V>) stream);
        }

        KStream<K, V> randomFilter() {
            final String name = builder.newProcessorName("RANDOM-FILTER-");
            final ProcessorGraphNode<K, V> processorNode = new ProcessorGraphNode<>(
                name,
                new ProcessorParameters<>(new ExtendedKStreamDummy<>(), name));
            builder.addGraphNode(this.streamsGraphNode, processorNode);
            return new KStreamImpl<>(name, null, null, subTopologySourceNodes, false, processorNode, builder);
        }
    }

    private static class ExtendedKStreamDummy<K, V> implements ProcessorSupplier<K, V> {

        private final Random rand;

        ExtendedKStreamDummy() {
            rand = new Random();
        }

        @Override
        public Processor<K, V> get() {
            return new ExtendedKStreamDummyProcessor();
        }

        private class ExtendedKStreamDummyProcessor extends AbstractProcessor<K, V> {
            @Override
            public void process(final K key, final V value) {
                // flip a coin and filter
                if (rand.nextBoolean()) {
                    context().forward(key, value);
                }
            }
        }
    }
}
