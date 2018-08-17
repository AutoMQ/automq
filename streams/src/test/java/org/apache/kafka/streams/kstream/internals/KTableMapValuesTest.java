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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverWrapper;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableMapValuesTest {

    private final Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String());
    private final Produced<String, String> produced = Produced.with(Serdes.String(), Serdes.String());
    private final ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    private void doTestKTable(final StreamsBuilder builder, final String topic1, final MockProcessorSupplier<String, Integer> supplier) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(topic1, "A", "1"));
            driver.pipeInput(recordFactory.create(topic1, "B", "2"));
            driver.pipeInput(recordFactory.create(topic1, "C", "3"));
            driver.pipeInput(recordFactory.create(topic1, "D", "4"));
            assertEquals(Utils.mkList("A:1", "B:2", "C:3", "D:4"), supplier.theCapturedProcessor().processed);
        }
    }

    @Test
    public void testKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(new ValueMapper<CharSequence, Integer>() {
            @Override
            public Integer apply(final CharSequence value) {
                return value.charAt(0) - 48;
            }
        });

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);

        doTestKTable(builder, topic1, supplier);
    }

    @Test
    public void testQueryableKTable() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTable<String, String> table1 = builder.table(topic1, consumed);
        final KTable<String, Integer> table2 = table1.mapValues(new ValueMapper<CharSequence, Integer>() {
            @Override
            public Integer apply(final CharSequence value) {
                return value.charAt(0) - 48;
            }
        }, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("anyName").withValueSerde(Serdes.Integer()));

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();
        table2.toStream().process(supplier);

        doTestKTable(builder, topic1, supplier);
    }

    private void doTestValueGetter(final StreamsBuilder builder,
                                   final String topic1,
                                   final KTableImpl<String, String, String> table1,
                                   final KTableImpl<String, String, Integer> table2,
                                   final KTableImpl<String, Integer, Integer> table3,
                                   final KTableImpl<String, String, String> table4) {

        final Topology topology = builder.build();

        final KTableValueGetterSupplier<String, String> getterSupplier1 = table1.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier2 = table2.valueGetterSupplier();
        final KTableValueGetterSupplier<String, Integer> getterSupplier3 = table3.valueGetterSupplier();
        final KTableValueGetterSupplier<String, String> getterSupplier4 = table4.valueGetterSupplier();

        final InternalTopologyBuilder topologyBuilder = TopologyWrapper.getInternalTopologyBuilder(topology);
        topologyBuilder.connectProcessorAndStateStores(table1.name, getterSupplier1.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table2.name, getterSupplier2.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table3.name, getterSupplier3.storeNames());
        topologyBuilder.connectProcessorAndStateStores(table4.name, getterSupplier4.storeNames());

        try (final TopologyTestDriverWrapper driver = new TopologyTestDriverWrapper(builder.build(), props)) {
            final KTableValueGetter<String, String> getter1 = getterSupplier1.get();
            final KTableValueGetter<String, Integer> getter2 = getterSupplier2.get();
            final KTableValueGetter<String, Integer> getter3 = getterSupplier3.get();
            final KTableValueGetter<String, String> getter4 = getterSupplier4.get();

            getter1.init(driver.setCurrentNodeForProcessorContext(table1.name));
            getter2.init(driver.setCurrentNodeForProcessorContext(table2.name));
            getter3.init(driver.setCurrentNodeForProcessorContext(table3.name));
            getter4.init(driver.setCurrentNodeForProcessorContext(table4.name));

            driver.pipeInput(recordFactory.create(topic1, "A", "01"));
            driver.pipeInput(recordFactory.create(topic1, "B", "01"));
            driver.pipeInput(recordFactory.create(topic1, "C", "01"));

            assertEquals("01", getter1.get("A"));
            assertEquals("01", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            assertEquals(new Integer(1), getter2.get("A"));
            assertEquals(new Integer(1), getter2.get("B"));
            assertEquals(new Integer(1), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertNull(getter3.get("B"));
            assertNull(getter3.get("C"));

            assertEquals("01", getter4.get("A"));
            assertEquals("01", getter4.get("B"));
            assertEquals("01", getter4.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "02"));
            driver.pipeInput(recordFactory.create(topic1, "B", "02"));

            assertEquals("02", getter1.get("A"));
            assertEquals("02", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            assertEquals(new Integer(2), getter2.get("A"));
            assertEquals(new Integer(2), getter2.get("B"));
            assertEquals(new Integer(1), getter2.get("C"));

            assertEquals(new Integer(2), getter3.get("A"));
            assertEquals(new Integer(2), getter3.get("B"));
            assertNull(getter3.get("C"));

            assertEquals("02", getter4.get("A"));
            assertEquals("02", getter4.get("B"));
            assertEquals("01", getter4.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", "03"));

            assertEquals("03", getter1.get("A"));
            assertEquals("02", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            assertEquals(new Integer(3), getter2.get("A"));
            assertEquals(new Integer(2), getter2.get("B"));
            assertEquals(new Integer(1), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertEquals(new Integer(2), getter3.get("B"));
            assertNull(getter3.get("C"));

            assertEquals("03", getter4.get("A"));
            assertEquals("02", getter4.get("B"));
            assertEquals("01", getter4.get("C"));

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null));

            assertNull(getter1.get("A"));
            assertEquals("02", getter1.get("B"));
            assertEquals("01", getter1.get("C"));

            assertNull(getter2.get("A"));
            assertEquals(new Integer(2), getter2.get("B"));
            assertEquals(new Integer(1), getter2.get("C"));

            assertNull(getter3.get("A"));
            assertEquals(new Integer(2), getter3.get("B"));
            assertNull(getter3.get("C"));

            assertNull(getter4.get("A"));
            assertEquals("02", getter4.get("B"));
            assertEquals("01", getter4.get("C"));
        }
    }

    @Test
    public void testValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(final String value) {
                        return new Integer(value);
                    }
                });
        final KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
                new Predicate<String, Integer>() {
                    @Override
                    public boolean test(final String key, final Integer value) {
                        return (value % 2) == 0;
                    }
                });
        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);
    }

    @Test
    public void testQueryableValueGetter() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";
        final String topic2 = "topic2";
        final String storeName2 = "anyMapName";
        final String storeName3 = "anyFilterName";

        final KTableImpl<String, String, String> table1 =
            (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
            new ValueMapper<String, Integer>() {
                @Override
                public Integer apply(final String value) {
                    return new Integer(value);
                }
            }, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName2).withValueSerde(Serdes.Integer()));
        final KTableImpl<String, Integer, Integer> table3 = (KTableImpl<String, Integer, Integer>) table2.filter(
            new Predicate<String, Integer>() {
                @Override
                public boolean test(final String key, final Integer value) {
                    return (value % 2) == 0;
                }
            }, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(storeName3).withValueSerde(Serdes.Integer()));
        table1.toStream().to(topic2, produced);
        final KTableImpl<String, String, String> table4 = (KTableImpl<String, String, String>) builder.table(topic2, consumed);

        doTestValueGetter(builder, topic1, table1, table2, table3, table4);
    }

    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(final String value) {
                        return new Integer(value);
                    }
                });

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        final Topology topology = builder.build().addProcessor("proc", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {

            final MockProcessor<String, Integer> proc = supplier.theCapturedProcessor();

            assertFalse(table1.sendingOldValueEnabled());
            assertFalse(table2.sendingOldValueEnabled());

            driver.pipeInput(recordFactory.create(topic1, "A", "01"));
            driver.pipeInput(recordFactory.create(topic1, "B", "01"));
            driver.pipeInput(recordFactory.create(topic1, "C", "01"));

            proc.checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", "02"));
            driver.pipeInput(recordFactory.create(topic1, "B", "02"));

            proc.checkAndClearProcessResult("A:(2<-null)", "B:(2<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", "03"));

            proc.checkAndClearProcessResult("A:(3<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null));

            proc.checkAndClearProcessResult("A:(null<-null)");
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String topic1 = "topic1";

        final KTableImpl<String, String, String> table1 =
                (KTableImpl<String, String, String>) builder.table(topic1, consumed);
        final KTableImpl<String, String, Integer> table2 = (KTableImpl<String, String, Integer>) table1.mapValues(
                new ValueMapper<String, Integer>() {
                    @Override
                    public Integer apply(final String value) {
                        return new Integer(value);
                    }
                });

        table2.enableSendingOldValues();

        final MockProcessorSupplier<String, Integer> supplier = new MockProcessorSupplier<>();

        builder.build().addProcessor("proc", supplier, table2.name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {

            final MockProcessor<String, Integer> proc = supplier.theCapturedProcessor();

            assertTrue(table1.sendingOldValueEnabled());
            assertTrue(table2.sendingOldValueEnabled());

            driver.pipeInput(recordFactory.create(topic1, "A", "01"));
            driver.pipeInput(recordFactory.create(topic1, "B", "01"));
            driver.pipeInput(recordFactory.create(topic1, "C", "01"));

            proc.checkAndClearProcessResult("A:(1<-null)", "B:(1<-null)", "C:(1<-null)");

            driver.pipeInput(recordFactory.create(topic1, "A", "02"));
            driver.pipeInput(recordFactory.create(topic1, "B", "02"));

            proc.checkAndClearProcessResult("A:(2<-1)", "B:(2<-1)");

            driver.pipeInput(recordFactory.create(topic1, "A", "03"));

            proc.checkAndClearProcessResult("A:(3<-2)");

            driver.pipeInput(recordFactory.create(topic1, "A", (String) null));

            proc.checkAndClearProcessResult("A:(null<-3)");
        }
    }
}
