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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.test.StreamsTestUtils.getMetricByName;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KTableKTableOuterJoinTest {
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final String output = "output";
    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final ConsumerRecordFactory<Integer, String> recordFactory =
            new ConsumerRecordFactory<>(Serdes.Integer().serializer(), Serdes.String().serializer(), 0L);
    private final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.outerJoin(table2, MockValueJoiner.TOSTRING_JOINER);
        joined.toStream().to(output);

        final Collection<Set<String>> copartitionGroups =
                TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            // push two items to the primary stream. the other table is empty
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
            }
            // pass tuple with null key, it will be discarded in join process
            driver.pipeInput(recordFactory.create(topic1, null, "SomeVal", 42L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            assertOutputKeyValueTimestamp(driver, 0, "X0+null", 5L);
            assertOutputKeyValueTimestamp(driver, 1, "X1+null", 6L);
            assertNull(driver.readOutput(output));

            // push two items to the other stream. this should produce two items.
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
            }
            // pass tuple with null key, it will be discarded in join process
            driver.pipeInput(recordFactory.create(topic2, null, "AnotherVal", 73L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            assertOutputKeyValueTimestamp(driver, 0, "X0+Y0", 5L);
            assertOutputKeyValueTimestamp(driver, 1, "X1+Y1", 10L);
            assertNull(driver.readOutput(output));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, 7L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            assertOutputKeyValueTimestamp(driver, 0, "XX0+Y0", 7L);
            assertOutputKeyValueTimestamp(driver, 1, "XX1+Y1", 10L);
            assertOutputKeyValueTimestamp(driver, 2, "XX2+null", 7L);
            assertOutputKeyValueTimestamp(driver, 3, "XX3+null", 7L);
            assertNull(driver.readOutput(output));

            // push all items to the other stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XX0+YY0", 7L);
            assertOutputKeyValueTimestamp(driver, 1, "XX1+YY1", 7L);
            assertOutputKeyValueTimestamp(driver, 2, "XX2+YY2", 10L);
            assertOutputKeyValueTimestamp(driver, 3, "XX3+YY3", 15L);
            assertNull(driver.readOutput(output));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXX" + expectedKey, 6L));
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XXX0+YY0", 6L);
            assertOutputKeyValueTimestamp(driver, 1, "XXX1+YY1", 6L);
            assertOutputKeyValueTimestamp(driver, 2, "XXX2+YY2", 10L);
            assertOutputKeyValueTimestamp(driver, 3, "XXX3+YY3", 15L);
            assertNull(driver.readOutput(output));

            // push two items with null to the other stream as deletes. this should produce two item.
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[0], null, 5L));
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[1], null, 7L));
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XXX0+null", 6L);
            assertOutputKeyValueTimestamp(driver, 1, "XXX1+null", 7L);
            assertNull(driver.readOutput(output));

            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, "XXXX0+null", 13L);
            assertOutputKeyValueTimestamp(driver, 1, "XXXX1+null", 13L);
            assertOutputKeyValueTimestamp(driver, 2, "XXXX2+YY2", 13L);
            assertOutputKeyValueTimestamp(driver, 3, "XXXX3+YY3", 15L);
            assertNull(driver.readOutput(output));

            // push four items to the primary stream with null. this should produce four items.
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[0], null, 0L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[1], null, 42L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[2], null, 5L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[3], null, 20L));
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            assertOutputKeyValueTimestamp(driver, 0, null, 0L);
            assertOutputKeyValueTimestamp(driver, 1, null, 42L);
            assertOutputKeyValueTimestamp(driver, 2, "null+YY2", 10L);
            assertOutputKeyValueTimestamp(driver, 3, "null+YY3", 20L);
            assertNull(driver.readOutput(output));
        }
    }

    @Test
    public void testNotSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier;

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.outerJoin(table2, MockValueJoiner.TOSTRING_JOINER);

        supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final MockProcessor<Integer, String> proc = supplier.theCapturedProcessor();

            assertTrue(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
            assertFalse(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
            }
            // pass tuple with null key, it will be discarded in join process
            driver.pipeInput(recordFactory.create(topic1, null, "SomeVal", 42L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+null", null), 5),
                    new KeyValueTimestamp<>(1, new Change<>("X1+null", null), 6));
            // push two items to the other stream. this should produce two items.
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
            }
            // pass tuple with null key, it will be discarded in join process
            driver.pipeInput(recordFactory.create(topic2, null, "AnotherVal", 73L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+Y0", null), 5),
                    new KeyValueTimestamp<>(1, new Change<>("X1+Y1", null), 10));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, 7L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+Y0", null), 7),
                    new KeyValueTimestamp<>(1, new Change<>("XX1+Y1", null), 10),
                    new KeyValueTimestamp<>(2, new Change<>("XX2+null", null), 7),
                    new KeyValueTimestamp<>(3, new Change<>("XX3+null", null), 7));
            // push all items to the other stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+YY0", null), 7),
                    new KeyValueTimestamp<>(1, new Change<>("XX1+YY1", null), 7),
                    new KeyValueTimestamp<>(2, new Change<>("XX2+YY2", null), 10),
                    new KeyValueTimestamp<>(3, new Change<>("XX3+YY3", null), 15));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXX" + expectedKey, 6L));
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+YY0", null), 6),
                    new KeyValueTimestamp<>(1, new Change<>("XXX1+YY1", null), 6),
                    new KeyValueTimestamp<>(2, new Change<>("XXX2+YY2", null), 10),
                    new KeyValueTimestamp<>(3, new Change<>("XXX3+YY3", null), 15));
            // push two items with null to the other stream as deletes. this should produce two item.
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[0], null, 5L));
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[1], null, 7L));
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+null", null), 6),
                    new KeyValueTimestamp<>(1, new Change<>("XXX1+null", null), 7));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXXX0+null", null), 13),
                    new KeyValueTimestamp<>(1, new Change<>("XXXX1+null", null), 13),
                    new KeyValueTimestamp<>(2, new Change<>("XXXX2+YY2", null), 13),
                    new KeyValueTimestamp<>(3, new Change<>("XXXX3+YY3", null), 15));
            // push four items to the primary stream with null. this should produce four items.
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[0], null, 0L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[1], null, 42L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[2], null, 5L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[3], null, 20L));
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>(null, null), 0),
                    new KeyValueTimestamp<>(1, new Change<>(null, null), 42),
                    new KeyValueTimestamp<>(2, new Change<>("null+YY2", null), 10),
                    new KeyValueTimestamp<>(3, new Change<>("null+YY3", null), 20));
        }
    }

    @Test
    public void testSendingOldValue() {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KTable<Integer, String> table1;
        final KTable<Integer, String> table2;
        final KTable<Integer, String> joined;
        final MockProcessorSupplier<Integer, String> supplier;

        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        joined = table1.outerJoin(table2, MockValueJoiner.TOSTRING_JOINER);

        ((KTableImpl<?, ?, ?>) joined).enableSendingOldValues();

        supplier = new MockProcessorSupplier<>();
        final Topology topology = builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            final MockProcessor<Integer, String> proc = supplier.theCapturedProcessor();

            assertTrue(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
            assertTrue(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());

            // push two items to the primary stream. the other table is empty
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic1, expectedKeys[i], "X" + expectedKeys[i], 5L + i));
            }
            // pass tuple with null key, it will be discarded in join process
            driver.pipeInput(recordFactory.create(topic1, null, "SomeVal", 42L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right:
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+null", null), 5),
                    new KeyValueTimestamp<>(1, new Change<>("X1+null", null), 6));
            // push two items to the other stream. this should produce two items.
            for (int i = 0; i < 2; i++) {
                driver.pipeInput(recordFactory.create(topic2, expectedKeys[i], "Y" + expectedKeys[i], 10L * i));
            }
            // pass tuple with null key, it will be discarded in join process
            driver.pipeInput(recordFactory.create(topic2, null, "AnotherVal", 73L));
            // left: X0:0 (ts: 5), X1:1 (ts: 6)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("X0+Y0", "X0+null"), 5),
                    new KeyValueTimestamp<>(1, new Change<>("X1+Y1", "X1+null"), 10));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XX" + expectedKey, 7L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: Y0:0 (ts: 0), Y1:1 (ts: 10)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+Y0", "X0+Y0"), 7),
                    new KeyValueTimestamp<>(1, new Change<>("XX1+Y1", "X1+Y1"), 10),
                    new KeyValueTimestamp<>(2, new Change<>("XX2+null", null), 7),
                    new KeyValueTimestamp<>(3, new Change<>("XX3+null", null), 7));
            // push all items to the other stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic2, expectedKey, "YY" + expectedKey, expectedKey * 5L));
            }
            // left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XX0+YY0", "XX0+Y0"), 7),
                    new KeyValueTimestamp<>(1, new Change<>("XX1+YY1", "XX1+Y1"), 7),
                    new KeyValueTimestamp<>(2, new Change<>("XX2+YY2", "XX2+null"), 10),
                    new KeyValueTimestamp<>(3, new Change<>("XX3+YY3", "XX3+null"), 15));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXX" + expectedKey, 6L));
            }
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+YY0", "XX0+YY0"), 6),
                    new KeyValueTimestamp<>(1, new Change<>("XXX1+YY1", "XX1+YY1"), 6),
                    new KeyValueTimestamp<>(2, new Change<>("XXX2+YY2", "XX2+YY2"), 10),
                    new KeyValueTimestamp<>(3, new Change<>("XXX3+YY3", "XX3+YY3"), 15));
            // push two items with null to the other stream as deletes. this should produce two item.
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[0], null, 5L));
            driver.pipeInput(recordFactory.create(topic2, expectedKeys[1], null, 7L));
            // left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXX0+null", "XXX0+YY0"), 6),
                    new KeyValueTimestamp<>(1, new Change<>("XXX1+null", "XXX1+YY1"), 7));
            // push all four items to the primary stream. this should produce four items.
            for (final int expectedKey : expectedKeys) {
                driver.pipeInput(recordFactory.create(topic1, expectedKey, "XXXX" + expectedKey, 13L));
            }
            // left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>("XXXX0+null", "XXX0+null"), 13),
                    new KeyValueTimestamp<>(1, new Change<>("XXXX1+null", "XXX1+null"), 13),
                    new KeyValueTimestamp<>(2, new Change<>("XXXX2+YY2", "XXX2+YY2"), 13),
                    new KeyValueTimestamp<>(3, new Change<>("XXXX3+YY3", "XXX3+YY3"), 15));
            // push four items to the primary stream with null. this should produce four items.
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[0], null, 0L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[1], null, 42L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[2], null, 5L));
            driver.pipeInput(recordFactory.create(topic1, expectedKeys[3], null, 20L));
            // left:
            // right: YY2:2 (ts: 10), YY3:3 (ts: 15)
            proc.checkAndClearProcessResult(new KeyValueTimestamp<>(0, new Change<>(null, "XXXX0+null"), 0),
                    new KeyValueTimestamp<>(1, new Change<>(null, "XXXX1+null"), 42),
                    new KeyValueTimestamp<>(2, new Change<>("null+YY2", "XXXX2+YY2"), 10),
                    new KeyValueTimestamp<>(3, new Change<>("null+YY3", "XXXX3+YY3"), 20));
        }
    }

    @Test
    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey() {
        final StreamsBuilder builder = new StreamsBuilder();

        @SuppressWarnings("unchecked")
        final Processor<String, Change<String>> join = new KTableKTableOuterJoin<>(
                (KTableImpl<String, String, String>) builder.table("left", Consumed.with(Serdes.String(), Serdes.String())),
                (KTableImpl<String, String, String>) builder.table("right", Consumed.with(Serdes.String(), Serdes.String())),
                null
        ).get();

        final MockProcessorContext context = new MockProcessorContext();
        context.setRecordMetadata("left", -1, -2, null, -3);
        join.init(context);
        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
        join.process(null, new Change<>("new", "old"));
        LogCaptureAppender.unregister(appender);

        assertEquals(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
        assertThat(appender.getMessages(), hasItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
    }

    private void assertOutputKeyValueTimestamp(final TopologyTestDriver driver,
                                               final Integer expectedKey,
                                               final String expectedValue,
                                               final long expectedTimestamp) {
        OutputVerifier.compareKeyValueTimestamp(
                driver.readOutput(output, Serdes.Integer().deserializer(), Serdes.String().deserializer()),
                expectedKey,
                expectedValue,
                expectedTimestamp);
    }

}
