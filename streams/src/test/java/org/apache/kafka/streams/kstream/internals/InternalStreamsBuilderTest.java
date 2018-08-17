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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.Topology.AutoOffsetReset;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class InternalStreamsBuilderTest {

    private static final String APP_ID = "app-id";

    private final InternalStreamsBuilder builder = new InternalStreamsBuilder(new InternalTopologyBuilder());
    private final ConsumedInternal<String, String> consumed = new ConsumedInternal<>();
    private final String storePrefix = "prefix-";
    private final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized = new MaterializedInternal<>(Materialized.as("test-store"));

    {
        materialized.generateStoreNameIfNeeded(builder, storePrefix);
    }

    @Test
    public void testNewName() {
        assertEquals("X-0000000000", builder.newProcessorName("X-"));
        assertEquals("Y-0000000001", builder.newProcessorName("Y-"));
        assertEquals("Z-0000000002", builder.newProcessorName("Z-"));

        final InternalStreamsBuilder newBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());

        assertEquals("X-0000000000", newBuilder.newProcessorName("X-"));
        assertEquals("Y-0000000001", newBuilder.newProcessorName("Y-"));
        assertEquals("Z-0000000002", newBuilder.newProcessorName("Z-"));
    }

    @Test
    public void testNewStoreName() {
        assertEquals("X-STATE-STORE-0000000000", builder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", builder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", builder.newStoreName("Z-"));

        final InternalStreamsBuilder newBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());

        assertEquals("X-STATE-STORE-0000000000", newBuilder.newStoreName("X-"));
        assertEquals("Y-STATE-STORE-0000000001", newBuilder.newStoreName("Y-"));
        assertEquals("Z-STATE-STORE-0000000002", newBuilder.newStoreName("Z-"));
    }

    @Test
    public void shouldHaveCorrectSourceTopicsForTableFromMergedStream() {
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final String topic3 = "topic-3";
        final KStream<String, String> source1 = builder.stream(Collections.singleton(topic1), consumed);
        final KStream<String, String> source2 = builder.stream(Collections.singleton(topic2), consumed);
        final KStream<String, String> source3 = builder.stream(Collections.singleton(topic3), consumed);
        final KStream<String, String> processedSource1 =
                source1.mapValues(new ValueMapper<String, String>() {
                    @Override
                    public String apply(final String value) {
                        return value;
                    }
                }).filter(new Predicate<String, String>() {
                    @Override
                    public boolean test(final String key, final String value) {
                        return true;
                    }
                });
        final KStream<String, String> processedSource2 = source2.filter(new Predicate<String, String>() {
            @Override
            public boolean test(final String key, final String value) {
                return true;
            }
        });

        final KStream<String, String> merged = processedSource1.merge(processedSource2).merge(source3);
        merged.groupByKey().count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("my-table"));
        builder.buildAndOptimizeTopology();
        final Map<String, List<String>> actual = builder.internalTopologyBuilder.stateStoreNameToSourceTopics();
        assertEquals(Utils.mkList("topic-1", "topic-2", "topic-3"), actual.get("my-table"));
    }

    @Test
    public void shouldStillMaterializeSourceKTableIfMaterializedIsntQueryable() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.with(null, null));
        materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
        final KTable table1 = builder.table("topic2", consumed, materializedInternal);

        builder.buildAndOptimizeTopology();
        final ProcessorTopology topology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .build(null);

        assertEquals(1, topology.stateStores().size());
        final String storeName = "prefix-STATE-STORE-0000000000";
        assertEquals(storeName, topology.stateStores().get(0).name());

        assertEquals(1, topology.storeToChangelogTopic().size());
        assertEquals("app-id-prefix-STATE-STORE-0000000000-changelog", topology.storeToChangelogTopic().get(storeName));
        assertNull(table1.queryableStoreName());
    }
    
    @Test
    public void shouldBuildGlobalTableWithNonQueryableStoreName() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.with(null, null));
        materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);

        final GlobalKTable<String, String> table1 = builder.globalTable("topic2", consumed, materializedInternal);

        assertNull(table1.queryableStoreName());
    }

    @Test
    public void shouldBuildGlobalTableWithQueryaIbleStoreName() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as("globalTable"));
        materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
        final GlobalKTable<String, String> table1 = builder.globalTable("topic2", consumed, materializedInternal);

        assertEquals("globalTable", table1.queryableStoreName());
    }

    @Test
    public void shouldBuildSimpleGlobalTableTopology() {
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as("globalTable"));
        materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
        builder.globalTable("table",
                            consumed,
            materializedInternal);

        builder.buildAndOptimizeTopology();
        final ProcessorTopology topology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildGlobalStateTopology();
        final List<StateStore> stateStores = topology.globalStateStores();

        assertEquals(1, stateStores.size());
        assertEquals("globalTable", stateStores.get(0).name());
    }

    private void doBuildGlobalTopologyWithAllGlobalTables() {
        final ProcessorTopology topology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .buildGlobalStateTopology();

        final List<StateStore> stateStores = topology.globalStateStores();
        final Set<String> sourceTopics = topology.sourceTopics();

        assertEquals(Utils.mkSet("table", "table2"), sourceTopics);
        assertEquals(2, stateStores.size());
    }

    @Test
    public void shouldBuildGlobalTopologyWithAllGlobalTables() {
        {
            final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(Materialized.as("global1"));
            materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
            builder.globalTable("table", consumed, materializedInternal);
        }
        {
            final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
                new MaterializedInternal<>(Materialized.as("global2"));
            materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
            builder.globalTable("table2", consumed, materializedInternal);
        }

        builder.buildAndOptimizeTopology();
        doBuildGlobalTopologyWithAllGlobalTables();
    }

    @Test
    public void shouldAddGlobalTablesToEachGroup() {
        final String one = "globalTable";
        final String two = "globalTable2";

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as(one));
        materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
        final GlobalKTable<String, String> globalTable = builder.globalTable("table", consumed, materializedInternal);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal2 =
            new MaterializedInternal<>(Materialized.as(two));
        materializedInternal2.generateStoreNameIfNeeded(builder, storePrefix);
        final GlobalKTable<String, String> globalTable2 = builder.globalTable("table2", consumed, materializedInternal2);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternalNotGlobal =
            new MaterializedInternal<>(Materialized.as("not-global"));
        materializedInternalNotGlobal.generateStoreNameIfNeeded(builder, storePrefix);
        builder.table("not-global", consumed, materializedInternalNotGlobal);

        final KeyValueMapper<String, String, String> kvMapper = (key, value) -> value;

        final KStream<String, String> stream = builder.stream(Collections.singleton("t1"), consumed);
        stream.leftJoin(globalTable, kvMapper, MockValueJoiner.TOSTRING_JOINER);
        final KStream<String, String> stream2 = builder.stream(Collections.singleton("t2"), consumed);
        stream2.leftJoin(globalTable2, kvMapper, MockValueJoiner.TOSTRING_JOINER);

        final Map<Integer, Set<String>> nodeGroups = builder.internalTopologyBuilder.nodeGroups();
        for (final Integer groupId : nodeGroups.keySet()) {
            final ProcessorTopology topology = builder.internalTopologyBuilder.build(groupId);
            final List<StateStore> stateStores = topology.globalStateStores();
            final Set<String> names = new HashSet<>();
            for (final StateStore stateStore : stateStores) {
                names.add(stateStore.name());
            }

            assertEquals(2, stateStores.size());
            assertTrue(names.contains(one));
            assertTrue(names.contains(two));
        }
    }

    @Test
    public void shouldMapStateStoresToCorrectSourceTopics() {
        final KStream<String, String> playEvents = builder.stream(Collections.singleton("events"), consumed);

        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materializedInternal =
            new MaterializedInternal<>(Materialized.as("table-store"));
        materializedInternal.generateStoreNameIfNeeded(builder, storePrefix);
        final KTable<String, String> table = builder.table("table-topic", consumed, materializedInternal);


        final KStream<String, String> mapped = playEvents.map(MockMapper.<String, String>selectValueKeyValueMapper());
        mapped.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).groupByKey().count(Materialized.as("count"));
        builder.buildAndOptimizeTopology();
        builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)));
        assertEquals(Collections.singletonList("table-topic"), builder.internalTopologyBuilder.stateStoreNameToSourceTopics().get("table-store"));
        assertEquals(Collections.singletonList(APP_ID + "-KSTREAM-MAP-0000000003-repartition"), builder.internalTopologyBuilder.stateStoreNameToSourceTopics().get("count"));
    }

    @Test
    public void shouldAddTopicToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        final ConsumedInternal consumed = new ConsumedInternal<>(Consumed.with(AutoOffsetReset.EARLIEST));
        builder.stream(Collections.singleton(topicName), consumed);
        builder.buildAndOptimizeTopology();

        assertTrue(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTopicToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";

        final ConsumedInternal consumed = new ConsumedInternal<>(Consumed.with(AutoOffsetReset.LATEST));
        builder.stream(Collections.singleton(topicName), consumed);
        builder.buildAndOptimizeTopology();
        assertTrue(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTableToEarliestAutoOffsetResetList() {
        final String topicName = "topic-1";
        builder.table(topicName, new ConsumedInternal<>(Consumed.<String, String>with(AutoOffsetReset.EARLIEST)), materialized);
        builder.buildAndOptimizeTopology();
        assertTrue(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldAddTableToLatestAutoOffsetResetList() {
        final String topicName = "topic-1";
        builder.table(topicName, new ConsumedInternal<>(Consumed.<String, String>with(AutoOffsetReset.LATEST)), materialized);
        builder.buildAndOptimizeTopology();
        assertTrue(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldNotAddTableToOffsetResetLists() {
        final String topicName = "topic-1";

        builder.table(topicName, consumed, materialized);

        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicName).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicName).matches());
    }

    @Test
    public void shouldNotAddRegexTopicsToOffsetResetLists() {
        final Pattern topicPattern = Pattern.compile("topic-\\d");
        final String topic = "topic-5";

        builder.stream(topicPattern, consumed);

        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topic).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topic).matches());

    }

    @Test
    public void shouldAddRegexTopicToEarliestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-500000";

        builder.stream(topicPattern, new ConsumedInternal<>(Consumed.with(AutoOffsetReset.EARLIEST)));
        builder.buildAndOptimizeTopology();

        assertTrue(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicTwo).matches());
        assertFalse(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicTwo).matches());
    }

    @Test
    public void shouldAddRegexTopicToLatestAutoOffsetResetList() {
        final Pattern topicPattern = Pattern.compile("topic-\\d+");
        final String topicTwo = "topic-1000000";

        builder.stream(topicPattern, new ConsumedInternal<>(Consumed.with(AutoOffsetReset.LATEST)));
        builder.buildAndOptimizeTopology();

        assertTrue(builder.internalTopologyBuilder.latestResetTopicsPattern().matcher(topicTwo).matches());
        assertFalse(builder.internalTopologyBuilder.earliestResetTopicsPattern().matcher(topicTwo).matches());
    }

    @Test
    public void shouldHaveNullTimestampExtractorWhenNoneSupplied() {
        builder.stream(Collections.singleton("topic"), consumed);
        builder.buildAndOptimizeTopology();
        builder.internalTopologyBuilder.rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)));
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder.build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void shouldUseProvidedTimestampExtractor() {
        final ConsumedInternal consumed = new ConsumedInternal<>(Consumed.with(new MockTimestampExtractor()));
        builder.stream(Collections.singleton("topic"), consumed);
        builder.buildAndOptimizeTopology();
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    @Test
    public void ktableShouldHaveNullTimestampExtractorWhenNoneSupplied() {
        builder.table("topic", consumed, materialized);
        builder.buildAndOptimizeTopology();
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .build(null);
        assertNull(processorTopology.source("topic").getTimestampExtractor());
    }

    @Test
    public void ktableShouldUseProvidedTimestampExtractor() {
        final ConsumedInternal<String, String> consumed = new ConsumedInternal<>(Consumed.<String, String>with(new MockTimestampExtractor()));
        builder.table("topic", consumed, materialized);
        builder.buildAndOptimizeTopology();
        final ProcessorTopology processorTopology = builder.internalTopologyBuilder
            .rewriteTopology(new StreamsConfig(StreamsTestUtils.getStreamsConfig(APP_ID)))
            .build(null);
        assertThat(processorTopology.source("topic").getTimestampExtractor(), instanceOf(MockTimestampExtractor.class));
    }

    // TODO: this static functions are added because some non-TopologyBuilder unit tests need to access the internal topology builder,
    //       which is usually a bad sign of design patterns between TopologyBuilder and StreamThread. We need to consider getting rid of them later
    public static InternalTopologyBuilder internalTopologyBuilder(final InternalStreamsBuilder internalStreamsBuilder) {
        return internalStreamsBuilder.internalTopologyBuilder;
    }
}