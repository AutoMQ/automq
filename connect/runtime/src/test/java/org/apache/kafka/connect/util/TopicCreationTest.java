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

package org.apache.kafka.connect.util;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfigTest.MOCK_PLUGINS;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_GROUPS_CONFIG;
import static org.apache.kafka.connect.runtime.SourceConnectorConfig.TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_GROUP;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.EXCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.INCLUDE_REGEX_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TopicCreationTest {

    private static final String FOO_CONNECTOR = "foo-source";
    private static final String FOO_GROUP = "foo";
    private static final String FOO_TOPIC = "foo-topic";
    private static final String FOO_REGEX = ".*foo.*";

    private static final String BAR_GROUP = "bar";
    private static final String BAR_TOPIC = "bar-topic";
    private static final String BAR_REGEX = ".*bar.*";

    private static final short DEFAULT_REPLICATION_FACTOR = -1;
    private static final int DEFAULT_PARTITIONS = -1;

    Map<String, String> workerProps;
    WorkerConfig workerConfig;
    Map<String, String> sourceProps;
    SourceConnectorConfig sourceConfig;

    @Before
    public void setup() {
        workerProps = defaultWorkerProps();
        workerConfig = new DistributedConfig(workerProps);
    }

    public Map<String, String> defaultWorkerProps() {
        Map<String, String> props = new HashMap<>();
        props.put(GROUP_ID_CONFIG, "connect-cluster");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(CONFIG_TOPIC_CONFIG, "connect-configs");
        props.put(OFFSET_STORAGE_TOPIC_CONFIG, "connect-offsets");
        props.put(STATUS_STORAGE_TOPIC_CONFIG, "connect-status");
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(true));
        return props;
    }

    public Map<String, String> defaultConnectorProps() {
        Map<String, String> props = new HashMap<>();
        props.put(NAME_CONFIG, FOO_CONNECTOR);
        props.put(CONNECTOR_CLASS_CONFIG, "TestConnector");
        return props;
    }

    public Map<String, String> defaultConnectorPropsWithTopicCreation() {
        Map<String, String> props = defaultConnectorProps();
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(DEFAULT_REPLICATION_FACTOR));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(DEFAULT_PARTITIONS));
        return props;
    }

    @Test
    public void testTopicCreationWhenTopicCreationIsEnabled() {
        sourceProps = defaultConnectorPropsWithTopicCreation();
        sourceProps.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP, BAR_GROUP));
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);

        Map<String, TopicCreationGroup> groups = TopicCreationGroup.configuredGroups(sourceConfig);
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, groups);

        assertTrue(topicCreation.isTopicCreationEnabled());
        assertTrue(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertThat(topicCreation.defaultTopicGroup(), is(groups.get(DEFAULT_TOPIC_CREATION_GROUP)));
        assertEquals(2, topicCreation.topicGroups().size());
        assertThat(topicCreation.topicGroups().keySet(), hasItems(FOO_GROUP, BAR_GROUP));
        assertEquals(topicCreation.defaultTopicGroup(), topicCreation.findFirstGroup(FOO_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
    }

    @Test
    public void testTopicCreationWhenTopicCreationIsDisabled() {
        workerProps.put(TOPIC_CREATION_ENABLE_CONFIG, String.valueOf(false));
        workerConfig = new DistributedConfig(workerProps);
        sourceProps = defaultConnectorPropsWithTopicCreation();
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);

        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig,
                TopicCreationGroup.configuredGroups(sourceConfig));

        assertFalse(topicCreation.isTopicCreationEnabled());
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertNull(topicCreation.defaultTopicGroup());
        assertThat(topicCreation.topicGroups(), is(Collections.emptyMap()));
        assertNull(topicCreation.findFirstGroup(FOO_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
    }

    @Test
    public void testEmptyTopicCreation() {
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, null);

        assertEquals(TopicCreation.empty(), topicCreation);
        assertFalse(topicCreation.isTopicCreationEnabled());
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertNull(topicCreation.defaultTopicGroup());
        assertEquals(0, topicCreation.topicGroups().size());
        assertThat(topicCreation.topicGroups(), is(Collections.emptyMap()));
        assertNull(topicCreation.findFirstGroup(FOO_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
    }

    @Test
    public void withDefaultTopicCreation() {
        sourceProps = defaultConnectorPropsWithTopicCreation();
        // Setting here but they should be ignored for the default group
        sourceProps.put(TOPIC_CREATION_PREFIX + DEFAULT_TOPIC_CREATION_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_REGEX);
        sourceProps.put(TOPIC_CREATION_PREFIX + DEFAULT_TOPIC_CREATION_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_REGEX);

        // verify config creation
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);
        assertTrue(sourceConfig.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) sourceConfig.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(DEFAULT_PARTITIONS, (int) sourceConfig.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(sourceConfig.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(sourceConfig.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(sourceConfig.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        // verify topic creation group is instantiated correctly
        Map<String, TopicCreationGroup> groups = TopicCreationGroup.configuredGroups(sourceConfig);
        assertEquals(1, groups.size());
        assertThat(groups.keySet(), hasItem(DEFAULT_TOPIC_CREATION_GROUP));

        // verify topic creation
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, groups);
        TopicCreationGroup group = topicCreation.defaultTopicGroup();
        // Default group will match all topics besides empty string
        assertTrue(group.matches(" "));
        assertTrue(group.matches(FOO_TOPIC));
        assertEquals(DEFAULT_TOPIC_CREATION_GROUP, group.name());
        assertTrue(topicCreation.isTopicCreationEnabled());
        assertTrue(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertThat(topicCreation.topicGroups(), is(Collections.emptyMap()));
        assertEquals(topicCreation.defaultTopicGroup(), topicCreation.findFirstGroup(FOO_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));

        // verify new topic properties
        NewTopic topicSpec = topicCreation.findFirstGroup(FOO_TOPIC).newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, topicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, topicSpec.replicationFactor());
        assertEquals(DEFAULT_PARTITIONS, topicSpec.numPartitions());
        assertThat(topicSpec.configs(), is(Collections.emptyMap()));
    }

    @Test
    public void topicCreationWithDefaultGroupAndCustomProps() {
        short replicas = 3;
        int partitions = 5;
        long retentionMs = TimeUnit.DAYS.toMillis(30);
        String compressionType = "lz4";
        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(COMPRESSION_TYPE_CONFIG, compressionType);
        topicProps.put(RETENTION_MS_CONFIG, String.valueOf(retentionMs));

        sourceProps = defaultConnectorPropsWithTopicCreation();
        sourceProps.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(replicas));
        sourceProps.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        topicProps.forEach((k, v) -> sourceProps.put(DEFAULT_TOPIC_CREATION_PREFIX + k, v));
        // Setting here but they should be ignored for the default group
        sourceProps.put(TOPIC_CREATION_PREFIX + DEFAULT_TOPIC_CREATION_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_REGEX);
        sourceProps.put(TOPIC_CREATION_PREFIX + DEFAULT_TOPIC_CREATION_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_REGEX);

        // verify config creation
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);
        assertTrue(sourceConfig.usesTopicCreation());
        assertEquals(replicas, (short) sourceConfig.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) sourceConfig.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(sourceConfig.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(sourceConfig.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(sourceConfig.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(topicProps));

        // verify topic creation group is instantiated correctly
        Map<String, TopicCreationGroup> groups = TopicCreationGroup.configuredGroups(sourceConfig);
        assertEquals(1, groups.size());
        assertThat(groups.keySet(), hasItem(DEFAULT_TOPIC_CREATION_GROUP));

        // verify topic creation
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, groups);
        TopicCreationGroup group = topicCreation.defaultTopicGroup();
        // Default group will match all topics besides empty string
        assertTrue(group.matches(" "));
        assertTrue(group.matches(FOO_TOPIC));
        assertEquals(DEFAULT_TOPIC_CREATION_GROUP, group.name());
        assertTrue(topicCreation.isTopicCreationEnabled());
        assertTrue(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertThat(topicCreation.topicGroups(), is(Collections.emptyMap()));
        assertEquals(topicCreation.defaultTopicGroup(), topicCreation.findFirstGroup(FOO_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));

        // verify new topic properties
        NewTopic topicSpec = topicCreation.findFirstGroup(FOO_TOPIC).newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, topicSpec.name());
        assertEquals(replicas, topicSpec.replicationFactor());
        assertEquals(partitions, topicSpec.numPartitions());
        assertThat(topicSpec.configs(), is(topicProps));
    }

    @Test
    public void topicCreationWithOneGroup() {
        short fooReplicas = 3;
        int partitions = 5;
        sourceProps = defaultConnectorPropsWithTopicCreation();
        sourceProps.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP));
        sourceProps.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_REGEX);
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + EXCLUDE_REGEX_CONFIG, BAR_REGEX);
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + REPLICATION_FACTOR_CONFIG, String.valueOf(fooReplicas));

        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        topicProps.forEach((k, v) -> sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + k, v));

        // verify config creation
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);
        assertTrue(sourceConfig.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) sourceConfig.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) sourceConfig.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(sourceConfig.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(sourceConfig.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(sourceConfig.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        // verify topic creation group is instantiated correctly
        Map<String, TopicCreationGroup> groups = TopicCreationGroup.configuredGroups(sourceConfig);
        assertEquals(2, groups.size());
        assertThat(groups.keySet(), hasItems(DEFAULT_TOPIC_CREATION_GROUP, FOO_GROUP));

        // verify topic creation
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, groups);
        TopicCreationGroup defaultGroup = topicCreation.defaultTopicGroup();
        // Default group will match all topics besides empty string
        assertTrue(defaultGroup.matches(" "));
        assertTrue(defaultGroup.matches(FOO_TOPIC));
        assertTrue(defaultGroup.matches(BAR_TOPIC));
        assertEquals(DEFAULT_TOPIC_CREATION_GROUP, defaultGroup.name());
        TopicCreationGroup fooGroup = groups.get(FOO_GROUP);
        assertFalse(fooGroup.matches(" "));
        assertTrue(fooGroup.matches(FOO_TOPIC));
        assertFalse(fooGroup.matches(BAR_TOPIC));
        assertEquals(FOO_GROUP, fooGroup.name());

        assertTrue(topicCreation.isTopicCreationEnabled());
        assertTrue(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertEquals(1, topicCreation.topicGroups().size());
        assertThat(topicCreation.topicGroups().keySet(), hasItems(FOO_GROUP));
        assertEquals(fooGroup, topicCreation.findFirstGroup(FOO_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));

        // verify new topic properties
        NewTopic defaultTopicSpec = topicCreation.findFirstGroup(BAR_TOPIC).newTopic(BAR_TOPIC);
        assertEquals(BAR_TOPIC, defaultTopicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, defaultTopicSpec.replicationFactor());
        assertEquals(partitions, defaultTopicSpec.numPartitions());
        assertThat(defaultTopicSpec.configs(), is(Collections.emptyMap()));

        NewTopic fooTopicSpec = topicCreation.findFirstGroup(FOO_TOPIC).newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, fooTopicSpec.name());
        assertEquals(fooReplicas, fooTopicSpec.replicationFactor());
        assertEquals(partitions, fooTopicSpec.numPartitions());
        assertThat(fooTopicSpec.configs(), is(topicProps));
    }

    @Test
    public void topicCreationWithOneGroupAndCombinedRegex() {
        short fooReplicas = 3;
        int partitions = 5;
        sourceProps = defaultConnectorPropsWithTopicCreation();
        sourceProps.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP));
        sourceProps.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        // Setting here but they should be ignored for the default group
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, String.join("|", FOO_REGEX, BAR_REGEX));
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + REPLICATION_FACTOR_CONFIG, String.valueOf(fooReplicas));

        Map<String, String> topicProps = new HashMap<>();
        topicProps.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        topicProps.forEach((k, v) -> sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + k, v));

        // verify config creation
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);
        assertTrue(sourceConfig.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) sourceConfig.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) sourceConfig.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(sourceConfig.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(sourceConfig.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(sourceConfig.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        // verify topic creation group is instantiated correctly
        Map<String, TopicCreationGroup> groups = TopicCreationGroup.configuredGroups(sourceConfig);
        assertEquals(2, groups.size());
        assertThat(groups.keySet(), hasItems(DEFAULT_TOPIC_CREATION_GROUP, FOO_GROUP));

        // verify topic creation
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, groups);
        TopicCreationGroup defaultGroup = topicCreation.defaultTopicGroup();
        // Default group will match all topics besides empty string
        assertTrue(defaultGroup.matches(" "));
        assertTrue(defaultGroup.matches(FOO_TOPIC));
        assertTrue(defaultGroup.matches(BAR_TOPIC));
        assertEquals(DEFAULT_TOPIC_CREATION_GROUP, defaultGroup.name());
        TopicCreationGroup fooGroup = groups.get(FOO_GROUP);
        assertFalse(fooGroup.matches(" "));
        assertTrue(fooGroup.matches(FOO_TOPIC));
        assertTrue(fooGroup.matches(BAR_TOPIC));
        assertEquals(FOO_GROUP, fooGroup.name());

        assertTrue(topicCreation.isTopicCreationEnabled());
        assertTrue(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertTrue(topicCreation.isTopicCreationRequired(BAR_TOPIC));
        assertEquals(1, topicCreation.topicGroups().size());
        assertThat(topicCreation.topicGroups().keySet(), hasItems(FOO_GROUP));
        assertEquals(fooGroup, topicCreation.findFirstGroup(FOO_TOPIC));
        assertEquals(fooGroup, topicCreation.findFirstGroup(BAR_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        topicCreation.addTopic(BAR_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertFalse(topicCreation.isTopicCreationRequired(BAR_TOPIC));

        // verify new topic properties
        NewTopic fooTopicSpec = topicCreation.findFirstGroup(FOO_TOPIC).newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, fooTopicSpec.name());
        assertEquals(fooReplicas, fooTopicSpec.replicationFactor());
        assertEquals(partitions, fooTopicSpec.numPartitions());
        assertThat(fooTopicSpec.configs(), is(topicProps));

        NewTopic barTopicSpec = topicCreation.findFirstGroup(BAR_TOPIC).newTopic(BAR_TOPIC);
        assertEquals(BAR_TOPIC, barTopicSpec.name());
        assertEquals(fooReplicas, barTopicSpec.replicationFactor());
        assertEquals(partitions, barTopicSpec.numPartitions());
        assertThat(barTopicSpec.configs(), is(topicProps));
    }

    @Test
    public void topicCreationWithTwoGroups() {
        short fooReplicas = 3;
        int partitions = 5;
        int barPartitions = 1;

        sourceProps = defaultConnectorPropsWithTopicCreation();
        sourceProps.put(TOPIC_CREATION_GROUPS_CONFIG, String.join(",", FOO_GROUP, BAR_GROUP));
        sourceProps.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(partitions));
        // Setting here but they should be ignored for the default group
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + INCLUDE_REGEX_CONFIG, FOO_TOPIC);
        sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + REPLICATION_FACTOR_CONFIG, String.valueOf(fooReplicas));
        sourceProps.put(TOPIC_CREATION_PREFIX + BAR_GROUP + "." + INCLUDE_REGEX_CONFIG, BAR_REGEX);
        sourceProps.put(TOPIC_CREATION_PREFIX + BAR_GROUP + "." + PARTITIONS_CONFIG, String.valueOf(barPartitions));

        Map<String, String> fooTopicProps = new HashMap<>();
        fooTopicProps.put(RETENTION_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(30)));
        fooTopicProps.forEach((k, v) -> sourceProps.put(TOPIC_CREATION_PREFIX + FOO_GROUP + "." + k, v));

        Map<String, String> barTopicProps = new HashMap<>();
        barTopicProps.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
        barTopicProps.forEach((k, v) -> sourceProps.put(TOPIC_CREATION_PREFIX + BAR_GROUP + "." + k, v));

        // verify config creation
        sourceConfig = new SourceConnectorConfig(MOCK_PLUGINS, sourceProps, true);
        assertTrue(sourceConfig.usesTopicCreation());
        assertEquals(DEFAULT_REPLICATION_FACTOR, (short) sourceConfig.topicCreationReplicationFactor(DEFAULT_TOPIC_CREATION_GROUP));
        assertEquals(partitions, (int) sourceConfig.topicCreationPartitions(DEFAULT_TOPIC_CREATION_GROUP));
        assertThat(sourceConfig.topicCreationInclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.singletonList(".*")));
        assertThat(sourceConfig.topicCreationExclude(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyList()));
        assertThat(sourceConfig.topicCreationOtherConfigs(DEFAULT_TOPIC_CREATION_GROUP), is(Collections.emptyMap()));

        // verify topic creation group is instantiated correctly
        Map<String, TopicCreationGroup> groups = TopicCreationGroup.configuredGroups(sourceConfig);
        assertEquals(3, groups.size());
        assertThat(groups.keySet(), hasItems(DEFAULT_TOPIC_CREATION_GROUP, FOO_GROUP, BAR_GROUP));

        // verify topic creation
        TopicCreation topicCreation = TopicCreation.newTopicCreation(workerConfig, groups);
        TopicCreationGroup defaultGroup = topicCreation.defaultTopicGroup();
        // Default group will match all topics besides empty string
        assertTrue(defaultGroup.matches(" "));
        assertTrue(defaultGroup.matches(FOO_TOPIC));
        assertTrue(defaultGroup.matches(BAR_TOPIC));
        assertEquals(DEFAULT_TOPIC_CREATION_GROUP, defaultGroup.name());
        TopicCreationGroup fooGroup = groups.get(FOO_GROUP);
        assertFalse(fooGroup.matches(" "));
        assertTrue(fooGroup.matches(FOO_TOPIC));
        assertFalse(fooGroup.matches(BAR_TOPIC));
        assertEquals(FOO_GROUP, fooGroup.name());
        TopicCreationGroup barGroup = groups.get(BAR_GROUP);
        assertTrue(barGroup.matches(BAR_TOPIC));
        assertFalse(barGroup.matches(FOO_TOPIC));
        assertEquals(BAR_GROUP, barGroup.name());

        assertTrue(topicCreation.isTopicCreationEnabled());
        assertTrue(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertTrue(topicCreation.isTopicCreationRequired(BAR_TOPIC));
        assertEquals(2, topicCreation.topicGroups().size());
        assertThat(topicCreation.topicGroups().keySet(), hasItems(FOO_GROUP, BAR_GROUP));
        assertEquals(fooGroup, topicCreation.findFirstGroup(FOO_TOPIC));
        assertEquals(barGroup, topicCreation.findFirstGroup(BAR_TOPIC));
        topicCreation.addTopic(FOO_TOPIC);
        topicCreation.addTopic(BAR_TOPIC);
        assertFalse(topicCreation.isTopicCreationRequired(FOO_TOPIC));
        assertFalse(topicCreation.isTopicCreationRequired(BAR_TOPIC));

        // verify new topic properties
        String otherTopic = "any-other-topic";
        NewTopic defaultTopicSpec = topicCreation.findFirstGroup(otherTopic).newTopic(otherTopic);
        assertEquals(otherTopic, defaultTopicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, defaultTopicSpec.replicationFactor());
        assertEquals(partitions, defaultTopicSpec.numPartitions());
        assertThat(defaultTopicSpec.configs(), is(Collections.emptyMap()));

        NewTopic fooTopicSpec = topicCreation.findFirstGroup(FOO_TOPIC).newTopic(FOO_TOPIC);
        assertEquals(FOO_TOPIC, fooTopicSpec.name());
        assertEquals(fooReplicas, fooTopicSpec.replicationFactor());
        assertEquals(partitions, fooTopicSpec.numPartitions());
        assertThat(fooTopicSpec.configs(), is(fooTopicProps));

        NewTopic barTopicSpec = topicCreation.findFirstGroup(BAR_TOPIC).newTopic(BAR_TOPIC);
        assertEquals(BAR_TOPIC, barTopicSpec.name());
        assertEquals(DEFAULT_REPLICATION_FACTOR, barTopicSpec.replicationFactor());
        assertEquals(barPartitions, barTopicSpec.numPartitions());
        assertThat(barTopicSpec.configs(), is(barTopicProps));
    }
}
