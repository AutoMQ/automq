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

package org.apache.kafka.controller;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.APPEND;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.DELETE;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SUBTRACT;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ConfigurationControlManagerTest {

    static final Map<ConfigResource.Type, ConfigDef> CONFIGS = new HashMap<>();

    static {
        CONFIGS.put(BROKER, new ConfigDef().
            define("foo.bar", ConfigDef.Type.LIST, "1", ConfigDef.Importance.HIGH, "foo bar").
            define("baz", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "baz").
            define("quux", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "quux"));
        CONFIGS.put(TOPIC, new ConfigDef().
            define("abc", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "abc").
            define("def", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "def").
            define("ghi", ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, "ghi"));
    }

    static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(CONFIGS);

    static final ConfigResource BROKER0 = new ConfigResource(BROKER, "0");
    static final ConfigResource MYTOPIC = new ConfigResource(TOPIC, "mytopic");

    static class TestExistenceChecker implements Consumer<ConfigResource> {
        static final TestExistenceChecker INSTANCE = new TestExistenceChecker();

        @Override
        public void accept(ConfigResource resource) {
            if (!resource.name().startsWith("Existing")) {
                throw new UnknownTopicOrPartitionException("Unknown resource.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <A, B> Map<A, B> toMap(Entry... entries) {
        Map<A, B> map = new LinkedHashMap<>();
        for (Entry<A, B> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    static <A, B> Entry<A, B> entry(A a, B b) {
        return new SimpleImmutableEntry<>(a, b);
    }

    static ConfigurationControlManager newConfigurationControlManager() {
        return newConfigurationControlManager(Optional.empty());
    }

    static ConfigurationControlManager newConfigurationControlManager(
        Optional<AlterConfigPolicy> alterConfigPolicy
    ) {
        LogContext logContext = new LogContext();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        return new ConfigurationControlManager(logContext,
                snapshotRegistry,
                SCHEMA,
                TestExistenceChecker.INSTANCE,
                alterConfigPolicy,
                ConfigurationValidator.NO_OP);
    }

    @Test
    public void testReplay() throws Exception {
        ConfigurationControlManager manager = newConfigurationControlManager();
        assertEquals(Collections.emptyMap(), manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue("1,2"));
        assertEquals(Collections.singletonMap("foo.bar", "1,2"),
            manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(BROKER.id()).setResourceName("0").
            setName("foo.bar").setValue(null));
        assertEquals(Collections.emptyMap(), manager.getConfigs(BROKER0));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("abc").setValue("x,y,z"));
        manager.replay(new ConfigRecord().
            setResourceType(TOPIC.id()).setResourceName("mytopic").
            setName("def").setValue("blah"));
        assertEquals(toMap(entry("abc", "x,y,z"), entry("def", "blah")),
            manager.getConfigs(MYTOPIC));
        RecordTestUtils.assertBatchIteratorContains(asList(
            asList(new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("x,y,z"), (short) 0),
                new ApiMessageAndVersion(new ConfigRecord().
                    setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("def").setValue("blah"), (short) 0))),
            manager.iterator(Long.MAX_VALUE));
    }

    @Test
    public void testIncrementalAlterConfigs() {
        ConfigurationControlManager manager = newConfigurationControlManager();

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(BROKER0, toMap(
                entry("baz", entry(SUBTRACT, "abc")),
                entry("quux", entry(SET, "abc")))),
                entry(MYTOPIC, toMap(entry("abc", entry(APPEND, "123"))))),
                true);

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue("123"), (short) 0)),
                toMap(entry(BROKER0, new ApiError(Errors.INVALID_CONFIG,
                            "Can't SUBTRACT to key baz because its type is not LIST.")),
                    entry(MYTOPIC, ApiError.NONE))), result);

        RecordTestUtils.replayAll(manager, result.records());

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("mytopic").
                    setName("abc").setValue(null), (short) 0)),
                toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(
                entry("abc", entry(DELETE, "xyz"))))),
                true));
    }

    @Test
    public void testIncrementalAlterConfigsWithoutExistence() {
        ConfigurationControlManager manager = newConfigurationControlManager();
        ConfigResource existingTopic = new ConfigResource(TOPIC, "ExistingTopic");

        ControllerResult<Map<ConfigResource, ApiError>> result = manager.
            incrementalAlterConfigs(toMap(entry(BROKER0, toMap(
                entry("quux", entry(SET, "1")))),
                entry(existingTopic, toMap(entry("def", entry(SET, "newVal"))))),
                false);

        assertEquals(ControllerResult.atomicOf(Collections.singletonList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(TOPIC.id()).setResourceName("ExistingTopic").
                    setName("def").setValue("newVal"), (short) 0)),
            toMap(entry(BROKER0, new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION,
                    "Unknown resource.")),
                entry(existingTopic, ApiError.NONE))), result);
    }

    private static class MockAlterConfigsPolicy implements AlterConfigPolicy {
        private final List<RequestMetadata> expecteds;
        private final AtomicLong index = new AtomicLong(0);

        MockAlterConfigsPolicy(List<RequestMetadata> expecteds) {
            this.expecteds = expecteds;
        }

        @Override
        public void validate(RequestMetadata actual) throws PolicyViolationException {
            long curIndex = index.getAndIncrement();
            if (curIndex >= expecteds.size()) {
                throw new PolicyViolationException("Unexpected config alteration: index " +
                    "out of range at " + curIndex);
            }
            RequestMetadata expected = expecteds.get((int) curIndex);
            if (!expected.equals(actual)) {
                throw new PolicyViolationException("Expected: " + expected +
                    ". Got: " + actual);
            }
        }

        @Override
        public void close() throws Exception {
            // nothing to do
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // nothing to do
        }
    }

    @Test
    public void testIncrementalAlterConfigsWithPolicy() {
        MockAlterConfigsPolicy policy = new MockAlterConfigsPolicy(asList(
            new RequestMetadata(MYTOPIC, Collections.emptyMap()),
            new RequestMetadata(BROKER0, toMap(entry("foo.bar", "123"),
                entry("quux", "456")))));

        ConfigurationControlManager manager = newConfigurationControlManager(Optional.of(policy));

        assertEquals(ControllerResult.atomicOf(asList(new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                    setName("foo.bar").setValue("123"), (short) 0), new ApiMessageAndVersion(
                new ConfigRecord().setResourceType(BROKER.id()).setResourceName("0").
                    setName("quux").setValue("456"), (short) 0)),
            toMap(entry(MYTOPIC, new ApiError(Errors.POLICY_VIOLATION,
                    "Expected: AlterConfigPolicy.RequestMetadata(resource=ConfigResource(" +
                    "type=TOPIC, name='mytopic'), configs={}). Got: " +
                    "AlterConfigPolicy.RequestMetadata(resource=ConfigResource(" +
                    "type=TOPIC, name='mytopic'), configs={foo.bar=123})")),
                entry(BROKER0, ApiError.NONE))),
            manager.incrementalAlterConfigs(toMap(entry(MYTOPIC, toMap(
                entry("foo.bar", entry(SET, "123")))),
                entry(BROKER0, toMap(
                entry("foo.bar", entry(SET, "123")),
                entry("quux", entry(SET, "456"))))),
                true));
    }

    @Test
    public void testLegacyAlterConfigs() {
        ConfigurationControlManager manager = newConfigurationControlManager();
        List<ApiMessageAndVersion> expectedRecords1 = asList(
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("abc").setValue("456"), (short) 0),
            new ApiMessageAndVersion(new ConfigRecord().
                setResourceType(TOPIC.id()).setResourceName("mytopic").
                setName("def").setValue("901"), (short) 0));
        assertEquals(ControllerResult.atomicOf(
                expectedRecords1, toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.legacyAlterConfigs(
                toMap(entry(MYTOPIC, toMap(entry("abc", "456"), entry("def", "901")))),
                true));
        for (ApiMessageAndVersion message : expectedRecords1) {
            manager.replay((ConfigRecord) message.message());
        }
        assertEquals(ControllerResult.atomicOf(asList(
            new ApiMessageAndVersion(
                new ConfigRecord()
                    .setResourceType(TOPIC.id())
                    .setResourceName("mytopic")
                    .setName("abc")
                    .setValue(null),
                (short) 0)),
            toMap(entry(MYTOPIC, ApiError.NONE))),
            manager.legacyAlterConfigs(toMap(entry(MYTOPIC, toMap(entry("def", "901")))),
                true));
    }
}
