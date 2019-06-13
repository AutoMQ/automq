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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.integration.SuppressionDurabilityIntegrationTest;
import org.apache.kafka.streams.integration.SuppressionIntegrationTest;
import org.apache.kafka.streams.kstream.SuppressedTest;
import org.apache.kafka.streams.kstream.internals.FullChangeSerdeTest;
import org.apache.kafka.streams.kstream.internals.SuppressScenarioTest;
import org.apache.kafka.streams.kstream.internals.SuppressTopologyTest;
import org.apache.kafka.streams.state.internals.BufferValueTest;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBufferTest;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This suite runs all the tests related to the Suppression feature.
 *
 * It can be used from an IDE to selectively just run these tests when developing code related to Suppress.
 *
 * If desired, it can also be added to a Gradle build task, although this isn't strictly necessary, since all
 * these tests are already included in the `:streams:test` task.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    BufferValueTest.class,
    KTableSuppressProcessorMetricsTest.class,
    KTableSuppressProcessorTest.class,
    SuppressScenarioTest.class,
    SuppressTopologyTest.class,
    SuppressedTest.class,
    InMemoryTimeOrderedKeyValueBufferTest.class,
    TimeOrderedKeyValueBufferTest.class,
    FullChangeSerdeTest.class,
    SuppressionIntegrationTest.class,
    SuppressionDurabilityIntegrationTest.class
})
public class SuppressSuite {
}


