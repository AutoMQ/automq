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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils.StableAssignmentListener;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStore;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class EosBetaUpgradeIntegrationTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
            {false},
            {true}
        });
    }

    @Parameterized.Parameter
    public boolean injectError;

    private static final int NUM_BROKERS = 3;
    private static final int MAX_POLL_INTERVAL_MS = 100 * 1000;
    private static final int MAX_WAIT_TIME_MS = 60 * 1000;

    private static final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> CLOSE =
        Collections.unmodifiableList(
            Arrays.asList(
                KeyValue.pair(KafkaStreams.State.RUNNING, KafkaStreams.State.PENDING_SHUTDOWN),
                KeyValue.pair(KafkaStreams.State.PENDING_SHUTDOWN, KafkaStreams.State.NOT_RUNNING)
            )
        );
    private static final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> CRASH =
        Collections.unmodifiableList(
            Collections.singletonList(
                KeyValue.pair(KafkaStreams.State.RUNNING, KafkaStreams.State.ERROR)
            )
        );
    private static final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> CLOSE_CRASHED =
        Collections.unmodifiableList(
            Arrays.asList(
                KeyValue.pair(KafkaStreams.State.ERROR, KafkaStreams.State.PENDING_SHUTDOWN),
                KeyValue.pair(KafkaStreams.State.PENDING_SHUTDOWN, KafkaStreams.State.NOT_RUNNING)
            )
        );

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(Collections.singletonMap("auto.create.topics.enable", "false"))
    );

    private static String applicationId;
    private final static int NUM_TOPIC_PARTITIONS = 4;
    private final static String CONSUMER_GROUP_ID = "readCommitted";
    private final static String MULTI_PARTITION_INPUT_TOPIC = "multiPartitionInputTopic";
    private final static String MULTI_PARTITION_OUTPUT_TOPIC = "multiPartitionOutputTopic";
    private final String storeName = "store";

    private final StableAssignmentListener assignmentListener = new StableAssignmentListener();

    private final AtomicBoolean errorInjectedClient1 = new AtomicBoolean(false);
    private final AtomicBoolean errorInjectedClient2 = new AtomicBoolean(false);
    private final AtomicBoolean commitErrorInjectedClient1 = new AtomicBoolean(false);
    private final AtomicBoolean commitErrorInjectedClient2 = new AtomicBoolean(false);
    private final AtomicInteger commitCounterClient1 = new AtomicInteger(-1);
    private final AtomicInteger commitCounterClient2 = new AtomicInteger(-1);
    private final AtomicInteger commitRequested = new AtomicInteger(0);

    // Note: this pattern only works when we just have a single instance running with a single thread
    // If we want to extend the test or reuse this CommitPunctuator we should tighten it up
    private final AtomicBoolean requestCommit = new AtomicBoolean(false);
    private static class CommitPunctuator implements Punctuator {
        final ProcessorContext context;
        final AtomicBoolean requestCommit;

        public CommitPunctuator(final ProcessorContext context, final AtomicBoolean requestCommit) {
            this.context = context;
            this.requestCommit = requestCommit;
        }

        @Override
        public void punctuate(final long timestamp) {
            if (requestCommit.get()) {
                context.commit();
                requestCommit.set(false);
            }
        }
    }

    private Throwable uncaughtException;

    private int testNumber = 0;

    @Before
    public void createTopics() throws Exception {
        applicationId = "appId-" + ++testNumber;
        CLUSTER.deleteTopicsAndWait(
            MULTI_PARTITION_INPUT_TOPIC,
            MULTI_PARTITION_OUTPUT_TOPIC,
            applicationId + "-" + storeName + "-changelog"
        );

        CLUSTER.createTopic(MULTI_PARTITION_INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        CLUSTER.createTopic(MULTI_PARTITION_OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    @Test
    public void shouldUpgradeFromEosAlphaToEosBeta() throws Exception {
        // We use two KafkaStreams clients that we upgrade from eos-alpha to eos-beta. During the upgrade,
        // we ensure that there are pending transaction and verify that data is processed correctly.
        //
        // We either close clients cleanly (`injectError = false`) or let them crash (`injectError = true`) during
        // the upgrade. For both cases, EOS should not be violated.
        //
        // Additionally, we inject errors while one client is on eos-alpha while the other client is on eos-beta:
        // For this case, we inject the error during task commit phase, i.e., after offsets are appended to a TX,
        // and before the TX is committed. The goal is to verify that the written but uncommitted offsets are not
        // picked up, i.e., GroupCoordinator fencing works correctly.
        //
        // The commit interval is set to MAX_VALUE and the used `Processor` request commits manually so we have full
        // control when a commit actually happens. We use an input topic with 4 partitions and each task will request
        // a commit after processing 10 records.
        //
        // 1.  start both clients and wait until rebalance stabilizes
        // 2.  write 10 records per input topic partition and verify that the result was committed
        // 3.  write 5 records per input topic partition to get pending transactions (verified via "read_uncommitted" mode)
        //      - all 4 pending transactions are based on task producers
        //      - we will get only 4 pending writes for one partition for the crash case as we crash processing the 5th record
        // 4.  stop/crash the first client, wait until rebalance stabilizes:
        //      - stop case:
        //        * verify that the stopped client did commit its pending transaction during shutdown
        //        * the second client will still have two pending transaction
        //      - crash case:
        //        * the pending transactions of the crashed client got aborted
        //        * the second client will have four pending transactions
        // 5.  restart the first client with eos-beta enabled and wait until rebalance stabilizes
        //       - the rebalance should result in a commit of all tasks
        // 6.  write 5 record per input topic partition
        //       - stop case:
        //         * verify that the result was committed
        //       - crash case:
        //         * fail the second (i.e., eos-alpha) client during commit
        //         * the eos-beta client should not pickup the pending offsets
        //         * verify uncommitted and committed result
        // 7.  only for crash case:
        //     7a. restart the second client in eos-alpha mode and wait until rebalance stabilizes
        //     7b. write 10 records per input topic partition
        //         * fail the first (i.e., eos-beta) client during commit
        //         * the eos-alpha client should not pickup the pending offsets
        //         * verify uncommitted and committed result
        //     7c. restart the first client in eos-beta mode and wait until rebalance stabilizes
        // 8.  write 5 records per input topic partition to get pending transactions (verified via "read_uncommitted" mode)
        //      - 2 transaction are base on a task producer; one transaction is based on a thread producer
        //      - we will get 4 pending writes for the crash case as we crash processing the 5th record
        // 9.  stop/crash the second client and wait until rebalance stabilizes:
        //      - stop only:
        //        * verify that the stopped client did commit its pending transaction during shutdown
        //        * the first client will still have one pending transaction
        //      - crash case:
        //        * the pending transactions of the crashed client got aborted
        //        * the first client will have one pending transactions
        // 10. restart the second client with eos-beta enabled and wait until rebalance stabilizes
        //       - the rebalance should result in a commit of all tasks
        // 11. write 5 record per input topic partition and verify that the result was committed

        final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> stateTransitions1 = new LinkedList<>();
        KafkaStreams streams1Alpha = null;
        KafkaStreams streams1Beta = null;
        KafkaStreams streams1BetaTwo = null;

        final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> stateTransitions2 = new LinkedList<>();
        KafkaStreams streams2Alpha = null;
        KafkaStreams streams2AlphaTwo = null;
        KafkaStreams streams2Beta = null;

        try {
            // phase 1: start both clients
            streams1Alpha = getKafkaStreams("appDir1", StreamsConfig.EXACTLY_ONCE);
            streams1Alpha.setStateListener(
                (newState, oldState) -> stateTransitions1.add(KeyValue.pair(oldState, newState))
            );

            assignmentListener.prepareForRebalance();
            streams1Alpha.cleanUp();
            streams1Alpha.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);

            streams2Alpha = getKafkaStreams("appDir2", StreamsConfig.EXACTLY_ONCE);
            streams2Alpha.setStateListener(
                (newState, oldState) -> stateTransitions2.add(KeyValue.pair(oldState, newState))
            );
            stateTransitions1.clear();

            assignmentListener.prepareForRebalance();
            streams2Alpha.cleanUp();
            streams2Alpha.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);
            waitForRunning(stateTransitions2);

            // in all phases, we write comments that assume that p-0/p-1 are assigned to the first client
            // and p-2/p-3 are assigned to the second client (in reality the assignment might be different though)

            // phase 2: (write first batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // p-0: ---> 10 rec + C
            // p-1: ---> 10 rec + C
            // p-2: ---> 10 rec + C
            // p-3: ---> 10 rec + C
            final List<KeyValue<Long, Long>> committedInputDataBeforeUpgrade =
                prepareData(0L, 10L, 0L, 1L, 2L, 3L);
            writeInputData(committedInputDataBeforeUpgrade);

            waitForCondition(
                () -> commitRequested.get() == 4,
                MAX_WAIT_TIME_MS,
                "SteamsTasks did not request commit."
            );

            final Map<Long, Long> committedState = new HashMap<>();
            final List<KeyValue<Long, Long>> expectedUncommittedResult =
                computeExpectedResult(committedInputDataBeforeUpgrade, committedState);
            verifyCommitted(expectedUncommittedResult);

            // phase 3: (write partial second batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C ---> 5 rec (pending)
            //   p-1: 10 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C ---> 5 rec (pending)
            //   p-3: 10 rec + C ---> 5 rec (pending)
            // crash case: (we just assumes that we inject the error for p-0; in reality it might be a different partition)
            //   p-0: 10 rec + C ---> 4 rec (pending)
            //   p-1: 10 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C ---> 5 rec (pending)
            //   p-3: 10 rec + C ---> 5 rec (pending)
            final Set<Long> cleanKeys = mkSet(0L, 1L, 2L, 3L);
            final Set<Long> keyFilterFirstClient = keysFromInstance(streams1Alpha);
            final long potentiallyFirstFailingKey = keyFilterFirstClient.iterator().next();
            cleanKeys.remove(potentiallyFirstFailingKey);

            final List<KeyValue<Long, Long>> uncommittedInputDataBeforeFirstUpgrade = new LinkedList<>();
            if (!injectError) {
                uncommittedInputDataBeforeFirstUpgrade.addAll(
                    prepareData(10L, 15L, 0L, 1L, 2L, 3L)
                );
                writeInputData(uncommittedInputDataBeforeFirstUpgrade);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataBeforeFirstUpgrade, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            } else {
                final List<KeyValue<Long, Long>> uncommittedInputDataWithoutFailingKey = new LinkedList<>();
                for (final long key : cleanKeys) {
                    uncommittedInputDataWithoutFailingKey.addAll(prepareData(10L, 15L, key));
                }
                uncommittedInputDataWithoutFailingKey.addAll(
                    prepareData(10L, 14L, potentiallyFirstFailingKey)
                );
                uncommittedInputDataBeforeFirstUpgrade.addAll(uncommittedInputDataWithoutFailingKey);
                writeInputData(uncommittedInputDataWithoutFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataWithoutFailingKey, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            }

            // phase 4: (stop/crash first client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec ---> C
            //   p-1: 10 rec + C + 5 rec ---> C
            //   p-2: 10 rec + C + 5 rec (pending)
            //   p-3: 10 rec + C + 5 rec (pending)
            // crash case:
            //   p-0: 10 rec + C + 4 rec ---> A + 5 rec (pending)
            //   p-1: 10 rec + C + 5 rec ---> A + 5 rec (pending)
            //   p-2: 10 rec + C + 5 rec (pending)
            //   p-3: 10 rec + C + 5 rec (pending)
            stateTransitions2.clear();
            assignmentListener.prepareForRebalance();

            if (!injectError) {
                stateTransitions1.clear();
                streams1Alpha.close();
                waitForStateTransition(stateTransitions1, CLOSE);
            } else {
                errorInjectedClient1.set(true);

                final List<KeyValue<Long, Long>> dataPotentiallyFirstFailingKey =
                    prepareData(14L, 15L, potentiallyFirstFailingKey);
                uncommittedInputDataBeforeFirstUpgrade.addAll(dataPotentiallyFirstFailingKey);
                writeInputData(dataPotentiallyFirstFailingKey);
            }
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions2);

            if (!injectError) {
                final List<KeyValue<Long, Long>> committedInputDataDuringFirstUpgrade =
                    uncommittedInputDataBeforeFirstUpgrade
                        .stream()
                        .filter(pair -> keyFilterFirstClient.contains(pair.key))
                        .collect(Collectors.toList());

                final List<KeyValue<Long, Long>> expectedCommittedResult =
                    computeExpectedResult(committedInputDataDuringFirstUpgrade, committedState);
                verifyCommitted(expectedCommittedResult);
            } else {
                // retrying TX
                expectedUncommittedResult.addAll(computeExpectedResult(
                    uncommittedInputDataBeforeFirstUpgrade
                        .stream()
                        .filter(pair -> keyFilterFirstClient.contains(pair.key))
                        .collect(Collectors.toList()),
                    new HashMap<>(committedState)
                ));
                verifyUncommitted(expectedUncommittedResult);

                errorInjectedClient1.set(false);
                stateTransitions1.clear();
                streams1Alpha.close();
                waitForStateTransition(stateTransitions1, CLOSE_CRASHED);
            }

            // phase 5: (restart first client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec + C
            //   p-1: 10 rec + C + 5 rec + C
            //   p-2: 10 rec + C + 5 rec ---> C
            //   p-3: 10 rec + C + 5 rec ---> C
            // crash case:
            //   p-0: 10 rec + C + 4 rec + A + 5 rec ---> C
            //   p-1: 10 rec + C + 5 rec + A + 5 rec ---> C
            //   p-2: 10 rec + C + 5 rec ---> C
            //   p-3: 10 rec + C + 5 rec ---> C
            requestCommit.set(true);
            waitForCondition(() -> !requestCommit.get(), "Punctuator did not request commit for running client");
            commitRequested.set(0);
            stateTransitions1.clear();
            stateTransitions2.clear();
            streams1Beta = getKafkaStreams("appDir1", StreamsConfig.EXACTLY_ONCE_BETA);
            streams1Beta.setStateListener((newState, oldState) -> stateTransitions1.add(KeyValue.pair(oldState, newState)));
            assignmentListener.prepareForRebalance();
            streams1Beta.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);
            waitForRunning(stateTransitions2);

            final Set<Long> committedKeys = mkSet(0L, 1L, 2L, 3L);
            if (!injectError) {
                committedKeys.removeAll(keyFilterFirstClient);
            }

            final List<KeyValue<Long, Long>> expectedCommittedResultAfterRestartFirstClient = computeExpectedResult(
                uncommittedInputDataBeforeFirstUpgrade
                    .stream()
                    .filter(pair -> committedKeys.contains(pair.key))
                    .collect(Collectors.toList()),
                committedState
            );
            verifyCommitted(expectedCommittedResultAfterRestartFirstClient);

            // phase 6: (complete second batch of data; crash: let second client fail on commit)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C + 5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C + 5 rec + C ---> 5 rec + C
            //   p-3: 10 rec + C + 5 rec + C ---> 5 rec + C
            // crash case:
            //   p-0: 10 rec + C + 4 rec + A + 5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C + 5 rec + A + 5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C + 5 rec + C ---> 5 rec + A + 5 rec + C
            //   p-3: 10 rec + C + 5 rec + C ---> 5 rec + A + 5 rec + C
            commitCounterClient1.set(0);

            if (!injectError) {
                final List<KeyValue<Long, Long>> committedInputDataDuringUpgrade =
                    prepareData(15L, 20L, 0L, 1L, 2L, 3L);
                writeInputData(committedInputDataDuringUpgrade);

                final List<KeyValue<Long, Long>> expectedCommittedResult =
                    computeExpectedResult(committedInputDataDuringUpgrade, committedState);
                verifyCommitted(expectedCommittedResult);
                expectedUncommittedResult.addAll(expectedCommittedResult);
            } else {
                final Set<Long> keysFirstClient = keysFromInstance(streams1Beta);
                final Set<Long> keysSecondClient = keysFromInstance(streams2Alpha);

                final List<KeyValue<Long, Long>> committedInputDataAfterFirstUpgrade =
                    prepareData(15L, 20L, keysFirstClient.toArray(new Long[0]));
                writeInputData(committedInputDataAfterFirstUpgrade);

                final List<KeyValue<Long, Long>> expectedCommittedResultBeforeFailure =
                    computeExpectedResult(committedInputDataAfterFirstUpgrade, committedState);
                verifyCommitted(expectedCommittedResultBeforeFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultBeforeFailure);

                commitCounterClient2.set(0);

                final Iterator<Long> it = keysSecondClient.iterator();
                final Long otherKey = it.next();
                final Long failingKey = it.next();

                final List<KeyValue<Long, Long>> uncommittedInputDataAfterFirstUpgrade =
                    prepareData(15L, 19L, keysSecondClient.toArray(new Long[0]));
                uncommittedInputDataAfterFirstUpgrade.addAll(prepareData(19L, 20L, otherKey));
                writeInputData(uncommittedInputDataAfterFirstUpgrade);

                final Map<Long, Long> uncommittedState = new HashMap<>(committedState);
                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataAfterFirstUpgrade, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                stateTransitions1.clear();
                stateTransitions2.clear();
                assignmentListener.prepareForRebalance();

                commitCounterClient1.set(0);
                commitErrorInjectedClient2.set(true);

                final List<KeyValue<Long, Long>> dataFailingKey = prepareData(19L, 20L, failingKey);
                uncommittedInputDataAfterFirstUpgrade.addAll(dataFailingKey);
                writeInputData(dataFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(dataFailingKey, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);

                waitForStateTransition(stateTransitions2, CRASH);

                commitErrorInjectedClient2.set(false);
                stateTransitions2.clear();
                streams2Alpha.close();
                waitForStateTransition(stateTransitions2, CLOSE_CRASHED);

                final List<KeyValue<Long, Long>> expectedCommittedResultAfterFailure =
                    computeExpectedResult(uncommittedInputDataAfterFirstUpgrade, committedState);
                verifyCommitted(expectedCommittedResultAfterFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultAfterFailure);
            }

            // 7. only for crash case:
            //     7a. restart the second client in eos-alpha mode and wait until rebalance stabilizes
            //     7b. write third batch of input data
            //         * fail the first (i.e., eos-beta) client during commit
            //         * the eos-alpha client should not pickup the pending offsets
            //         * verify uncommitted and committed result
            //     7c. restart the first client in eos-beta mode and wait until rebalance stabilizes
            //
            // crash case:
            //   p-0: 10 rec + C + 4 rec + A + 5 rec + C + 5 rec + C ---> 10 rec + A + 10 rec + C
            //   p-1: 10 rec + C + 5 rec + A + 5 rec + C + 5 rec + C ---> 10 rec + A + 10 rec + C
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C ---> 10 rec + C
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C ---> 10 rec + C
            if (!injectError) {
                streams2AlphaTwo = streams2Alpha;
            } else {
                // 7a restart the second client in eos-alpha mode and wait until rebalance stabilizes
                commitCounterClient1.set(0);
                commitCounterClient2.set(-1);
                stateTransitions1.clear();
                stateTransitions2.clear();
                streams2AlphaTwo = getKafkaStreams("appDir2", StreamsConfig.EXACTLY_ONCE);
                streams2AlphaTwo.setStateListener(
                    (newState, oldState) -> stateTransitions2.add(KeyValue.pair(oldState, newState))
                );
                assignmentListener.prepareForRebalance();
                streams2AlphaTwo.start();
                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
                waitForRunning(stateTransitions1);
                waitForRunning(stateTransitions2);

                // 7b. write third batch of input data
                final Set<Long> keysFirstClient = keysFromInstance(streams1Beta);
                final Set<Long> keysSecondClient = keysFromInstance(streams2AlphaTwo);

                final List<KeyValue<Long, Long>> committedInputDataBetweenUpgrades =
                    prepareData(20L, 30L, keysSecondClient.toArray(new Long[0]));
                writeInputData(committedInputDataBetweenUpgrades);

                final List<KeyValue<Long, Long>> expectedCommittedResultBeforeFailure =
                    computeExpectedResult(committedInputDataBetweenUpgrades, committedState);
                verifyCommitted(expectedCommittedResultBeforeFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultBeforeFailure);

                commitCounterClient2.set(0);

                final Iterator<Long> it = keysFirstClient.iterator();
                final Long otherKey = it.next();
                final Long failingKey = it.next();

                final List<KeyValue<Long, Long>> uncommittedInputDataBetweenUpgrade =
                    prepareData(20L, 29L, keysFirstClient.toArray(new Long[0]));
                uncommittedInputDataBetweenUpgrade.addAll(prepareData(29L, 30L, otherKey));
                writeInputData(uncommittedInputDataBetweenUpgrade);

                final Map<Long, Long> uncommittedState = new HashMap<>(committedState);
                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataBetweenUpgrade, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                stateTransitions1.clear();
                stateTransitions2.clear();
                assignmentListener.prepareForRebalance();
                commitCounterClient2.set(0);
                commitErrorInjectedClient1.set(true);

                final List<KeyValue<Long, Long>> dataFailingKey = prepareData(29L, 30L, failingKey);
                uncommittedInputDataBetweenUpgrade.addAll(dataFailingKey);
                writeInputData(dataFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(dataFailingKey, uncommittedState)
                );
                verifyUncommitted(expectedUncommittedResult);

                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
                waitForStateTransition(stateTransitions1, CRASH);

                commitErrorInjectedClient1.set(false);
                stateTransitions1.clear();
                streams1Beta.close();
                waitForStateTransition(stateTransitions1, CLOSE_CRASHED);

                final List<KeyValue<Long, Long>> expectedCommittedResultAfterFailure =
                    computeExpectedResult(uncommittedInputDataBetweenUpgrade, committedState);
                verifyCommitted(expectedCommittedResultAfterFailure);
                expectedUncommittedResult.addAll(expectedCommittedResultAfterFailure);

                // 7c. restart the first client in eos-beta mode and wait until rebalance stabilizes
                stateTransitions1.clear();
                stateTransitions2.clear();
                streams1BetaTwo = getKafkaStreams("appDir1", StreamsConfig.EXACTLY_ONCE_BETA);
                streams1BetaTwo.setStateListener((newState, oldState) -> stateTransitions1.add(KeyValue.pair(oldState, newState)));
                assignmentListener.prepareForRebalance();
                streams1BetaTwo.start();
                assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
                waitForRunning(stateTransitions1);
                waitForRunning(stateTransitions2);
            }

            // phase 8: (write partial fourth batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec + C + 5 rec + C ---> 5 rec (pending)
            //   p-1: 10 rec + C + 5 rec + C + 5 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + C ---> 5 rec (pending)
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + C ---> 5 rec (pending)
            // crash case:  (we just assumes that we inject the error for p-2; in reality it might be a different partition)
            //   p-0: 10 rec + C + 4 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C ---> 5 rec (pending)
            //   p-1: 10 rec + C + 5 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C ---> 5 rec (pending)
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C ---> 4 rec (pending)
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C ---> 5 rec (pending)
            cleanKeys.addAll(mkSet(0L, 1L, 2L, 3L));
            final Set<Long> keyFilterSecondClient = keysFromInstance(streams2AlphaTwo);
            final long potentiallySecondFailingKey = keyFilterSecondClient.iterator().next();
            cleanKeys.remove(potentiallySecondFailingKey);

            final List<KeyValue<Long, Long>> uncommittedInputDataBeforeSecondUpgrade = new LinkedList<>();
            if (!injectError) {
                uncommittedInputDataBeforeSecondUpgrade.addAll(
                    prepareData(30L, 35L, 0L, 1L, 2L, 3L)
                );
                writeInputData(uncommittedInputDataBeforeSecondUpgrade);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataBeforeSecondUpgrade, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            } else {
                final List<KeyValue<Long, Long>> uncommittedInputDataWithoutFailingKey = new LinkedList<>();
                for (final long key : cleanKeys) {
                    uncommittedInputDataWithoutFailingKey.addAll(prepareData(30L, 35L, key));
                }
                uncommittedInputDataWithoutFailingKey.addAll(
                    prepareData(30L, 34L, potentiallySecondFailingKey)
                );
                uncommittedInputDataBeforeSecondUpgrade.addAll(uncommittedInputDataWithoutFailingKey);
                writeInputData(uncommittedInputDataWithoutFailingKey);

                expectedUncommittedResult.addAll(
                    computeExpectedResult(uncommittedInputDataWithoutFailingKey, new HashMap<>(committedState))
                );
                verifyUncommitted(expectedUncommittedResult);
            }

            // phase 9: (stop/crash second client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec (pending)
            //   p-1: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec (pending)
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec ---> C
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec ---> C
            // crash case:  (we just assumes that we inject the error for p-2; in reality it might be a different partition)
            //   p-0: 10 rec + C + 4 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C + 5 rec (pending)
            //   p-1: 10 rec + C + 5 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C + 5 rec (pending)
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C + 4 rec ---> A + 5 rec (pending)
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C + 5 rec ---> A + 5 rec (pending)
            stateTransitions1.clear();
            assignmentListener.prepareForRebalance();
            if (!injectError) {
                stateTransitions2.clear();
                streams2AlphaTwo.close();
                waitForStateTransition(stateTransitions2, CLOSE);
            } else {
                errorInjectedClient2.set(true);

                final List<KeyValue<Long, Long>> dataPotentiallySecondFailingKey =
                    prepareData(34L, 35L, potentiallySecondFailingKey);
                uncommittedInputDataBeforeSecondUpgrade.addAll(dataPotentiallySecondFailingKey);
                writeInputData(dataPotentiallySecondFailingKey);
            }
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);

            if (!injectError) {
                final List<KeyValue<Long, Long>> committedInputDataDuringSecondUpgrade =
                    uncommittedInputDataBeforeSecondUpgrade
                        .stream()
                        .filter(pair -> keyFilterSecondClient.contains(pair.key))
                        .collect(Collectors.toList());

                final List<KeyValue<Long, Long>> expectedCommittedResult =
                    computeExpectedResult(committedInputDataDuringSecondUpgrade, committedState);
                verifyCommitted(expectedCommittedResult);
            } else {
                // retrying TX
                expectedUncommittedResult.addAll(computeExpectedResult(
                    uncommittedInputDataBeforeSecondUpgrade
                        .stream()
                        .filter(pair -> keyFilterSecondClient.contains(pair.key))
                        .collect(Collectors.toList()),
                    new HashMap<>(committedState)
                ));
                verifyUncommitted(expectedUncommittedResult);

                errorInjectedClient2.set(false);
                stateTransitions2.clear();
                streams2AlphaTwo.close();
                waitForStateTransition(stateTransitions2, CLOSE_CRASHED);
            }

            // phase 10: (restart second client)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // the state below indicate the case for which the "original" tasks of client2 are migrated back to client2
            // if a task "switch" happens, we might get additional commits (omitted in the comment for brevity)
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec ---> C
            //   p-1: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec ---> C
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec + C
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec + C
            // crash case:  (we just assumes that we inject the error for p-2; in reality it might be a different partition)
            //   p-0: 10 rec + C + 4 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C + 5 rec ---> C
            //   p-1: 10 rec + C + 5 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C + 5 rec ---> C
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C + 4 rec + A + 5 rec ---> C
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C + 5 rec + A + 5 rec ---> C
            requestCommit.set(true);
            waitForCondition(() -> !requestCommit.get(), "Punctuator did not request commit for running client");
            commitRequested.set(0);
            stateTransitions1.clear();
            stateTransitions2.clear();
            streams2Beta = getKafkaStreams("appDir1", StreamsConfig.EXACTLY_ONCE_BETA);
            streams2Beta.setStateListener(
                (newState, oldState) -> stateTransitions2.add(KeyValue.pair(oldState, newState))
            );
            assignmentListener.prepareForRebalance();
            streams2Beta.start();
            assignmentListener.waitForNextStableAssignment(MAX_WAIT_TIME_MS);
            waitForRunning(stateTransitions1);
            waitForRunning(stateTransitions2);

            committedKeys.addAll(mkSet(0L, 1L, 2L, 3L));
            if (!injectError) {
                committedKeys.removeAll(keyFilterSecondClient);
            }

            final List<KeyValue<Long, Long>> expectedCommittedResultAfterRestartSecondClient = computeExpectedResult(
                uncommittedInputDataBeforeSecondUpgrade
                    .stream()
                    .filter(pair -> committedKeys.contains(pair.key))
                    .collect(Collectors.toList()),
                committedState
            );
            verifyCommitted(expectedCommittedResultAfterRestartSecondClient);

            // phase 11: (complete fourth batch of data)
            // expected end state per output partition (C == COMMIT; A == ABORT; ---> indicate the changes):
            //
            // stop case:
            //   p-0: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec + C ---> 5 rec + C
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + C + 5 rec + C ---> 5 rec + C
            // crash case:  (we just assumes that we inject the error for p-2; in reality it might be a different partition)
            //   p-0: 10 rec + C + 4 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C + 5 rec + C ---> 5 rec + C
            //   p-1: 10 rec + C + 5 rec + A + 5 rec + C + 5 rec + C + 10 rec + A + 10 rec + C + 5 rec + C ---> 5 rec + C
            //   p-2: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C + 4 rec + A + 5 rec + C ---> 5 rec + C
            //   p-3: 10 rec + C + 5 rec + C + 5 rec + A + 5 rec + C + 10 rec + C + 5 rec + A + 5 rec + C ---> 5 rec + C
            commitCounterClient1.set(-1);
            commitCounterClient2.set(-1);

            final List<KeyValue<Long, Long>> committedInputDataAfterUpgrade =
                prepareData(35L, 40L, 0L, 1L, 2L, 3L);
            writeInputData(committedInputDataAfterUpgrade);

            final List<KeyValue<Long, Long>> expectedCommittedResult =
                computeExpectedResult(committedInputDataAfterUpgrade, committedState);
            verifyCommitted(expectedCommittedResult);
        } finally {
            if (streams1Alpha != null) {
                streams1Alpha.close();
            }
            if (streams1Beta != null) {
                streams1Beta.close();
            }
            if (streams1BetaTwo != null) {
                streams1BetaTwo.close();
            }
            if (streams2Alpha != null) {
                streams2Alpha.close();
            }
            if (streams2AlphaTwo != null) {
                streams2AlphaTwo.close();
            }
            if (streams2Beta != null) {
                streams2Beta.close();
            }
        }
    }

    private KafkaStreams getKafkaStreams(final String appDir,
                                         final String processingGuarantee) {
        final StreamsBuilder builder = new StreamsBuilder();

        final String[] storeNames = new String[] {storeName};
        final StoreBuilder<KeyValueStore<Long, Long>> storeBuilder = Stores
            .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), Serdes.Long(), Serdes.Long())
            .withCachingEnabled();

        builder.addStateStore(storeBuilder);

        final KStream<Long, Long> input = builder.stream(MULTI_PARTITION_INPUT_TOPIC);
        input.transform(new TransformerSupplier<Long, Long, KeyValue<Long, Long>>() {
            @SuppressWarnings("unchecked")
            @Override
            public Transformer<Long, Long, KeyValue<Long, Long>> get() {
                return new Transformer<Long, Long, KeyValue<Long, Long>>() {
                    ProcessorContext context;
                    KeyValueStore<Long, Long> state = null;
                    AtomicBoolean crash;
                    AtomicInteger sharedCommit;
                    Cancellable punctuator;

                    @Override
                    public void init(final ProcessorContext context) {
                        this.context = context;
                        state = (KeyValueStore<Long, Long>) context.getStateStore(storeName);
                        final String clientId = context.appConfigs().get(StreamsConfig.CLIENT_ID_CONFIG).toString();
                        if ("appDir1".equals(clientId)) {
                            crash = errorInjectedClient1;
                            sharedCommit = commitCounterClient1;
                        } else {
                            crash = errorInjectedClient2;
                            sharedCommit = commitCounterClient2;
                        }
                        punctuator = context.schedule(
                            Duration.ofMillis(100),
                            PunctuationType.WALL_CLOCK_TIME,
                            new CommitPunctuator(context, requestCommit)
                        );
                    }

                    @Override
                    public KeyValue<Long, Long> transform(final Long key, final Long value) {
                        if ((value + 1) % 10 == 0) {
                            if (sharedCommit.get() < 0 ||
                                sharedCommit.incrementAndGet() == 2) {

                                context.commit();
                            }
                            commitRequested.incrementAndGet();
                        }

                        Long sum = state.get(key);
                        if (sum == null) {
                            sum = value;
                        } else {
                            sum += value;
                        }
                        state.put(key, sum);
                        state.flush();

                        if (value % 10 == 4 && // potentially crash when processing 5th, 15th, or 25th record (etc.)
                            crash != null && crash.compareAndSet(true, false)) {
                            // only crash a single task
                            throw new RuntimeException("Injected test exception.");
                        }

                        return new KeyValue<>(key, state.get(key));
                    }

                    @Override
                    public void close() {
                        punctuator.cancel();
                    }
                };
            } }, storeNames)
            .to(MULTI_PARTITION_OUTPUT_TOPIC);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, appDir);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Long.MAX_VALUE);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), "1000");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), 5 * 1000);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 5 * 1000 - 1);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), MAX_POLL_INTERVAL_MS);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.PARTITIONER_CLASS_CONFIG), KeyPartitioner.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + File.separator + appDir);
        properties.put(InternalConfig.ASSIGNMENT_LISTENER, assignmentListener);

        final Properties config = StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
            Serdes.LongSerde.class.getName(),
            Serdes.LongSerde.class.getName(),
            properties
        );

        final KafkaStreams streams = new KafkaStreams(builder.build(), config, new TestKafkaClientSupplier());

        streams.setUncaughtExceptionHandler((t, e) -> {
            if (uncaughtException != null) {
                e.printStackTrace(System.err);
                fail("Should only get one uncaught exception from Streams.");
            }
            uncaughtException = e;
        });

        return streams;
    }

    private void waitForRunning(final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> observed) throws Exception {
        waitForCondition(
            () -> !observed.isEmpty() && observed.get(observed.size() - 1).value.equals(State.RUNNING),
            MAX_WAIT_TIME_MS,
            () -> "Client did not startup on time. Observers transitions: " + observed
        );
    }

    private void waitForStateTransition(final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> observed,
                                        final List<KeyValue<KafkaStreams.State, KafkaStreams.State>> expected)
            throws Exception {

        waitForCondition(
            () -> observed.equals(expected),
            MAX_WAIT_TIME_MS,
            () -> "Client did not startup on time. Observers transitions: " + observed
        );
    }

    private List<KeyValue<Long, Long>> prepareData(final long fromInclusive,
                                                   final long toExclusive,
                                                   final Long... keys) {
        final List<KeyValue<Long, Long>> data = new ArrayList<>();

        for (final Long k : keys) {
            for (long v = fromInclusive; v < toExclusive; ++v) {
                data.add(new KeyValue<>(k, v));
            }
        }

        return data;
    }

    private void writeInputData(final List<KeyValue<Long, Long>> records) {
        final Properties config = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            LongSerializer.class,
            LongSerializer.class
        );
        config.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, KeyPartitioner.class.getName());
        IntegrationTestUtils.produceKeyValuesSynchronously(
            MULTI_PARTITION_INPUT_TOPIC,
            records,
            config,
            CLUSTER.time
        );
    }

    private void verifyCommitted(final List<KeyValue<Long, Long>> expectedResult) throws Exception {
        final List<KeyValue<Long, Long>> committedOutput = readResult(expectedResult.size(), true);
        checkResultPerKey(committedOutput, expectedResult);
    }

    private void verifyUncommitted(final List<KeyValue<Long, Long>> expectedResult) throws Exception {
        final List<KeyValue<Long, Long>> uncommittedOutput = readResult(expectedResult.size(), false);
        checkResultPerKey(uncommittedOutput, expectedResult);
    }

    private List<KeyValue<Long, Long>> readResult(final int numberOfRecords,
                                                  final boolean readCommitted) throws Exception {
        if (readCommitted) {
            return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    CONSUMER_GROUP_ID,
                    LongDeserializer.class,
                    LongDeserializer.class,
                    Utils.mkProperties(Collections.singletonMap(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT))
                    )
                ),
                MULTI_PARTITION_OUTPUT_TOPIC,
                numberOfRecords
            );
        }

        // read uncommitted
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), LongDeserializer.class, LongDeserializer.class),
            MULTI_PARTITION_OUTPUT_TOPIC,
            numberOfRecords
        );
    }

    private void checkResultPerKey(final List<KeyValue<Long, Long>> result,
                                   final List<KeyValue<Long, Long>> expectedResult) {
        final Set<Long> allKeys = new HashSet<>();
        addAllKeys(allKeys, result);
        addAllKeys(allKeys, expectedResult);

        for (final Long key : allKeys) {
            assertThat(getAllRecordPerKey(key, result), equalTo(getAllRecordPerKey(key, expectedResult)));
        }
    }

    private void addAllKeys(final Set<Long> allKeys, final List<KeyValue<Long, Long>> records) {
        for (final KeyValue<Long, Long> record : records) {
            allKeys.add(record.key);
        }
    }

    private List<KeyValue<Long, Long>> getAllRecordPerKey(final Long key, final List<KeyValue<Long, Long>> records) {
        final List<KeyValue<Long, Long>> recordsPerKey = new ArrayList<>(records.size());

        for (final KeyValue<Long, Long> record : records) {
            if (record.key.equals(key)) {
                recordsPerKey.add(record);
            }
        }

        return recordsPerKey;
    }

    private List<KeyValue<Long, Long>> computeExpectedResult(final List<KeyValue<Long, Long>> input,
                                                             final Map<Long, Long> currentState) {
        final List<KeyValue<Long, Long>> expectedResult = new ArrayList<>(input.size());

        for (final KeyValue<Long, Long> record : input) {
            final long sum = currentState.getOrDefault(record.key, 0L);
            currentState.put(record.key, sum + record.value);
            expectedResult.add(new KeyValue<>(record.key, sum + record.value));
        }

        return expectedResult;
    }

    private Set<Long> keysFromInstance(final KafkaStreams streams) throws Exception {
        final ReadOnlyKeyValueStore<Long, Long> store = getStore(
            MAX_WAIT_TIME_MS,
            streams,
            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())
        );
        waitForCondition(() -> store.get(-1L) == null, MAX_WAIT_TIME_MS, () -> "State store did not ready: " + storeName);
        final Set<Long> keys = new HashSet<>();
        try (final KeyValueIterator<Long, Long> it = store.all()) {
            while (it.hasNext()) {
                final KeyValue<Long, Long> row = it.next();
                keys.add(row.key);
            }
        }
        return keys;
    }

    // must be public to allow KafkaProducer to instantiate it
    public static class KeyPartitioner implements Partitioner {
        private final static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

        @Override
        public int partition(final String topic,
                             final Object key,
                             final byte[] keyBytes,
                             final Object value,
                             final byte[] valueBytes,
                             final Cluster cluster) {
            return LONG_DESERIALIZER.deserialize(topic, keyBytes).intValue() % NUM_TOPIC_PARTITIONS;
        }

        @Override
        public void close() {}

        @Override
        public void configure(final Map<String, ?> configs) {}
    }

    private class TestKafkaClientSupplier extends DefaultKafkaClientSupplier {
        @Override
        public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
            return new ErrorInjector(config);
        }
    }

    private class ErrorInjector extends KafkaProducer<byte[], byte[]> {
        private final AtomicBoolean crash;

        public ErrorInjector(final Map<String, Object> configs) {
            super(configs, new ByteArraySerializer(), new ByteArraySerializer());
            final String clientId = configs.get(ProducerConfig.CLIENT_ID_CONFIG).toString();
            if (clientId.contains("appDir1")) {
                crash = commitErrorInjectedClient1;
            } else {
                crash = commitErrorInjectedClient2;
            }
        }

        @Override
        public void commitTransaction() throws ProducerFencedException {
            super.flush(); // we flush to ensure that the offsets are written
            if (!crash.compareAndSet(true, false)) {
                super.commitTransaction();
            } else {
                throw new RuntimeException("Injected producer commit exception.");
            }
        }
    }
}
