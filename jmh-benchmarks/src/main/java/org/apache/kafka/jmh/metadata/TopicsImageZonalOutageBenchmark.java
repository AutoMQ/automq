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

package org.apache.kafka.jmh.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TopicsImageZonalOutageBenchmark {
    @Param({"12500", "25000", "50000", "100000"})
    private int totalTopicCount;
    @Param({"10"})
    private int partitionsPerTopic;
    @Param({"3"})
    private int replicationFactor;
    @Param({"10000"})
    private int numReplicasPerBroker;

    private TopicsDelta topicsDelta;


    @Setup(Level.Trial)
    public void setup() {
        // build an image containing all of the specified topics and partitions
        TopicsDelta buildupTopicsDelta = TopicsImageSnapshotLoadBenchmark.getInitialTopicsDelta(totalTopicCount, partitionsPerTopic, replicationFactor, numReplicasPerBroker);
        TopicsImage builtupTopicsImage = buildupTopicsDelta.apply();
        // build a delta to apply within the benchmark code
        // that perturbs all the topic-partitions for broker 0
        // (as might happen in a zonal outage, one broker at a time, ultimately across 1/3 of the brokers in the cluster).
        // It turns out that
        topicsDelta = new TopicsDelta(builtupTopicsImage);
        Set<Uuid> perturbedTopics = new HashSet<>();
        builtupTopicsImage.topicsById().forEach((topicId, topicImage) ->
            topicImage.partitions().forEach((partitionNumber, partitionRegistration) -> {
                List<Integer> newIsr = Arrays.stream(partitionRegistration.isr).boxed().filter(n -> n != 0).collect(Collectors.toList());
                if (newIsr.size() < replicationFactor) {
                    perturbedTopics.add(topicId);
                    topicsDelta.replay(new PartitionRecord().
                        setPartitionId(partitionNumber).
                        setTopicId(topicId).
                        setReplicas(Arrays.stream(partitionRegistration.replicas).boxed().collect(Collectors.toList())).
                        setIsr(newIsr).
                        setRemovingReplicas(Collections.emptyList()).
                        setAddingReplicas(Collections.emptyList()).
                        setLeader(newIsr.get(0)));
                }
            })
        );
        int numBrokers = TopicsImageSnapshotLoadBenchmark.getNumBrokers(totalTopicCount, partitionsPerTopic, replicationFactor, numReplicasPerBroker);
        System.out.print("(Perturbing 1 of " + numBrokers + " brokers, or " + perturbedTopics.size() + " topics within metadata having " + totalTopicCount + " total topics) ");
    }

    @Benchmark
    public void testTopicsDeltaZonalOutage() {
        topicsDelta.apply();
    }
}
