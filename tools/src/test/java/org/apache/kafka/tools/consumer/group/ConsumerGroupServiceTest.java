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
package org.apache.kafka.tools.consumer.group;

import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerGroupServiceTest {
    public static final String GROUP = "testGroup";

    public static final int NUM_PARTITIONS = 10;

    private static final List<String> TOPICS = IntStream.range(0, 5).mapToObj(i -> "testTopic" + i).collect(Collectors.toList());

    private static final List<TopicPartition> TOPIC_PARTITIONS = TOPICS.stream()
            .flatMap(topic -> IntStream.range(0, NUM_PARTITIONS).mapToObj(i -> new TopicPartition(topic, i)))
            .collect(Collectors.toList());

    private final Admin admin = mock(Admin.class);

    @Test
    public void testAdminRequestsForDescribeOffsets() {
        String[] args = new String[]{"--bootstrap-server", "localhost:9092", "--group", GROUP, "--describe", "--offsets"};
        ConsumerGroupCommand.ConsumerGroupService groupService = consumerGroupService(args);

        when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(GROUP)), any()))
                .thenReturn(describeGroupsResult(ConsumerGroupState.STABLE));
        when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any()))
                .thenReturn(listGroupOffsetsResult(GROUP));
        when(admin.listOffsets(offsetsArgMatcher(), any()))
                .thenReturn(listOffsetsResult());

        Tuple2<Option<String>, Option<Seq<ConsumerGroupCommand.PartitionAssignmentState>>> statesAndAssignments = groupService.collectGroupOffsets(GROUP);
        assertEquals(Some.apply("Stable"), statesAndAssignments._1);
        assertTrue(statesAndAssignments._2.isDefined());
        assertEquals(TOPIC_PARTITIONS.size(), statesAndAssignments._2.get().size());

        verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(GROUP)), any());
        verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any());
        verify(admin, times(1)).listOffsets(offsetsArgMatcher(), any());
    }

    @Test
    public void testAdminRequestsForDescribeNegativeOffsets() {
        String[] args = new String[]{"--bootstrap-server", "localhost:9092", "--group", GROUP, "--describe", "--offsets"};
        ConsumerGroupCommand.ConsumerGroupService groupService = consumerGroupService(args);

        TopicPartition testTopicPartition0 = new TopicPartition("testTopic1", 0);
        TopicPartition testTopicPartition1 = new TopicPartition("testTopic1", 1);
        TopicPartition testTopicPartition2 = new TopicPartition("testTopic1", 2);
        TopicPartition testTopicPartition3 = new TopicPartition("testTopic2", 0);
        TopicPartition testTopicPartition4 = new TopicPartition("testTopic2", 1);
        TopicPartition testTopicPartition5 = new TopicPartition("testTopic2", 2);

        // Some topic's partitions gets valid OffsetAndMetadata values, other gets nulls values (negative integers) and others aren't defined
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        committedOffsets.put(testTopicPartition1, new OffsetAndMetadata(100));
        committedOffsets.put(testTopicPartition2, null);
        committedOffsets.put(testTopicPartition3, new OffsetAndMetadata(100));
        committedOffsets.put(testTopicPartition4, new OffsetAndMetadata(100));
        committedOffsets.put(testTopicPartition5, null);

        ListOffsetsResultInfo resultInfo = new ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.of(1));
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> endOffsets = new HashMap<>();

        endOffsets.put(testTopicPartition0, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition1, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition2, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition3, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition4, KafkaFuture.completedFuture(resultInfo));
        endOffsets.put(testTopicPartition5, KafkaFuture.completedFuture(resultInfo));

        Set<TopicPartition> assignedTopicPartitions = new HashSet<>(Arrays.asList(testTopicPartition0, testTopicPartition1, testTopicPartition2));
        Set<TopicPartition> unassignedTopicPartitions = new HashSet<>(Arrays.asList(testTopicPartition3, testTopicPartition4, testTopicPartition5));

        ConsumerGroupDescription consumerGroupDescription = new ConsumerGroupDescription(GROUP,
                true,
                Collections.singleton(new MemberDescription("member1", Optional.of("instance1"), "client1", "host1", new MemberAssignment(assignedTopicPartitions))),
                RangeAssignor.class.getName(),
                ConsumerGroupState.STABLE,
                new Node(1, "localhost", 9092));

        Function<Collection<TopicPartition>, ArgumentMatcher<Map<TopicPartition, OffsetSpec>>> offsetsArgMatcher = expectedPartitions ->
                topicPartitionOffsets -> topicPartitionOffsets != null && topicPartitionOffsets.keySet().equals(expectedPartitions);

        KafkaFutureImpl<ConsumerGroupDescription> future = new KafkaFutureImpl<>();
        future.complete(consumerGroupDescription);
        when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(GROUP)), any()))
                .thenReturn(new DescribeConsumerGroupsResult(Collections.singletonMap(GROUP, future)));
        when(admin.listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any()))
                .thenReturn(
                        AdminClientTestUtils.listConsumerGroupOffsetsResult(
                                Collections.singletonMap(GROUP, committedOffsets)));
        when(admin.listOffsets(
                ArgumentMatchers.argThat(offsetsArgMatcher.apply(assignedTopicPartitions)),
                any()
        )).thenReturn(new ListOffsetsResult(endOffsets.entrySet().stream().filter(e -> assignedTopicPartitions.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
        when(admin.listOffsets(
                ArgumentMatchers.argThat(offsetsArgMatcher.apply(unassignedTopicPartitions)),
                any()
        )).thenReturn(new ListOffsetsResult(endOffsets.entrySet().stream().filter(e -> unassignedTopicPartitions.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));

        Tuple2<Option<String>, Option<Seq<ConsumerGroupCommand.PartitionAssignmentState>>> statesAndAssignments = groupService.collectGroupOffsets(GROUP);
        Option<String> state = statesAndAssignments._1;
        Option<Seq<ConsumerGroupCommand.PartitionAssignmentState>> assignments = statesAndAssignments._2;

        Map<TopicPartition, Optional<Long>> returnedOffsets = new HashMap<>();
        assignments.foreach(results -> {
            results.foreach(assignment -> {
                returnedOffsets.put(
                        new TopicPartition(assignment.topic().get(), (Integer) assignment.partition().get()),
                        assignment.offset().isDefined() ? Optional.of((Long) assignment.offset().get()) : Optional.empty());
                return null;
            });
            return null;
        });

        Map<TopicPartition, Optional<Long>> expectedOffsets = new HashMap<>();

        expectedOffsets.put(testTopicPartition0, Optional.empty());
        expectedOffsets.put(testTopicPartition1, Optional.of(100L));
        expectedOffsets.put(testTopicPartition2, Optional.empty());
        expectedOffsets.put(testTopicPartition3, Optional.of(100L));
        expectedOffsets.put(testTopicPartition4, Optional.of(100L));
        expectedOffsets.put(testTopicPartition5, Optional.empty());

        assertEquals(Some.apply("Stable"), state);
        assertEquals(expectedOffsets, returnedOffsets);

        verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(GROUP)), any());
        verify(admin, times(1)).listConsumerGroupOffsets(ArgumentMatchers.eq(listConsumerGroupOffsetsSpec()), any());
        verify(admin, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher.apply(assignedTopicPartitions)), any());
        verify(admin, times(1)).listOffsets(ArgumentMatchers.argThat(offsetsArgMatcher.apply(unassignedTopicPartitions)), any());
    }

    @Test
    public void testAdminRequestsForResetOffsets() {
        List<String> args = new ArrayList<>(Arrays.asList("--bootstrap-server", "localhost:9092", "--group", GROUP, "--reset-offsets", "--to-latest"));
        List<String> topicsWithoutPartitionsSpecified = TOPICS.subList(1, TOPICS.size());
        List<String> topicArgs = new ArrayList<>(Arrays.asList("--topic", TOPICS.get(0) + ":" + Utils.mkString(IntStream.range(0, NUM_PARTITIONS).mapToObj(Integer::toString), "", "", ",")));
        topicsWithoutPartitionsSpecified.forEach(topic -> topicArgs.addAll(Arrays.asList("--topic", topic)));

        args.addAll(topicArgs);
        ConsumerGroupCommand.ConsumerGroupService groupService = consumerGroupService(args.toArray(new String[0]));

        when(admin.describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(GROUP)), any()))
                .thenReturn(describeGroupsResult(ConsumerGroupState.DEAD));
        when(admin.describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified), any()))
                .thenReturn(describeTopicsResult(topicsWithoutPartitionsSpecified));
        when(admin.listOffsets(offsetsArgMatcher(), any()))
                .thenReturn(listOffsetsResult());

        scala.collection.Map<String, scala.collection.Map<TopicPartition, OffsetAndMetadata>> resetResult = groupService.resetOffsets();
        assertEquals(set(Collections.singletonList(GROUP)), resetResult.keySet());
        assertEquals(set(TOPIC_PARTITIONS), resetResult.get(GROUP).get().keys().toSet());

        verify(admin, times(1)).describeConsumerGroups(ArgumentMatchers.eq(Collections.singletonList(GROUP)), any());
        verify(admin, times(1)).describeTopics(ArgumentMatchers.eq(topicsWithoutPartitionsSpecified), any());
        verify(admin, times(1)).listOffsets(offsetsArgMatcher(), any());
    }

    private ConsumerGroupCommand.ConsumerGroupService consumerGroupService(String[] args) {
        return new ConsumerGroupCommand.ConsumerGroupService(new kafka.admin.ConsumerGroupCommand.ConsumerGroupCommandOptions(args), Map$.MODULE$.empty()) {
            @Override
            public Admin createAdminClient(scala.collection.Map<String, String> configOverrides) {
                return admin;
            }
        };
    }

    private DescribeConsumerGroupsResult describeGroupsResult(ConsumerGroupState groupState) {
        MemberDescription member1 = new MemberDescription("member1", Optional.of("instance1"), "client1", "host1", null);
        ConsumerGroupDescription description = new ConsumerGroupDescription(GROUP,
                true,
                Collections.singleton(member1),
                RangeAssignor.class.getName(),
                groupState,
                new Node(1, "localhost", 9092));
        KafkaFutureImpl<ConsumerGroupDescription> future = new KafkaFutureImpl<>();
        future.complete(description);
        return new DescribeConsumerGroupsResult(Collections.singletonMap(GROUP, future));
    }

    private ListConsumerGroupOffsetsResult listGroupOffsetsResult(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> offsets = TOPIC_PARTITIONS.stream().collect(Collectors.toMap(
                Function.identity(),
                __ -> new OffsetAndMetadata(100)));
        return AdminClientTestUtils.listConsumerGroupOffsetsResult(Collections.singletonMap(groupId, offsets));
    }

    private Map<TopicPartition, OffsetSpec> offsetsArgMatcher() {
        Map<TopicPartition, OffsetSpec> expectedOffsets = TOPIC_PARTITIONS.stream().collect(Collectors.toMap(
                Function.identity(),
                __ -> OffsetSpec.latest()
        ));
        return ArgumentMatchers.argThat(map ->
                Objects.equals(map.keySet(), expectedOffsets.keySet()) && map.values().stream().allMatch(v -> v instanceof OffsetSpec.LatestSpec)
        );
    }

    private ListOffsetsResult listOffsetsResult() {
        ListOffsetsResultInfo resultInfo = new ListOffsetsResultInfo(100, System.currentTimeMillis(), Optional.of(1));
        Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = TOPIC_PARTITIONS.stream().collect(Collectors.toMap(
                Function.identity(),
                __ -> KafkaFuture.completedFuture(resultInfo)));
        return new ListOffsetsResult(futures);
    }

    private DescribeTopicsResult describeTopicsResult(Collection<String> topics) {
        Map<String, TopicDescription> topicDescriptions = new HashMap<>();

        topics.forEach(topic -> {
            List<TopicPartitionInfo> partitions = IntStream.range(0, NUM_PARTITIONS)
                    .mapToObj(i -> new TopicPartitionInfo(i, null, Collections.emptyList(), Collections.emptyList()))
                    .collect(Collectors.toList());
            topicDescriptions.put(topic, new TopicDescription(topic, false, partitions));
        });
        return AdminClientTestUtils.describeTopicsResult(topicDescriptions);
    }

    private Map<String, ListConsumerGroupOffsetsSpec> listConsumerGroupOffsetsSpec() {
        return Collections.singletonMap(GROUP, new ListConsumerGroupOffsetsSpec());
    }

    @SuppressWarnings({"deprecation"})
    private static <T> scala.collection.immutable.Set<T> set(final Collection<T> set) {
        return JavaConverters.asScalaSet(new HashSet<>(set)).toSet();
    }
}
