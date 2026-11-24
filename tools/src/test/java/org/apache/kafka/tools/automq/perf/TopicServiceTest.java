/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package org.apache.kafka.tools.automq.perf;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Protects exact topic lookup and its failure behavior without contacting a broker.
 */
@Tag("S3Unit")
class TopicServiceTest {

    /**
     * Given a plain or perf-prefixed topic, describe only its exact name and use the actual partition count.
     */
    @Test
    void shouldDescribeOnlyExactTopicWithActualPartitions() {
        for (String name : List.of("existing-topic", "__automq_perf_existing_0750_0000000")) {
            Admin admin = mock(Admin.class);
            DescribeTopicsResult result = mock(DescribeTopicsResult.class);
            List<TopicPartitionInfo> partitions = IntStream.range(0, 5000)
                .mapToObj(i -> new TopicPartitionInfo(i, Node.noNode(), List.of(), List.of()))
                .collect(Collectors.toList());
            when(admin.describeTopics(List.of(name))).thenReturn(result);
            when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of(name, new TopicDescription(name, false, partitions))));

            TopicService.Topic topic = new TopicService(admin).describeTopic(name);

            assertEquals(name, topic.name());
            assertEquals(5000, topic.partitions().size());
            assertEquals(4999, topic.partitions().get(4999).partition());
            verify(admin).describeTopics(List.of(name));
            verifyNoMoreInteractions(admin);
        }
    }

    /**
     * Given a missing topic, fail with the admin error instead of creating a replacement.
     */
    @Test
    void shouldFailWhenExistingTopicIsMissing() {
        Admin admin = mock(Admin.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        KafkaFutureImpl<Map<String, TopicDescription>> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new UnknownTopicOrPartitionException("missing-topic"));
        when(admin.describeTopics(List.of("missing-topic"))).thenReturn(result);
        when(result.allTopicNames()).thenReturn(future);

        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> new TopicService(admin).describeTopic("missing-topic"));

        assertEquals("Failed to describe existing topic: missing-topic", exception.getMessage());
        assertInstanceOf(UnknownTopicOrPartitionException.class, exception.getCause().getCause());
        verify(admin).describeTopics(List.of("missing-topic"));
        verifyNoMoreInteractions(admin);
    }

    /**
     * Given an incomplete admin response, fail instead of silently selecting another topic.
     */
    @Test
    void shouldFailWhenTopicDescriptionIsAbsent() {
        Admin admin = mock(Admin.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(List.of("existing-topic"))).thenReturn(result);
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of()));

        assertThrows(IllegalStateException.class,
            () -> new TopicService(admin).describeTopic("existing-topic"));
        verify(admin).describeTopics(List.of("existing-topic"));
        verifyNoMoreInteractions(admin);
    }

    /**
     * Given an interrupted describe request, preserve the interruption and stop topic selection.
     */
    @Test
    void shouldPreserveInterruptWhenDescribingTopic() throws ExecutionException, InterruptedException {
        Admin admin = mock(Admin.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        @SuppressWarnings("unchecked")
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);
        when(admin.describeTopics(List.of("existing-topic"))).thenReturn(result);
        when(result.allTopicNames()).thenReturn(future);
        when(future.get()).thenThrow(new InterruptedException());

        try {
            RuntimeException exception = assertThrows(RuntimeException.class,
                () -> new TopicService(admin).describeTopic("existing-topic"));
            assertInstanceOf(InterruptedException.class, exception.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            verify(admin).describeTopics(List.of("existing-topic"));
            verifyNoMoreInteractions(admin);
        } finally {
            Thread.interrupted();
        }
    }
}
