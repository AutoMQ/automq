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

package kafka.automq.zerozone;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
@Tag("S3Unit")
public class RouterOutTest {

    @Test
    public void testProxyRequestCompleteWithError() throws Exception {
        // Test that ProxyRequest.completeWithUnknownError properly completes the future
        ProduceRequestData data = createProduceRequestData("topic1", 0);
        RouterOut.ProxyRequest request = new RouterOut.ProxyRequest((short) 9, (short) 0, data, 100);

        assertFalse(request.cf.isDone(), "Future should not be completed initially");

        request.completeWithUnknownError();

        assertTrue(request.cf.isDone(), "Future should be completed after completeWithUnknownError");

        Map<TopicPartition, ProduceResponse.PartitionResponse> result = request.cf.get(1, TimeUnit.SECONDS);
        assertEquals(1, result.size(), "Should have one partition response");

        TopicPartition tp = new TopicPartition("topic1", 0);
        assertTrue(result.containsKey(tp), "Should contain the expected topic partition");
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, result.get(tp).error,
            "Should be completed with UNKNOWN_SERVER_ERROR");
    }

    @Test
    public void testProxyRequestCompleteWithNotLeaderNotFollower() throws Exception {
        ProduceRequestData data = createProduceRequestData("topic1", 0);
        RouterOut.ProxyRequest request = new RouterOut.ProxyRequest((short) 9, (short) 0, data, 100);

        request.completeWithNotLeaderNotFollower();

        assertTrue(request.cf.isDone(), "Future should be completed");

        Map<TopicPartition, ProduceResponse.PartitionResponse> result = request.cf.get(1, TimeUnit.SECONDS);
        TopicPartition tp = new TopicPartition("topic1", 0);
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, result.get(tp).error,
            "Should be completed with NOT_LEADER_OR_FOLLOWER");
    }

    @Test
    public void testProxyRequestTopicPartitionsExtraction() {
        ProduceRequestData data = new ProduceRequestData();
        ProduceRequestData.TopicProduceDataCollection topicData = new ProduceRequestData.TopicProduceDataCollection();
        topicData.add(new ProduceRequestData.TopicProduceData()
            .setName("topic1")
            .setPartitionData(List.of(
                new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(null),
                new ProduceRequestData.PartitionProduceData().setIndex(1).setRecords(null)
            )));
        topicData.add(new ProduceRequestData.TopicProduceData()
            .setName("topic2")
            .setPartitionData(List.of(
                new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(null)
            )));
        data.setTopicData(topicData);

        RouterOut.ProxyRequest request = new RouterOut.ProxyRequest((short) 9, (short) 0, data, 100);

        assertEquals(3, request.topicPartitions.size(), "Should extract 3 topic partitions");
        assertTrue(request.topicPartitions.contains(new TopicPartition("topic1", 0)));
        assertTrue(request.topicPartitions.contains(new TopicPartition("topic1", 1)));
        assertTrue(request.topicPartitions.contains(new TopicPartition("topic2", 0)));
    }

    @Test
    public void testProxyRequestAfterRouter() {
        ProduceRequestData data = createProduceRequestData("topic1", 0);
        RouterOut.ProxyRequest request = new RouterOut.ProxyRequest((short) 9, (short) 0, data, 100);

        // Data should be set initially
        assertEquals(data, request.data);

        // After router, data should be nullified for GC
        request.afterRouter();

        assertNull(request.data, "Data should be null after afterRouter() for GC");
    }

    @Test
    public void testMultipleProxyRequestsCompleteWithError() throws Exception {
        // Simulate the scenario where multiple requests need to be completed with error
        // (as would happen in handleRouterResponse when size mismatch occurs)
        ProduceRequestData data1 = createProduceRequestData("topic1", 0);
        ProduceRequestData data2 = createProduceRequestData("topic2", 1);

        RouterOut.ProxyRequest request1 = new RouterOut.ProxyRequest((short) 9, (short) 0, data1, 100);
        RouterOut.ProxyRequest request2 = new RouterOut.ProxyRequest((short) 9, (short) 0, data2, 100);

        List<RouterOut.ProxyRequest> requests = List.of(request1, request2);

        // Simulate what handleRouterResponse does on size mismatch
        requests.forEach(RouterOut.ProxyRequest::completeWithUnknownError);

        // Verify all requests are completed with error
        for (RouterOut.ProxyRequest request : requests) {
            assertTrue(request.cf.isDone(), "All requests should be completed");
            Map<TopicPartition, ProduceResponse.PartitionResponse> result = request.cf.get(1, TimeUnit.SECONDS);
            for (ProduceResponse.PartitionResponse response : result.values()) {
                assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error);
            }
        }
    }

    private ProduceRequestData createProduceRequestData(String topic, int partition) {
        ProduceRequestData data = new ProduceRequestData();
        ProduceRequestData.TopicProduceDataCollection topicData = new ProduceRequestData.TopicProduceDataCollection();
        topicData.add(new ProduceRequestData.TopicProduceData()
            .setName(topic)
            .setPartitionData(List.of(
                new ProduceRequestData.PartitionProduceData()
                    .setIndex(partition)
                    .setRecords(null)
            )));
        data.setTopicData(topicData);
        return data;
    }
}
