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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.cloudevents.CloudEvent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class ClusterEventPublisherTest {

    /**
     * Pending events must not hold broker shutdown open until the delivery timeout
     * when the event topic's broker cannot respond.
     */
    @Test
    void testShutdownWithUndeliverableEvent() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // Reserve a local endpoint that never responds to Kafka requests.
        try (ServerSocket endpoint = new ServerSocket(0)) {
            ClusterEventPublisher.setup(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + endpoint.getLocalPort(),
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000,
                ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, false));
            AtomicReference<IClusterEventPublisher> instance =
                TestUtils.fieldValue(null, ClusterEventPublisher.class, "INSTANCE");
            KafkaProducer<String, CloudEvent> producer =
                TestUtils.fieldValue(instance.get(), ClusterEventPublisher.class, "producer");
            try {
                ProducerMetadata metadata = TestUtils.fieldValue(producer, KafkaProducer.class, "metadata");
                metadata.add(Topic.CLUSTER_EVENTS_TOPIC_NAME, System.currentTimeMillis());
                MetadataResponse response = RequestTestUtils.metadataUpdateWith(1,
                    Map.of(Topic.CLUSTER_EVENTS_TOPIC_NAME, 1));
                response.data().brokers().find(0).setHost("127.0.0.1").setPort(endpoint.getLocalPort());
                metadata.updateWithCurrentRequestVersion(response, false, System.currentTimeMillis());

                ClusterEventPublisher.publish("com.automq.test", "/automq/broker/0", null,
                    "com.automq.events.TestEvent", new byte[]{1});
                RecordAccumulator accumulator = TestUtils.fieldValue(producer, KafkaProducer.class, "accumulator");
                assertTrue(accumulator.hasIncomplete(), "The event must still be waiting for delivery");

                Future<?> shutdown = executor.submit(ClusterEventPublisher::shutdown);
                shutdown.get(10, TimeUnit.SECONDS);

                assertFalse(accumulator.hasIncomplete(), "Undelivered events must be released on close");
                Thread ioThread = TestUtils.fieldValue(producer, KafkaProducer.class, "ioThread");
                assertFalse(ioThread.isAlive(), "The producer sender must stop on close");
                ClusterEventPublisher.shutdown();
            } finally {
                producer.close(Duration.ZERO);
                ClusterEventPublisher.shutdown();
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
