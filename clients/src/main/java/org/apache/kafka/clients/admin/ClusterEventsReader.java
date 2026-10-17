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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;

/**
 * Reads events from the {@code __automq_cluster_events} internal topic.
 *
 * <p>Supports both one-shot reads and continuous polling (tail mode).
 * Call {@link #poll(Duration)} repeatedly to consume new events as they arrive.
 * Must be {@link #close() closed} when done.
 */
public class ClusterEventsReader implements Closeable {

    private static final TopicPartition TP =
        new TopicPartition(Topic.CLUSTER_EVENTS_TOPIC_NAME, 0);

    private final KafkaConsumer<String, CloudEvent> consumer;
    private long endOffset;

    ClusterEventsReader(Map<String, Object> consumerConfig, Long sinceMs) {
        this.consumer = createConsumer(consumerConfig);
        consumer.assign(Collections.singletonList(TP));
        seek(sinceMs);
        refreshEndOffset();
    }

    /**
     * Poll for new events.
     *
     * @return events received within the timeout, may be empty
     */
    public List<CloudEvent> poll(Duration timeout) {
        List<CloudEvent> results = new ArrayList<>();
        for (ConsumerRecord<String, CloudEvent> record : consumer.poll(timeout)) {
            if (record.value() != null) {
                results.add(record.value());
            }
        }
        return results;
    }

    /**
     * Returns true if the consumer position has reached the end offset
     * that was snapshotted when this reader was created.
     */
    public boolean isPolledToEnd() {
        return consumer.position(TP) >= endOffset;
    }

    @Override
    public void close() {
        consumer.close();
    }

    private void refreshEndOffset() {
        Long end = consumer.endOffsets(Collections.singletonList(TP)).get(TP);
        this.endOffset = end != null ? end : 0L;
    }

    private void seek(Long sinceMs) {
        if (sinceMs != null) {
            Map<TopicPartition, OffsetAndTimestamp> offsets =
                consumer.offsetsForTimes(Collections.singletonMap(TP, sinceMs));
            OffsetAndTimestamp ot = offsets.get(TP);
            if (ot != null) {
                consumer.seek(TP, ot.offset());
            } else {
                consumer.seekToEnd(Collections.singletonList(TP));
            }
        } else {
            consumer.seekToBeginning(Collections.singletonList(TP));
        }
    }

    private static KafkaConsumer<String, CloudEvent> createConsumer(Map<String, Object> config) {
        Properties props = new Properties();
        props.putAll(config);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    static Map<String, Object> buildConsumerConfig(AdminClientConfig config) {
        Map<String, Object> result = new java.util.HashMap<>();
        result.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            String.join(",", config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)));
        for (Map.Entry<String, ?> entry : config.values().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value != null && (key.startsWith("ssl.") || key.startsWith("sasl.")
                    || key.equals("security.protocol"))) {
                result.put(key, value);
            }
        }
        return result;
    }
}
