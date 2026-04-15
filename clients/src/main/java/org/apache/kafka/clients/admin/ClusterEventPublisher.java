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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventSerializer;

/**
 * Publishes cluster events to the {@code __automq_cluster_events} internal topic.
 *
 * <p>Events are written asynchronously and best-effort: if the producer fails to send an event,
 * a warning is logged and the event is dropped. Events must never block or fail the critical path
 * (rebalancing, failover, request handling).
 *
 * <p>Example usage:
 * <pre>{@code
 * // At broker startup
 * ClusterEventPublisher.setup(Map.of("bootstrap.servers", "localhost:9092"));
 *
 * // From any code path
 * ClusterEventPublisher.publish(
 *     "com.automq.risk.request_error",
 *     "/automq/broker/0",
 *     "PRODUCE:my-topic",
 *     "com.automq.events.RequestErrorEvent",
 *     requestErrorEvent.toByteArray());
 *
 * // At shutdown
 * ClusterEventPublisher.shutdown();
 * }</pre>
 */
public class ClusterEventPublisher implements IClusterEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventPublisher.class);

    private static final AtomicReference<IClusterEventPublisher> INSTANCE =
        new AtomicReference<>(NoopClusterEventPublisher.INSTANCE);

    private final KafkaProducer<String, CloudEvent> producer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a publisher from a config map. The map should contain at least
     * {@link ProducerConfig#BOOTSTRAP_SERVERS_CONFIG}. Any additional producer or security
     * properties (e.g. SASL/SSL) can be included directly. Serializer, acks, retries,
     * batch.size and linger.ms are set with sensible defaults if not provided.
     *
     * @param config producer configuration map
     */
    private ClusterEventPublisher(Map<String, Object> config) {
        Map<String, Object> props = new HashMap<>(config);
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class.getName());
        props.putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, ClusterEventsConfig.DEFAULT_PUBLISHER_BATCH_SIZE);
        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, ClusterEventsConfig.DEFAULT_PUBLISHER_LINGER_MS);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
    }

    // ---- Global singleton ----

    /**
     * Initialize the global singleton publisher. If already set up, the previous instance
     * is closed and replaced.
     *
     * @param config producer configuration map
     */
    public static void setup(Map<String, Object> config) {
        IClusterEventPublisher prev = INSTANCE.getAndSet(new ClusterEventPublisher(config));
        if (prev != null) {
            prev.close();
        }
    }

    /**
     * Shut down the global singleton publisher, reverting to the no-op instance.
     */
    public static void shutdown() {
        IClusterEventPublisher prev = INSTANCE.getAndSet(NoopClusterEventPublisher.INSTANCE);
        if (prev != null) {
            prev.close();
        }
    }

    /**
     * Publish an event via the global singleton. If {@link #setup(Map)} has not been called,
     * this is a no-op.
     */
    public static void publish(String type, String source, String subject, String dataSchema, byte[] data) {
        INSTANCE.get().publishEvent(type, source, subject, dataSchema, data);
    }

    // ---- Instance methods ----

    @Override
    public void publishEvent(String type, String source, String subject, String dataSchema, byte[] data) {
        if (closed.get()) {
            log.warn("ClusterEventPublisher is closed, dropping event type={}", type);
            return;
        }

        CloudEvent event = buildEvent(type, source, subject, dataSchema, data);
        String key = type + ":" + event.getId();
        ProducerRecord<String, CloudEvent> record =
            new ProducerRecord<>(Topic.CLUSTER_EVENTS_TOPIC_NAME, key, event);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.warn("Failed to publish cluster event type={}, dropping: {}",
                    record.value().getType(), exception.getMessage());
            }
        });
    }

    private CloudEvent buildEvent(String type, String source, String subject, String dataSchema, byte[] data) {
        CloudEventBuilder builder = CloudEventBuilder.v1()
            .withId(UUID.randomUUID().toString())
            .withType(type)
            .withSource(URI.create(source))
            .withTime(OffsetDateTime.now())
            .withDataContentType("application/protobuf")
            .withDataSchema(URI.create(dataSchema))
            .withData("application/protobuf", data);

        if (subject != null) {
            builder = builder.withSubject(subject);
        }

        return builder.build();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            producer.close();
        }
    }
}
