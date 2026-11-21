/*
 * Copyright 2026, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.table;

import kafka.automq.AutoMQConfig;
import kafka.automq.table.events.CommitResponse;
import kafka.automq.table.events.Errors;
import kafka.automq.table.events.Event;
import kafka.automq.table.events.EventType;
import kafka.automq.table.events.TopicMetric;
import kafka.automq.table.events.WorkerOffset;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class ChannelTest {
    private static final int MIB = 1024 * 1024;

    /** The default producer configuration accommodates a commit response larger than Kafka's 1 MiB default. */
    @Test
    void testLargeCommitResponseFitsDefaultProducerLimit() throws Exception {
        String dataTopic = Topic.TABLE_TOPIC_DATA_TOPIC_NAME;
        try (var producers = mockConstruction(KafkaProducer.class, (producer, context) -> {
            ProducerConfig config = new ProducerConfig((Properties) context.arguments().get(0));
            int maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            assertEquals(20 * MIB, maxRequestSize);
            when(producer.partitionsFor(dataTopic))
                .thenReturn(List.of(new PartitionInfo(dataTopic, 0, null, new Node[0], new Node[0])));
            when(producer.send(any(), any())).thenAnswer(invocation -> {
                ProducerRecord<byte[], byte[]> record = invocation.getArgument(0);
                assertEquals(dataTopic, record.topic());
                int estimatedSize = AbstractRecords.estimateSizeInBytesUpperBound(RecordBatch.CURRENT_MAGIC_VALUE,
                    CompressionType.NONE, record.key(), record.value(), record.headers().toArray());
                assertTrue(estimatedSize > MIB, "The commit response must exceed the previous producer limit");
                assertTrue(estimatedSize <= maxRequestSize, "The commit response must fit the configured producer limit");
                invocation.<Callback>getArgument(1).onCompletion(null, null);
                return CompletableFuture.completedFuture(null);
            });
        })) {
            String topic = "events-backfill";
            Channel channel = new Channel(KafkaConfig.fromProps(TestUtils.createDummyBrokerConfig()));
            channel.asyncSend(topic, largeCommitResponse(topic)).get(10, TimeUnit.SECONDS);
            assertEquals(1, producers.constructed().size());
        }
    }

    /** A broker override reaches the producer and allows a single response to exceed its default buffer size. */
    @Test
    void testCustomProducerLimit() {
        long maxRequestSize = 40 * MIB;
        Properties brokerProps = TestUtils.createDummyBrokerConfig();
        brokerProps.setProperty(AutoMQConfig.TABLE_TOPIC_PRODUCER_MAX_REQUEST_SIZE_CONFIG, Long.toString(maxRequestSize));
        try (var producers = mockConstruction(KafkaProducer.class, (producer, context) -> {
            ProducerConfig config = new ProducerConfig((Properties) context.arguments().get(0));
            assertEquals(maxRequestSize, config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG).longValue());
            assertEquals(maxRequestSize, config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG).longValue());
        })) {
            new Channel(KafkaConfig.fromProps(brokerProps));
            assertEquals(1, producers.constructed().size());
        }
    }

    private Event largeCommitResponse(String topic) {
        PartitionSpec spec = PartitionSpec.builderFor(new Schema(Types.NestedField.required(1, "customer_id", Types.IntegerType.get())))
            .identity("customer_id").build();
        int partitionCount = 8192;
        List<DataFile> files = IntStream.range(0, partitionCount).mapToObj(i -> DataFiles.builder(spec)
            .withPath("s3://warehouse-bucket/iceberg/analytics/events_backfill/data/customer_id=" + i
                + "/00000-0-1c3dd933-9416-4afc-873f-828f71585c2a-00001.parquet")
            .withPartitionPath("customer_id=" + i)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1024L)
            .withRecordCount(1)
            .build()).toList();
        CommitResponse response = new CommitResponse(spec.partitionType(), Errors.NONE, new UUID(0, 1), topic,
            List.of(new WorkerOffset(0, 1, partitionCount)), files, List.of(), TopicMetric.NOOP, List.of());
        return new Event(1, EventType.COMMIT_RESPONSE, response);
    }
}
