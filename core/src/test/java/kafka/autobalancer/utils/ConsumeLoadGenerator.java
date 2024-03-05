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

package kafka.autobalancer.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class ConsumeLoadGenerator extends AbstractLoadGenerator {
    private final Properties props;

    public ConsumeLoadGenerator(Properties consumerProps, String topic, int partition) {
        super(topic, partition);
        this.props = consumerProps;
    }

    @Override
    public void run() {
        try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(this.props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            Optional<PartitionInfo> partitionInfo = partitions.stream().filter(p -> p.topic().equals(topic) && p.partition() == partition).findFirst();
            Assertions.assertTrue(partitionInfo.isPresent());
            List<TopicPartition> topicPartitions = List.of(new TopicPartition(partitionInfo.get().topic(), partitionInfo.get().partition()));
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);
            while (!shutdown) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                    msgSucceed.addAndGet(records.count());
                } catch (Exception ignored) {

                }
            }
        }
    }
}
