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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckpointTest {

    @Test
    public void testSerde() {
        Checkpoint checkpoint = new Checkpoint("group-1", new TopicPartition("topic-2", 3), 4, 5, "metadata-6");
        byte[] key = checkpoint.recordKey();
        byte[] value = checkpoint.recordValue();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("any-topic", 7, 8, key, value);
        Checkpoint deserialized = Checkpoint.deserializeRecord(record);
        assertEquals(checkpoint.consumerGroupId(), deserialized.consumerGroupId());
        assertEquals(checkpoint.topicPartition(), deserialized.topicPartition());
        assertEquals(checkpoint.upstreamOffset(), deserialized.upstreamOffset());
        assertEquals(checkpoint.downstreamOffset(), deserialized.downstreamOffset());
    }
}
