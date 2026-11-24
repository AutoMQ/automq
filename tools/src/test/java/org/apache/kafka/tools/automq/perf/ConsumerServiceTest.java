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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import com.google.common.primitives.Longs;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.tools.automq.perf.ProducerService.HEADER_KEY_SEND_TIME_NANOS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@Tag("S3Unit")
class ConsumerServiceTest {

    /** Given ordinary records before and after benchmark traffic, retain only benchmark callbacks and statistics. */
    @Test
    void shouldProcessBenchmarkRecordsInMixedPoll() throws InterruptedException {
        TopicPartition partition = new TopicPartition("existing-topic", 0);
        ConsumerRecord<String, byte[]> first = benchmarkRecord(1, new byte[4], 100);
        ConsumerRecord<String, byte[]> second = benchmarkRecord(3, new byte[8], 200);
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(Map.of(partition, List.of(
            record(0, new byte[16]), first, record(2, null), second, record(4, new byte[32]))));
        ConsumerService.ConsumerCallback callback = mock(ConsumerService.ConsumerCallback.class);
        Stats stats = mock(Stats.class);

        ConsumerService.processRecords(records, callback, stats);

        verify(callback, times(2)).messageReceived(partition);
        verify(stats).messageReceived(2, 12, 100);
        verifyNoMoreInteractions(callback, stats);
    }

    /** Given missing, null, or malformed timestamp headers, skip each record without affecting the next benchmark record. */
    @Test
    void shouldSkipInvalidTimestampHeaders() throws InterruptedException {
        ConsumerRecord<String, byte[]> missing = record(0, new byte[8]);
        ConsumerRecord<String, byte[]> nullHeader = record(1, new byte[8]);
        nullHeader.headers().add(HEADER_KEY_SEND_TIME_NANOS, null);
        ConsumerRecord<String, byte[]> shortHeader = record(2, new byte[8]);
        shortHeader.headers().add(HEADER_KEY_SEND_TIME_NANOS, new byte[Long.BYTES - 1]);
        ConsumerRecord<String, byte[]> longHeader = record(3, new byte[8]);
        longHeader.headers().add(HEADER_KEY_SEND_TIME_NANOS, new byte[Long.BYTES + 1]);
        TopicPartition partition = new TopicPartition("existing-topic", 0);
        ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(Map.of(partition, List.of(
            missing, nullHeader, shortHeader, longHeader, benchmarkRecord(4, null, 300))));
        ConsumerService.ConsumerCallback callback = mock(ConsumerService.ConsumerCallback.class);
        Stats stats = mock(Stats.class);

        ConsumerService.processRecords(records, callback, stats);

        verify(callback).messageReceived(partition);
        verify(stats).messageReceived(1, 0, 300);
        verifyNoMoreInteractions(callback, stats);
    }

    /** Given an empty or ordinary-only poll, do not report benchmark progress or latency. */
    @Test
    void shouldIgnorePollWithoutBenchmarkRecords() throws InterruptedException {
        ConsumerService.ConsumerCallback callback = mock(ConsumerService.ConsumerCallback.class);
        Stats stats = mock(Stats.class);
        TopicPartition partition = new TopicPartition("existing-topic", 0);

        ConsumerService.processRecords(ConsumerRecords.empty(), callback, stats);
        ConsumerService.processRecords(new ConsumerRecords<>(Map.of(partition,
            List.of(record(0, new byte[4]), record(1, null)))), callback, stats);

        verifyNoInteractions(callback, stats);
    }

    private static ConsumerRecord<String, byte[]> record(long offset, byte[] value) {
        return new ConsumerRecord<>("existing-topic", 0, offset, "key", value);
    }

    private static ConsumerRecord<String, byte[]> benchmarkRecord(long offset, byte[] value, long sendTimeNanos) {
        ConsumerRecord<String, byte[]> record = record(offset, value);
        record.headers().add(HEADER_KEY_SEND_TIME_NANOS, Longs.toByteArray(sendTimeNanos));
        return record;
    }
}
