/*
 * Copyright 2026, AutoMQ HK Limited.
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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class SampleReplayTest {
    @TempDir
    Path directory;

    /** Binary keys, nulls, empty fields and duplicate records survive both raw and gzip loading. */
    @Test
    void preservesBinaryRecords() throws Exception {
        byte[] bytes = records(new byte[][] {{0, (byte) 255}, {}, null, {0, (byte) 255}},
            new byte[][] {{1, 2}, {}, null, {1, 2}});
        for (boolean gzip : List.of(false, true)) {
            List<List<SampleRecords.Entry>> lanes = SampleRecords.read(sample(bytes, gzip), 1024).assign(2, null, 0);
            assertEquals(2, lanes.get(0).size());
            assertArrayEquals(new byte[] {0, (byte) 255}, lanes.get(0).get(0).key);
            assertArrayEquals(new byte[] {1, 2}, lanes.get(0).get(0).value);
            assertNull(lanes.get(0).get(1).key);
            assertNull(lanes.get(0).get(1).value);
            assertEquals(0, lanes.get(1).get(0).key.length);
            assertEquals(0, lanes.get(1).get(0).value.length);
            assertArrayEquals(lanes.get(0).get(0).key, lanes.get(1).get(1).key);
        }
    }

    /** Every truncated prefix, including a partial next length, is rejected; budgets apply after decompression. */
    @Test
    void rejectsMalformedAndOversizedSamples() throws Exception {
        byte[] bytes = records(new byte[][] {{1, 2}}, new byte[][] {{3, 4}});
        for (int i = 0; i < bytes.length; i++) {
            Path path = sample(Arrays.copyOf(bytes, i), false);
            assertThrows(IOException.class, () -> SampleRecords.read(path, 1024));
        }
        Path extraByte = sample(Arrays.copyOf(bytes, bytes.length + 1), false);
        assertThrows(IOException.class, () -> SampleRecords.read(extraByte, 1024));
        Path compressed = sample(bytes, true);
        assertThrows(IOException.class, () -> SampleRecords.read(compressed, 67));
        Path invalid = sample(new byte[] {(byte) 254, (byte) 255, (byte) 255, (byte) 255}, false);
        assertThrows(IOException.class, () -> SampleRecords.read(invalid, 1024));
        assertThrows(IllegalArgumentException.class, () -> SampleRecords.read(compressed, 0));
    }

    /** Timing rows retain separate producer order even when the combined file is not time sorted. */
    @Test
    void assignsIndependentProducerTimelines() throws Exception {
        SampleRecords sample = threeRecords();
        Path timing = timing("producer,offset_ns\n0,10\n1,0\n0,50\n");
        List<List<SampleRecords.Entry>> lanes = sample.assign(2, timing, 100);
        assertEquals(2, lanes.get(0).size());
        assertEquals(50, lanes.get(0).get(1).offsetNanos);
        assertEquals(0, lanes.get(1).get(0).offsetNanos);
        assertArrayEquals(new byte[] {2}, lanes.get(1).get(0).key);
    }

    /** Missing/extra rows, backwards times, out-of-cycle offsets and idle configured producers are invalid. */
    @Test
    void rejectsInvalidTimelines() throws Exception {
        SampleRecords sample = threeRecords();
        for (String rows : List.of(
            "producer,offset_ns\n0,0\n1,0\n",
            "producer,offset_ns\n0,0\n1,0\n0,50\n0,60\n",
            "producer,offset_ns\n0,50\n1,0\n0,10\n",
            "producer,offset_ns\n0,0\n1,0\n0,100\n",
            "producer,offset_ns\n0,0\n2,0\n0,50\n",
            "producer,offset_ns\n0,0\n0,10\n0,50\n",
            "producer,offset_ns\n0,-1\n1,0\n0,50\n",
            "producer,offset_ns\n0,0\n1,0\ninvalid\n",
            "wrong header\n0,0\n1,0\n0,50\n")) {
            Path timing = timing(rows);
            assertThrows(IOException.class, () -> sample.assign(2, timing, 100));
        }
        assertThrows(IllegalArgumentException.class, () -> sample.assign(4, null, 0));
    }

    /** Replay retains burst gaps and the final idle gap, scales speed, and avoids catch-up bursts after delays. */
    @Test
    void preservesTimingAndStretchesOnlyTheDelayedProducer() {
        List<SampleRecords.Entry> entries = List.of(new SampleRecords.Entry(null, null, 10), new SampleRecords.Entry(null, null, 50));
        ReplaySchedule first = new ReplaySchedule(entries, 100, 2, 1000);
        ReplaySchedule second = new ReplaySchedule(entries, 100, 2, 1000);
        assertEquals(1005, first.deadline());
        assertEquals(0, first.advance(1005));
        assertEquals(1025, first.deadline());
        assertEquals(75, first.advance(1100));
        assertEquals(1130, first.deadline());
        assertEquals(1005, second.deadline());
        assertThrows(IllegalArgumentException.class, () -> ReplaySchedule.scale(100, 0));
        assertThrows(IllegalArgumentException.class, () -> ReplaySchedule.scale(100, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> ReplaySchedule.scale(100, Double.MIN_VALUE));
    }

    /** Synthetic defaults stay unchanged; samples default to binary keyed partitioning and retain explicit overrides. */
    @Test
    void preservesProducerConfigurationDefaults() {
        Properties original = new Properties();
        Properties synthetic = ProducerService.producerProperties(new ProducerService.ProducersConfig("localhost:9092", 1, original));
        assertEquals(StringSerializer.class.getName(), synthetic.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(true, synthetic.get(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG));
        Properties sampled = ProducerService.producerProperties(new ProducerService.ProducersConfig("localhost:9092", 1, original, true, false));
        assertEquals(ByteArraySerializer.class.getName(), sampled.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(false, sampled.get(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG));
        assertTrue(original.isEmpty());
        original.put(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG, "false");
        assertEquals("false", ProducerService.producerProperties(new ProducerService.ProducersConfig("localhost:9092", 1, original))
            .get(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG));
    }

    /** Omitting the timestamp header retains exact key/value bytes and ACK callbacks; binary readiness probes still serialize. */
    @Test
    void optionalHeaderDoesNotChangeKeysOrAckAccounting() {
        for (boolean header : List.of(false, true)) {
            AtomicInteger callbacks = new AtomicInteger();
            MockProducer<Object, byte[]> client = new MockProducer<>(true, (topic, key) -> (byte[]) key, new ByteArraySerializer());
            try (ProducerService.Producer producer = new ProducerService.Producer(client, new TopicService.Topic("sample", 2),
                (size, time, error) -> {
                    assertNull(error);
                    callbacks.incrementAndGet();
                }, header, true)) {
                byte[] key = {(byte) 255, 0};
                byte[] value = {1, 2, 3};
                producer.sendAsync(key, value, null).join();
                assertArrayEquals(key, (byte[]) client.history().get(0).key());
                assertArrayEquals(value, client.history().get(0).value());
                assertEquals(header, client.history().get(0).headers().lastHeader("st") != null);
                producer.probe().forEach(future -> future.join());
                assertEquals(3, callbacks.get());
            }
        }
    }

    /** CLI rejects silently ignored or unsafe combinations while allowing timed warmup and optional headers. */
    @Test
    void validatesReplayOptions() {
        PerfConfig defaults = new PerfConfig(new String[0]);
        assertEquals(1000, defaults.sendRate);
        assertFalse(defaults.omitSendTimeHeader);
        PerfConfig timed = new PerfConfig(new String[] {"--sample-file", "records.gz", "--sample-timing-file", "timing.csv",
            "--sample-cycle-ms", "1000", "--sample-speed", "2", "--omit-send-time-header", "-g", "0"});
        assertEquals(1_000_000_000L, timed.sampleCycleNanos);
        assertTrue(timed.omitSendTimeHeader);
        assertNotNull(timed.sampleFile);
        for (String[] args : List.of(
            new String[] {"--omit-send-time-header"},
            new String[] {"--sample-file", "records.gz"},
            new String[] {"--sample-timing-file", "timing.csv"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--sample-cycle-ms", "1000"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--sample-timing-file", "timing.csv"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--send-throughput", "1"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--values-file", "values.json"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--sample-speed", "NaN"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--sample-max-bytes", "0"},
            new String[] {"--sample-file", "records.gz", "-g", "0", "--sample-timing-file", "timing.csv", "--sample-cycle-ms", "1000", "--send-rate", "1000"})) {
            assertThrows(IllegalArgumentException.class, () -> new PerfConfig(args));
        }
    }

    private SampleRecords threeRecords() throws IOException {
        return SampleRecords.read(sample(records(new byte[][] {{1}, {2}, {3}}, new byte[][] {{4}, {5}, {6}}), false), 1024);
    }

    private Path sample(byte[] bytes, boolean gzip) throws IOException {
        Path path = directory.resolve("sample.bin");
        if (gzip) {
            try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(path))) {
                out.write(bytes);
            }
        } else {
            Files.write(path, bytes);
        }
        return path;
    }

    private Path timing(String text) throws IOException {
        Path path = directory.resolve("timing.csv");
        Files.writeString(path, text, StandardCharsets.UTF_8);
        return path;
    }

    private byte[] records(byte[][] keys, byte[][] values) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            for (int i = 0; i < keys.length; i++) {
                for (byte[] field : new byte[][] {keys[i], values[i]}) {
                    out.writeInt(Integer.reverseBytes(field == null ? -1 : field.length));
                    if (field != null) {
                        out.write(field);
                    }
                }
            }
        }
        return bytes.toByteArray();
    }
}
