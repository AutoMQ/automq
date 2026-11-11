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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Loads a bounded, reusable corpus of binary Kafka keys and values before a test starts.
 * Optional timing rows assign records to independent producer schedules; no payload decoding is performed.
 */
public final class SampleRecords {
    private static final int MAX_RECORDS = 1_000_000;
    private static final int RECORD_OVERHEAD_BYTES = 64;
    private final List<Entry> entries;

    private SampleRecords(List<Entry> entries) {
        this.entries = entries;
    }

    /**
     * Read little-endian length-prefixed key/value pairs, optionally gzip compressed.
     * A length of -1 represents null. The budget includes 64 bytes per record for bookkeeping;
     * it is an allocation guard, not a guarantee of total JVM heap usage.
     * Empty, truncated, oversized and malformed inputs fail before Kafka clients are created.
     */
    public static SampleRecords read(Path path, long maxBytes) throws IOException {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("Sample byte budget must be positive");
        }
        List<Entry> entries = new ArrayList<>();
        long remaining = maxBytes;
        try (InputStream file = Files.newInputStream(path);
             BufferedInputStream buffered = new BufferedInputStream(file)) {
            buffered.mark(2);
            boolean gzip = buffered.read() == 0x1f && buffered.read() == 0x8b;
            buffered.reset();
            try (DataInputStream in = new DataInputStream(gzip ? new GZIPInputStream(buffered) : buffered)) {
                while (true) {
                    int first = in.read();
                    if (first == -1) {
                        break;
                    }
                    if (entries.size() == MAX_RECORDS || remaining < RECORD_OVERHEAD_BYTES) {
                        throw new IOException("Sample exceeds its byte budget or 1000000 record limit");
                    }
                    remaining -= RECORD_OVERHEAD_BYTES;
                    int keyLength = first | in.readUnsignedByte() << 8 | in.readUnsignedByte() << 16 | in.readUnsignedByte() << 24;
                    byte[] key = readField(in, keyLength, remaining);
                    remaining -= key == null ? 0 : key.length;
                    byte[] value = readField(in, Integer.reverseBytes(in.readInt()), remaining);
                    remaining -= value == null ? 0 : value.length;
                    entries.add(new Entry(key, value, 0));
                }
            }
        } catch (EOFException e) {
            throw new IOException("Truncated sample at record " + entries.size(), e);
        }
        if (entries.isEmpty()) {
            throw new IOException("Sample contains no records");
        }
        return new SampleRecords(entries);
    }

    private static byte[] readField(DataInputStream in, int length, long remaining) throws IOException {
        if (length < -1 || length > remaining) {
            throw new IOException("Invalid sample field length or exhausted byte budget");
        }
        if (length == -1) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    /**
     * Assign records to producers, retaining their order within each producer.
     * Without timing, assignments are round-robin over records, not Kafka partitions.
     * Timing CSV has a producer,offset_ns header and exactly one row per binary record.
     * Each producer must have a nonempty, nondecreasing schedule inside the supplied cycle.
     */
    public List<List<Entry>> assign(int producerCount, Path timingPath, long cycleNanos) throws IOException {
        if (producerCount <= 0 || producerCount > entries.size()) {
            throw new IllegalArgumentException("Producer count must be between 1 and the sample record count");
        }
        List<List<Entry>> assigned = new ArrayList<>();
        for (int i = 0; i < producerCount; i++) {
            assigned.add(new ArrayList<>());
        }
        if (timingPath == null) {
            for (int i = 0; i < entries.size(); i++) {
                assigned.get(i % producerCount).add(entries.get(i));
            }
        } else {
            readTiming(timingPath, cycleNanos, assigned);
        }
        for (List<Entry> producer : assigned) {
            if (producer.isEmpty()) {
                throw new IOException("Every configured producer must have at least one sample record");
            }
        }
        return assigned.stream().map(Collections::unmodifiableList).collect(Collectors.toList());
    }

    private void readTiming(Path path, long cycleNanos, List<List<Entry>> assigned) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            if (!"producer,offset_ns".equals(reader.readLine())) {
                throw new IOException("Timing CSV must start with producer,offset_ns");
            }
            for (int i = 0; i < entries.size(); i++) {
                String line = reader.readLine();
                try {
                    if (line == null) {
                        throw new IllegalArgumentException("missing row");
                    }
                    String[] fields = line.split(",", -1);
                    if (fields.length != 2) {
                        throw new IllegalArgumentException("expected two fields");
                    }
                    int producer = Integer.parseInt(fields[0].trim());
                    long offset = Long.parseLong(fields[1].trim());
                    if (producer < 0 || producer >= assigned.size() || offset < 0 || offset >= cycleNanos) {
                        throw new IllegalArgumentException("producer or offset out of range");
                    }
                    List<Entry> lane = assigned.get(producer);
                    if (!lane.isEmpty() && offset < lane.get(lane.size() - 1).offsetNanos) {
                        throw new IllegalArgumentException("offsets decrease within producer");
                    }
                    Entry entry = entries.get(i);
                    lane.add(new Entry(entry.key, entry.value, offset));
                } catch (IllegalArgumentException e) {
                    throw new IOException("Invalid timing row " + (i + 2), e);
                }
            }
            if (reader.readLine() != null) {
                throw new IOException("Timing CSV has more rows than the sample");
            }
        }
    }

    /** An immutable record descriptor; callers must not modify the shared key/value arrays. */
    public static final class Entry {
        final byte[] key;
        final byte[] value;
        final long offsetNanos;

        Entry(byte[] key, byte[] value, long offsetNanos) {
            this.key = key;
            this.value = value;
            this.offsetNanos = offsetNanos;
        }
    }
}
