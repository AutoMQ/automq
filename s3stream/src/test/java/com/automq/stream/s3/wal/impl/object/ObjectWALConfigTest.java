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

package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.wal.OpenMode;
import com.automq.stream.s3.wal.ReservationService;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies migration defaults and rejects ambiguous object-key layouts before opening a WAL. */
@Tag("S3Unit")
public class ObjectWALConfigTest {
    /** Existing builder, URI, and constructor callers keep legacy writes and gain 64-shard reads. */
    @Test
    public void testDefaults() {
        assertDefaults(ObjectWALConfig.builder().build());
        assertDefaults(ObjectWALConfig.builder().withURI("0@s3://wal").build());
        assertDefaults(new ObjectWALConfig("", ReservationService.NOOP, 250, 1024, 1024, 1, 1024,
            "cluster", 1001, 42, OpenMode.READ_WRITE, (short) 0, "rc"));
    }

    /** Sharding mode is independent of read-only bucket mode and WAL open mode. */
    @Test
    public void testURIOptions() {
        ObjectWALConfig config = ObjectWALConfig.builder()
            .withURI("7@s3://wal?shardCount=3&shardMode=rw&mode=r")
            .withOpenMode(OpenMode.READ_ONLY).build();
        assertEquals(3, config.shardCount());
        assertEquals("rw", config.shardMode());
        assertEquals(OpenMode.READ_ONLY, config.openMode());
        assertEquals(7, config.bucketId());
    }

    /** A non-power-of-two count and a single shard are valid; neither changes the offset format. */
    @ParameterizedTest
    @ValueSource(ints = {1, 3, 64, 256, 1024})
    public void testPositiveCounts(int count) {
        assertEquals(count, ObjectWALConfig.builder().withShardCount(count).build().shardCount());
    }

    /** Invalid or repeated URI options must fail instead of silently using a different key layout. */
    @ParameterizedTest
    @ValueSource(strings = {"shardCount=0", "shardCount=-1", "shardCount=no", "shardCount=1.5",
        "shardCount", "shardCount=", "shardCount=2147483648", "shardCount=64&shardCount=128", "shardMode", "shardMode=",
        "shardMode=w", "shardMode=RW", "shardMode=r&shardMode=rw"})
    public void testInvalidURIOptions(String option) {
        assertThrows(IllegalArgumentException.class, () -> ObjectWALConfig.builder().withURI("0@s3://wal?" + option));
    }

    /** Programmatic configuration enforces the same validation as URI configuration. */
    @Test
    public void testInvalidBuilderOptions() {
        assertThrows(IllegalArgumentException.class, () -> ObjectWALConfig.builder().withShardCount(0));
        assertThrows(IllegalArgumentException.class, () -> ObjectWALConfig.builder().withShardCount(-1));
        assertThrows(IllegalArgumentException.class, () -> ObjectWALConfig.builder().withShardMode(null));
        assertThrows(IllegalArgumentException.class, () -> ObjectWALConfig.builder().withShardMode("w"));
    }

    private static void assertDefaults(ObjectWALConfig config) {
        assertEquals(64, config.shardCount());
        assertEquals("r", config.shardMode());
    }
}
