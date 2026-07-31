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

package com.automq.stream.api;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class ReadOptionsTest {

    /**
     * Given a reused builder, when options are built before and after a builder update, then the first value remains unchanged.
     */
    @Test
    void testBuilderCreatesImmutableValues() {
        ReadOptions.Builder builder = ReadOptions.builder().fastRead(true);

        ReadOptions first = builder.build();
        ReadOptions second = builder.pooledBuf(true).build();

        assertTrue(first.fastRead());
        assertFalse(first.pooledBuf());
        assertTrue(second.fastRead());
        assertTrue(second.pooledBuf());
        assertNotSame(first, second);
    }

    /**
     * Given default read options, when snapshot-read mode is derived, then the shared default remains unchanged.
     */
    @Test
    void testToBuilderCopiesOptions() {
        ReadOptions original = ReadOptions.builder()
            .fastRead(true)
            .pooledBuf(true)
            .prioritizedRead(true)
            .build();

        ReadOptions derived = original.toBuilder().snapshotRead(true).build();

        assertFalse(original.snapshotRead());
        assertFalse(ReadOptions.DEFAULT.snapshotRead());
        assertTrue(derived.fastRead());
        assertTrue(derived.pooledBuf());
        assertTrue(derived.prioritizedRead());
        assertTrue(derived.snapshotRead());
        assertNotSame(original, derived);
    }

}
