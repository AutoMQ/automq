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

package com.automq.stream.s3.operator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("S3Unit")
class ObjectStorageListTest {
    private MemoryObjectStorage objectStorage;

    @BeforeEach
    void setUp() throws Exception {
        objectStorage = new MemoryObjectStorage();
        for (String key : List.of("archive/1/001", "archive/1/003", "archive/1/005", "archive/2/002")) {
            objectStorage.write(ObjectStorage.WriteOptions.DEFAULT, key,
                Unpooled.wrappedBuffer(key.getBytes(StandardCharsets.UTF_8))).get();
        }
    }

    @AfterEach
    void tearDown() {
        objectStorage.close();
    }

    /**
     * Given only the required prefix, then list options default to no cursor and an unlimited result bound.
     */
    @Test
    void testListOptionsDefaultsAndRequiredPrefix() {
        ObjectStorage.ListOptions options = new ObjectStorage.ListOptions("archive/1/");

        assertEquals("archive/1/", options.prefix());
        assertNull(options.startAfter());
        assertEquals(ObjectStorage.ListOptions.UNLIMITED, options.maxKeys());
        assertThrows(NullPointerException.class, () -> new ObjectStorage.ListOptions(null));
        assertThrows(IllegalArgumentException.class, () -> options.maxKeys(-2));
    }

    /**
     * Given objects in two prefixes, when listing one prefix, then only its objects are returned in key order.
     */
    @Test
    void testListIsPrefixIsolatedAndOrdered() throws Exception {
        assertEquals(List.of("archive/1/001", "archive/1/003", "archive/1/005"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/"))));
    }

    /**
     * Given cursors before, within, between, and after stored keys, then listing starts strictly after each cursor.
     */
    @Test
    void testListUsesExclusiveCursorAtAllPositions() throws Exception {
        assertEquals(List.of("archive/1/001", "archive/1/003", "archive/1/005"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").startAfter("archive/1/000"))));
        assertEquals(List.of("archive/1/003", "archive/1/005"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").startAfter("archive/1/001"))));
        assertEquals(List.of("archive/1/003", "archive/1/005"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").startAfter("archive/1/002"))));
        assertEquals(List.of("archive/1/005"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").startAfter("archive/1/003"))));
        assertEquals(List.of(),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").startAfter("archive/1/005"))));
    }

    /**
     * Given zero, unlimited, and positive bounds, then listing returns none, all, or at most the requested count.
     */
    @Test
    void testListHonorsResultBoundsAndLegacyUnlimitedBehavior() throws Exception {
        assertEquals(List.of(),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").maxKeys(0))));
        assertEquals(List.of("archive/1/001", "archive/1/003", "archive/1/005"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").maxKeys(-1))));
        assertEquals(List.of("archive/1/001", "archive/1/003"),
            keys(objectStorage.list(new ObjectStorage.ListOptions("archive/1/").maxKeys(2))));
        assertEquals(List.of("archive/1/001", "archive/1/003", "archive/1/005"),
            keys(objectStorage.list("archive/1/")));
    }

    private static List<String> keys(java.util.concurrent.CompletableFuture<List<ObjectStorage.ObjectInfo>> future)
        throws Exception {
        return future.get().stream().map(ObjectStorage.ObjectPath::key).toList();
    }
}
