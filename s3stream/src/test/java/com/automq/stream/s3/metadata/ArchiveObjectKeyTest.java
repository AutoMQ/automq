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

package com.automq.stream.s3.metadata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
class ArchiveObjectKeyTest {

    /**
     * Given canonical Archive fields, when a key is generated and parsed, then every encoded field round-trips and
     * the lookup cursor sorts after every key whose end offset equals the requested offset.
     */
    @Test
    public void testManifestKeyRoundTripAndOrderedCursor() {
        String key = ArchiveObjectKey.manifestKey(7L, 10L, 20L, 101L, 999L);

        assertEquals(new ArchiveObjectKey.ManifestKey(7L, 10L, 20L,
                com.automq.stream.s3.objects.ObjectAttributes.Type.Composite, 101L, 999L),
            ArchiveObjectKey.parseManifestKey(key));
        assertEquals("70000000/" + ObjectUtils.getNamespace() + "/archive/7/",
            ArchiveObjectKey.manifestPrefix(7L));
        assertTrue(ArchiveObjectKey.startAfter(7L, 20L).compareTo(key) > 0);
        String normalKey = ArchiveObjectKey.manifestKey(7L, 10L, 20L,
            com.automq.stream.s3.objects.ObjectAttributes.Type.Normal, 101L, 123L);
        assertEquals(123L, ArchiveObjectKey.parseManifestKey(normalKey).objectSize());
        assertTrue(ArchiveObjectKey.isArchiveKey(key));
        assertFalse(ArchiveObjectKey.isArchiveKey("not-an-archive-key"));
    }

    /**
     * Given keys outside the canonical namespace or with invalid ranges, when parsed, then they are rejected before
     * Fetch can construct physical metadata.
     */
    @Test
    public void testMalformedManifestKeysAreRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> ArchiveObjectKey.parseManifestKey(
                "70000000/" + ObjectUtils.getNamespace() + "/archive/7/not-a-manifest"));
        assertThrows(IllegalArgumentException.class,
            () -> ArchiveObjectKey.parseManifestKey(
                "70000000/" + ObjectUtils.getNamespace()
                    + "/archive/7/0000000000000000010-0000000000000000010-1-101-999"));
        assertThrows(IllegalArgumentException.class,
            () -> ArchiveObjectKey.parseManifestKey(
                "70000000/" + ObjectUtils.getNamespace()
                    + "/archive/07/0000000000000000020-0000000000000000010-1-101-999"));
    }
}
