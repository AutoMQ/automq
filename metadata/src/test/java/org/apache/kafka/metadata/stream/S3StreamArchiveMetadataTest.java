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

package org.apache.kafka.metadata.stream;

import com.automq.stream.s3.streams.StreamArchivePhase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
class S3StreamArchiveMetadataTest {
    /**
     * Given complete metadata, when a named transition is applied, then unrelated facts remain unchanged.
     */
    @Test
    void testNamedTransitionRetainsCompleteState() {
        S3StreamArchiveMetadata initial = new S3StreamArchiveMetadata(1L, 10L, 20L, 30L, 40L, 50L, 60L, 70L);

        S3StreamArchiveMetadata updated = initial.publishArchive(31L, 51L);

        assertEquals(new S3StreamArchiveMetadata(1L, 10L, 20L, 31L, 40L, 51L, 60L, 70L), updated);
        assertEquals(new S3StreamArchiveMetadata(1L, 10L, 20L, 30L, 40L, 50L, 60L, 70L), initial);
    }

    /**
     * Given complete KRaft Archive facts, when the phase is queried, then no persisted discriminator is required.
     */
    @Test
    void testDerivePhase() {
        S3StreamArchiveMetadata idle = S3StreamArchiveMetadata.defaultAt(1L, 10L);

        assertEquals(StreamArchivePhase.IDLE, idle.phase());
        assertEquals(StreamArchivePhase.ARCHIVE_PREPARED,
            new S3StreamArchiveMetadata(1L, 10L, 10L, 10L, 20L, 0L, 10L, 0L).phase());
        assertEquals(StreamArchivePhase.CLEANUP_PREPARED,
            new S3StreamArchiveMetadata(1L, 10L, 10L, 20L, 20L, 10L, 15L, 5L).phase());
    }
}
