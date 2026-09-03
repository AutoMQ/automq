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

package com.automq.stream.s3.streams;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
class StreamArchiveStateTest {
    /**
     * Given durable Archive facts, when the phase is queried, then it identifies idle and both prepared states.
     */
    @Test
    void testDerivePhase() {
        StreamArchiveState idle = StreamArchiveState.builder()
            .archiveEndOffset(10L)
            .archivePreparedEndOffset(10L)
            .build();

        assertEquals(StreamArchivePhase.IDLE, idle.phase());
        assertEquals(StreamArchivePhase.ARCHIVE_PREPARED,
            idle.toBuilder().archivePreparedEndOffset(20L).build().phase());
        assertEquals(StreamArchivePhase.CLEANUP_PREPARED,
            idle.toBuilder().archiveCleanupSize(1L).build().phase());
    }

    /**
     * Given a complete Archive state, when one field is changed through toBuilder, then every other field is retained.
     */
    @Test
    void testToBuilderRetainsCompleteState() {
        StreamArchiveState initial = StreamArchiveState.builder()
            .streamId(1L)
            .streamEpoch(2L)
            .archiveStartOffset(3L)
            .archiveMetadataEndOffset(4L)
            .archiveEndOffset(5L)
            .archivePreparedEndOffset(6L)
            .archiveSize(7L)
            .archiveCleanupEndOffset(8L)
            .archiveCleanupSize(9L)
            .build();

        StreamArchiveState updated = initial.toBuilder().archiveEndOffset(12L).build();

        assertEquals(1L, updated.streamId());
        assertEquals(2L, updated.streamEpoch());
        assertEquals(3L, updated.archiveStartOffset());
        assertEquals(4L, updated.archiveMetadataEndOffset());
        assertEquals(12L, updated.archiveEndOffset());
        assertEquals(6L, updated.archivePreparedEndOffset());
        assertEquals(7L, updated.archiveSize());
        assertEquals(8L, updated.archiveCleanupEndOffset());
        assertEquals(9L, updated.archiveCleanupSize());
    }
}
