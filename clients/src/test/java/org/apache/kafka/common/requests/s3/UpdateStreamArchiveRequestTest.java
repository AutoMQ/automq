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

package org.apache.kafka.common.requests.s3;

import org.apache.kafka.common.message.UpdateStreamArchiveRequestData;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.ArchivePrepare;
import org.apache.kafka.common.message.UpdateStreamArchiveRequestData.StreamArchiveOperation;
import org.apache.kafka.common.protocol.ApiKeys;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the version-zero Broker-owned Stream Archive wire contract.
 */
@Tag("S3Unit")
public class UpdateStreamArchiveRequestTest {

    /**
     * Given one typed Broker-owned operation, verify v0 is flexible and preserves its payload.
     */
    @Test
    public void testVersionZeroWireContract() {
        StreamArchiveOperation update = new StreamArchiveOperation()
            .setStreamId(1L)
            .setStreamEpoch(2L)
            .setOperation(StreamArchiveOperationType.ARCHIVE_PREPARE.value())
            .setArchivePrepare(new ArchivePrepare()
                .setExpectedArchiveEndOffset(5L)
                .setArchivePreparedEndOffset(6L)
                .setArchiveObjectIds(List.of(10L, 11L)));
        UpdateStreamArchiveRequestData data = new UpdateStreamArchiveRequestData()
            .setNodeId(12)
            .setNodeEpoch(13L)
            .setOperations(List.of(update));

        assertTrue(ApiKeys.UPDATE_STREAM_ARCHIVE.messageType.highestSupportedVersion(true) >= 0);
        assertEquals(data, new UpdateStreamArchiveRequest.Builder(data).build((short) 0).data());
        assertEquals(List.of(10L, 11L), data.operations().get(0).archivePrepare().archiveObjectIds());
    }
}
