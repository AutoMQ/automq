/*
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

import org.apache.kafka.common.message.CloseStreamsRequestData;
import org.apache.kafka.common.message.CloseStreamsRequestData.CloseStreamRequest;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the wire-compatibility contract for close-stream requests across legacy and V6 senders.
 */
@Tag("S3Unit")
public class CloseStreamsRequestTest {
    private static final short VERSION = 0;

    /**
     * Given bytes emitted by the legacy version 0 schema, the new reader uses the legacy end-offset sentinel.
     */
    @Test
    public void testLegacyRequestDefaultsEndOffset() {
        ByteBuffer buffer = ByteBuffer.allocate(64);
        ByteBufferAccessor writable = new ByteBufferAccessor(buffer);
        writable.writeInt(1);
        writable.writeLong(2L);
        writable.writeUnsignedVarint(2);
        writable.writeLong(3L);
        writable.writeLong(4L);
        writable.writeUnsignedVarint(0);
        writable.writeUnsignedVarint(0);
        buffer.flip();

        CloseStreamsRequest request = CloseStreamsRequest.parse(buffer, VERSION);

        assertEquals(-1L, request.data().closeStreamRequests().get(0).endOffset());
    }

    /**
     * Given a non-negative end offset, version 0 carries it as a nested flexible tag.
     */
    @Test
    public void testVersionZeroRoundTripsTaggedEndOffset() {
        CloseStreamsRequestData data = new CloseStreamsRequestData()
            .setNodeId(1)
            .setNodeEpoch(2L)
            .setCloseStreamRequests(List.of(new CloseStreamRequest()
                .setStreamId(3L)
                .setStreamEpoch(4L)
                .setEndOffset(5L)));

        CloseStreamsRequest decoded = CloseStreamsRequest.parse(MessageUtil.toByteBuffer(data, VERSION), VERSION);

        assertEquals(5L, decoded.data().closeStreamRequests().get(0).endOffset());
    }

    /**
     * Given a new version 0 request, a legacy flexible reader skips the unknown nested tag.
     */
    @Test
    public void testLegacyFlexibleReaderIgnoresEndOffsetTag() {
        CloseStreamsRequestData data = new CloseStreamsRequestData()
            .setNodeId(1)
            .setNodeEpoch(2L)
            .setCloseStreamRequests(List.of(new CloseStreamRequest()
                .setStreamId(3L)
                .setStreamEpoch(4L)
                .setEndOffset(5L)));
        ByteBufferAccessor legacyReader = new ByteBufferAccessor(MessageUtil.toByteBuffer(data, VERSION));

        assertEquals(1, legacyReader.readInt());
        assertEquals(2L, legacyReader.readLong());
        assertEquals(2, legacyReader.readUnsignedVarint());
        assertEquals(3L, legacyReader.readLong());
        assertEquals(4L, legacyReader.readLong());
        int taggedFieldCount = legacyReader.readUnsignedVarint();
        assertEquals(1, taggedFieldCount);
        for (int i = 0; i < taggedFieldCount; i++) {
            legacyReader.readUnsignedVarint();
            int size = legacyReader.readUnsignedVarint();
            legacyReader.readArray(size);
        }
        assertEquals(0, legacyReader.readUnsignedVarint());
        assertEquals(0, legacyReader.remaining());
    }
}
