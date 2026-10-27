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

package kafka.automq.zerozone;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;

import com.automq.stream.s3.DefaultByteBufSupplier;
import com.automq.stream.s3.model.StreamRecordBatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies caller ownership and error preservation when a link cannot be decoded. */
@Tag("S3Unit")
public class DefaultLinkRecordDecoderTest {
    /** Synchronous and asynchronous channel failures preserve the input for the caller to release exactly once. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testChannelFailurePreservesSourceOwnership(boolean synchronousFailure) {
        RouterChannelProvider provider = mock(RouterChannelProvider.class);
        RouterChannel channel = mock(RouterChannel.class);
        when(provider.readOnlyChannel(1)).thenReturn(channel);
        RuntimeException failure = new IllegalStateException("missing RouterChannel object");
        if (synchronousFailure) {
            when(channel.get(any())).thenThrow(failure);
        } else {
            when(channel.get(any())).thenReturn(CompletableFuture.failedFuture(failure));
        }
        ChannelOffset offset = ChannelOffset.of((short) 0, (short) 0, 1, 1, Unpooled.EMPTY_BUFFER);
        ByteBuf link = LinkRecord.encode(offset, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[] {1})));
        StreamRecordBatch src = StreamRecordBatch.of(42, 0, 0, -1, link, DefaultByteBufSupplier.INSTANCE);
        offset.byteBuf().release();
        ByteBuf encoded = src.encoded();
        try {
            CompletionException exception = assertThrows(CompletionException.class, () ->
                new DefaultLinkRecordDecoder(provider).decode(src, DefaultByteBufSupplier.INSTANCE).join());
            assertSame(failure, exception.getCause());
            assertEquals(1, encoded.refCnt());
        } finally {
            src.release();
        }
        assertEquals(0, encoded.refCnt());
    }

    /** An invalid link header also leaves cleanup to its caller. */
    @Test
    public void testMalformedLinkPreservesSourceOwnership() {
        ByteBuf invalid = Unpooled.wrappedBuffer(new byte[] {0});
        StreamRecordBatch src = StreamRecordBatch.of(42, 0, 0, -1, invalid, DefaultByteBufSupplier.INSTANCE);
        try {
            assertThrows(CompletionException.class, () ->
                new DefaultLinkRecordDecoder(mock(RouterChannelProvider.class))
                    .decode(src, DefaultByteBufSupplier.INSTANCE).join());
            assertEquals(1, src.encoded().refCnt());
        } finally {
            src.release();
        }
    }
}
