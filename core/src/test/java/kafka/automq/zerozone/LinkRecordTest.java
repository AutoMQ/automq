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

import com.automq.stream.s3.wal.impl.DefaultRecordOffset;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LinkRecordTest {

    @Test
    public void testEncodeDecode() {
        SimpleRecord record = new SimpleRecord(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords memoryRecords = MemoryRecords.withRecords(Compression.NONE, record);

        ByteBuf linkRecordBuf = LinkRecord.encode(
            ChannelOffset.of(
                (short) 1, (short) 2, 3, 4,
                DefaultRecordOffset.of(5, 6, 7).buffer()
            ),
            memoryRecords);

        assertEquals(7, LinkRecord.decodedSize(linkRecordBuf));
    }

}
