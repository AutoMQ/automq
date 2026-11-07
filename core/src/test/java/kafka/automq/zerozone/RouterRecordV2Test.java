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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(60)
@Tag("S3Unit")
public class RouterRecordV2Test {

    @Test
    public void test_encodeDecode() {
        int nodeId = 233;
        ByteBuf channelOffset1 = Unpooled.buffer();
        channelOffset1.writeInt(1);
        ByteBuf channelOffset2 = Unpooled.buffer();
        channelOffset2.writeInt(2);

        RouterRecordV2 record = RouterRecordV2.decode(new RouterRecordV2(nodeId, List.of(channelOffset1, channelOffset2)).encode());

        assertEquals(nodeId, record.nodeId());
        assertEquals(2, record.channelOffsets().size());
        assertEquals(4, record.channelOffsets().get(0).readableBytes());
        assertEquals(1, record.channelOffsets().get(0).readInt());
        assertEquals(4, record.channelOffsets().get(1).readableBytes());
        assertEquals(2, record.channelOffsets().get(1).readInt());
    }

}
