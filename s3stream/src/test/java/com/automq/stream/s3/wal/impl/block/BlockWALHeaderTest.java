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

package com.automq.stream.s3.wal.impl.block;

import com.automq.stream.s3.wal.common.ShutdownType;
import com.automq.stream.s3.wal.exception.UnmarshalException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BlockWALHeaderTest {

    @Test
    public void test() throws UnmarshalException {
        BlockWALHeader header = new BlockWALHeader(128 * 1024, 100);
        header.updateTrimOffset(10);
        header.setLastWriteTimestamp(11);
        header.setShutdownType(ShutdownType.GRACEFULLY);
        header.setNodeId(233);
        header.setEpoch(234);

        BlockWALHeader unmarshal = BlockWALHeader.unmarshal(header.marshal().duplicate());
        assertEquals(1, unmarshal.version());
        assertEquals(header.getCapacity(), unmarshal.getCapacity());
        assertEquals(header.getTrimOffset(), unmarshal.getTrimOffset());
        assertEquals(header.getLastWriteTimestamp(), unmarshal.getLastWriteTimestamp());
        assertEquals(header.getSlidingWindowMaxLength(), unmarshal.getSlidingWindowMaxLength());
        assertEquals(header.getShutdownType(), unmarshal.getShutdownType());
        assertEquals(header.getNodeId(), unmarshal.getNodeId());
        assertEquals(header.getEpoch(), unmarshal.getEpoch());
    }

}
