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

package com.automq.stream.s3.wal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WALHeaderTest {

    @Test
    public void test() throws UnmarshalException {
        WALHeader header = new WALHeader();
        header.setCapacity(128 * 1024);
        header.updateTrimOffset(10);
        header.setLastWriteTimestamp(11);
        header.setSlidingWindowNextWriteOffset(20);
        header.setSlidingWindowStartOffset(15);
        header.setSlidingWindowMaxLength(100);
        header.setShutdownType(ShutdownType.GRACEFULLY);
        header.setNodeId(233);
        header.setEpoch(234);

        WALHeader unmarshal = WALHeader.unmarshal(header.marshal().duplicate());
        assertEquals(header.getCapacity(), unmarshal.getCapacity());
        assertEquals(header.getTrimOffset(), unmarshal.getTrimOffset());
        assertEquals(header.getLastWriteTimestamp(), unmarshal.getLastWriteTimestamp());
        assertEquals(header.getSlidingWindowNextWriteOffset(), unmarshal.getSlidingWindowNextWriteOffset());
        assertEquals(header.getSlidingWindowStartOffset(), unmarshal.getSlidingWindowStartOffset());
        assertEquals(header.getSlidingWindowMaxLength(), unmarshal.getSlidingWindowMaxLength());
        assertEquals(header.getShutdownType(), unmarshal.getShutdownType());
        assertEquals(header.getNodeId(), unmarshal.getNodeId());
        assertEquals(header.getEpoch(), unmarshal.getEpoch());
    }

}
