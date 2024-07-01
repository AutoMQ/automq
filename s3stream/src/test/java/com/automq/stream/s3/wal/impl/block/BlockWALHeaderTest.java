/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
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
        assertEquals(header.getCapacity(), unmarshal.getCapacity());
        assertEquals(header.getTrimOffset(), unmarshal.getTrimOffset());
        assertEquals(header.getLastWriteTimestamp(), unmarshal.getLastWriteTimestamp());
        assertEquals(header.getSlidingWindowMaxLength(), unmarshal.getSlidingWindowMaxLength());
        assertEquals(header.getShutdownType(), unmarshal.getShutdownType());
        assertEquals(header.getNodeId(), unmarshal.getNodeId());
        assertEquals(header.getEpoch(), unmarshal.getEpoch());
    }

}
