/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.metadata.stream;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3StreamEndOffsetsCodecTest {

    @Test
    public void test() {
        List<StreamEndOffset> offsets = List.of(new StreamEndOffset(1, 100), new StreamEndOffset(2, 200));
        byte[] bytes = S3StreamEndOffsetsCodec.encode(offsets);
        List<StreamEndOffset> decoded = new ArrayList<>();
        for (StreamEndOffset offset : S3StreamEndOffsetsCodec.decode(bytes)) {
            decoded.add(offset);
        }
        assertEquals(offsets, decoded);
    }
}
