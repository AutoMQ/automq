/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.metadata.stream;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
