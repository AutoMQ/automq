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

package com.automq.stream.s3.compact.objects;

import com.automq.stream.s3.StreamDataBlock;
import java.util.List;

public class CompactedObject {
    private final CompactionType type;
    private final List<StreamDataBlock> streamDataBlocks;
    private final long size;

    public CompactedObject(CompactionType type, List<StreamDataBlock> streamDataBlocks) {
        this.type = type;
        this.streamDataBlocks = streamDataBlocks;
        this.size = streamDataBlocks.stream().mapToLong(StreamDataBlock::getBlockSize).sum();
    }

    public CompactionType type() {
        return type;
    }

    public List<StreamDataBlock> streamDataBlocks() {
        return this.streamDataBlocks;
    }

    public long size() {
        return size;
    }
}
