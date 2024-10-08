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

package com.automq.stream.s3.compact.utils;

import com.automq.stream.s3.StreamDataBlock;

import java.util.function.Predicate;

public class GroupByLimitPredicate implements Predicate<StreamDataBlock> {
    private final long blockSizeThreshold;
    private long streamId = -1;
    private long startOffset = 0;
    private long nextStartOffset = 0;
    private int blockSize = 0;
    private int recordCnt = 0;

    public GroupByLimitPredicate(long blockSizeThreshold) {
        this.blockSizeThreshold = blockSizeThreshold;
    }

    @Override
    public boolean test(StreamDataBlock block) {
        boolean flag = true;
        if (streamId == -1 // first block
            || block.getStreamId() != streamId // iterate to next stream
            || block.getStartOffset() != nextStartOffset // block start offset is not continuous for same stream (unlikely to happen)
            || (long) blockSize + block.getBlockSize() >= blockSizeThreshold // group size exceeds threshold
            || (long) recordCnt + block.dataBlockIndex().recordCount() > Integer.MAX_VALUE // group record count exceeds int32
            || (block.getEndOffset() - startOffset) > Integer.MAX_VALUE) { // group delta offset exceeds int32

            if (streamId != -1) {
                flag = false;
            }

            streamId = block.getStreamId();
            startOffset = block.getStartOffset();
            blockSize = 0;
            recordCnt = 0;
        }

        nextStartOffset = block.getEndOffset();
        blockSize += block.getBlockSize();
        recordCnt += block.dataBlockIndex().recordCount();
        return flag;
    }
}
