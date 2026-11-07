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
