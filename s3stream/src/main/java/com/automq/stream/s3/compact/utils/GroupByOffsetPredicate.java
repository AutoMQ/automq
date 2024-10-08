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

public class GroupByOffsetPredicate implements Predicate<StreamDataBlock> {

    private long currStreamId = -1;
    private long nextStartOffset = 0;

    @Override
    public boolean test(StreamDataBlock block) {
        if (currStreamId == -1) {
            currStreamId = block.getStreamId();
            nextStartOffset = block.getEndOffset();
            return true;
        } else {
            if (currStreamId == block.getStreamId() && nextStartOffset == block.getStartOffset()) {
                nextStartOffset = block.getEndOffset();
                return true;
            } else {
                currStreamId = block.getStreamId();
                nextStartOffset = block.getEndOffset();
                return false;
            }
        }
    }
}
