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
