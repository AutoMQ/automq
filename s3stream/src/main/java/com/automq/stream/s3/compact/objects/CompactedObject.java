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

package com.automq.stream.s3.compact.objects;

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
