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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BlockBatch {

    private final Collection<Block> blocks;
    private final long startOffset;
    private final long endOffset;

    public BlockBatch(Collection<Block> blocks) {
        assert !blocks.isEmpty();
        this.blocks = blocks;
        this.startOffset = blocks.stream()
                .map(Block::startOffset)
                .min(Long::compareTo)
                .orElseThrow();
        this.endOffset = blocks.stream()
                .map(b -> b.startOffset() + b.size())
                .max(Long::compareTo)
                .orElseThrow();
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public Collection<Block> blocks() {
        return Collections.unmodifiableCollection(blocks);
    }

    public List<CompletableFuture<WriteAheadLog.AppendResult.CallbackResult>> futures() {
        return blocks.stream()
                .map(Block::futures)
                .flatMap(List::stream)
                .toList();

    }

    public void release() {
        blocks.forEach(Block::release);
    }
}
