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

import com.automq.stream.s3.wal.AppendResult;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class BlockBatch {
    private final Collection<Block> blocks;
    private final long startOffset;
    private final long endOffset;
    private final long blockBatchSize;

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
        this.blockBatchSize = blocks.stream()
            .mapToLong(Block::size)
            .sum();
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

    public long blockBatchSize(){
        return blockBatchSize;
    }

    public Iterator<CompletableFuture<AppendResult.CallbackResult>> futures() {
        return new Iterator<>() {
            private final Iterator<Block> blockIterator = blocks.iterator();
            private Iterator<CompletableFuture<AppendResult.CallbackResult>> futureIterator = blockIterator.next().futures().iterator();

            @Override
            public boolean hasNext() {
                if (futureIterator.hasNext()) {
                    return true;
                } else {
                    if (blockIterator.hasNext()) {
                        futureIterator = blockIterator.next().futures().iterator();
                        return hasNext();
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public CompletableFuture<AppendResult.CallbackResult> next() {
                return futureIterator.next();
            }
        };
    }

    public void release() {
        blocks.forEach(Block::release);
    }
}
