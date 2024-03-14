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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.DataBlockIndex;
import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.StreamDataBlock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulate inflight data block read requests to one real read request.
 */
public class DataBlockReadAccumulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockReadAccumulator.class);
    private final Map<Pair<String, Long>, DataBlockRecords> inflightDataBlockReads = new ConcurrentHashMap<>();

    public List<ReserveResult> reserveDataBlock(List<Pair<ObjectReader, StreamDataBlock>> dataBlockPairList) {
        List<ReserveResult> reserveResults = new ArrayList<>();
        synchronized (inflightDataBlockReads) {
            for (Pair<ObjectReader, StreamDataBlock> pair : dataBlockPairList) {
                ObjectReader reader = pair.getLeft();
                DataBlockIndex blockIndex = pair.getRight().dataBlockIndex();
                Pair<String, Long> key = Pair.of(reader.objectKey(), blockIndex.startPosition());
                DataBlockRecords records = inflightDataBlockReads.get(key);
                CompletableFuture<DataBlockRecords> cf = new CompletableFuture<>();
                BiConsumer<DataBlockRecords, Throwable> listener = (rst, ex) -> {
                    if (ex != null) {
                        cf.completeExceptionally(ex);
                        rst.release();
                    } else {
                        // consumer of DataBlockRecords should release it on completion
                        cf.complete(rst);
                    }
                };
                int reservedSize = 0;
                if (records == null) {
                    records = new DataBlockRecords();
                    records.registerListener(listener);
                    inflightDataBlockReads.put(key, records);
                    reservedSize = blockIndex.size();
                } else {
                    records.registerListener(listener);
                }
                reserveResults.add(new ReserveResult(reservedSize, cf));
            }
        }
        return reserveResults;
    }

    public void readDataBlock(ObjectReader reader, DataBlockIndex blockIndex) {
        Pair<String, Long> key = Pair.of(reader.objectKey(), blockIndex.startPosition());
        synchronized (inflightDataBlockReads) {
            DataBlockRecords records = inflightDataBlockReads.get(key);
            if (records != null) {
                reader.read(blockIndex).whenComplete((dataBlock, ex) -> {
                    try (dataBlock) {
                        synchronized (inflightDataBlockReads) {
                            inflightDataBlockReads.remove(key, records);
                        }
                        records.complete(dataBlock, ex);
                    } finally {
                        records.release();
                    }
                });
            }
        }
    }

    public static final class ReserveResult {
        private final int reserveSize;
        private final CompletableFuture<DataBlockRecords> cf;

        public ReserveResult(int reserveSize, CompletableFuture<DataBlockRecords> cf) {
            this.reserveSize = reserveSize;
            this.cf = cf;
        }

        public int reserveSize() {
            return reserveSize;
        }

        public CompletableFuture<DataBlockRecords> cf() {
            return cf;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (ReserveResult) obj;
            return this.reserveSize == that.reserveSize &&
                Objects.equals(this.cf, that.cf);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reserveSize, cf);
        }

        @Override
        public String toString() {
            return "ReserveResult[" +
                "reserveSize=" + reserveSize + ", " +
                "cf=" + cf + ']';
        }

    }
}
