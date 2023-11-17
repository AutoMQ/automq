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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.ObjectReader;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Accumulate inflight data block read requests to one real read request.
 */
public class DataBlockReadAccumulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockReadAccumulator.class);
    private final Map<Pair<String, Integer>, DataBlockRecords> inflightDataBlockReads = new ConcurrentHashMap<>();

    public CompletableFuture<DataBlockRecords> readDataBlock(ObjectReader reader, ObjectReader.DataBlockIndex blockIndex) {
        CompletableFuture<DataBlockRecords> cf = new CompletableFuture<>();
        BiConsumer<DataBlockRecords, Throwable> listener = (rst, ex) -> {
            if (ex != null) {
                cf.completeExceptionally(ex);
            } else {
                cf.complete(rst);
            }
        };
        Pair<String, Integer> key = Pair.of(reader.objectKey(), blockIndex.blockId());
        synchronized (inflightDataBlockReads) {
            DataBlockRecords records = inflightDataBlockReads.get(key);
            if (records == null) {
                records = new DataBlockRecords();
                records.registerListener(listener);
                inflightDataBlockReads.put(key, records);
                DataBlockRecords finalRecords = records;
                reader.read(blockIndex).whenComplete((dataBlock, ex) -> {
                    try (dataBlock) {
                        synchronized (inflightDataBlockReads) {
                            inflightDataBlockReads.remove(key, finalRecords);
                        }
                        finalRecords.complete(dataBlock, ex);
                    } catch (Throwable e) {
                        LOGGER.error("[UNEXPECTED] DataBlockRecords fail to notify listener {}", listener, e);
                    } finally {
                        finalRecords.release();
                    }
                });
            } else {
                records.registerListener(listener);
            }
        }
        return cf;
    }
}
