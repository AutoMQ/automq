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

package com.automq.stream.s3.cache;

import com.automq.stream.s3.model.StreamRecordBatch;
import java.util.List;
import java.util.OptionalLong;

public class ReadDataBlock {
    private final CacheAccessType cacheAccessType;
    private List<StreamRecordBatch> records;

    public ReadDataBlock(List<StreamRecordBatch> records, CacheAccessType cacheAccessType) {
        this.records = records;
        this.cacheAccessType = cacheAccessType;
    }

    public CacheAccessType getCacheAccessType() {
        return cacheAccessType;
    }

    public List<StreamRecordBatch> getRecords() {
        return records;
    }

    public void setRecords(List<StreamRecordBatch> records) {
        this.records = records;
    }

    public OptionalLong startOffset() {
        if (records.isEmpty()) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(records.get(0).getBaseOffset());
        }
    }

    public OptionalLong endOffset() {
        if (records.isEmpty()) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(records.get(records.size() - 1).getLastOffset());
        }
    }

    public int sizeInBytes() {
        return records.stream().mapToInt(StreamRecordBatch::size).sum();
    }
}
