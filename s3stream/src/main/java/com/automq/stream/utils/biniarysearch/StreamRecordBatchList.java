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

package com.automq.stream.utils.biniarysearch;

import com.automq.stream.s3.model.StreamRecordBatch;

import java.util.List;

public class StreamRecordBatchList extends AbstractOrderedCollection<Long> {

    private final List<StreamRecordBatch> records;
    private final int size;

    public StreamRecordBatchList(List<StreamRecordBatch> records) {
        this.records = records;
        this.size = records.size();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    protected ComparableItem<Long> get(int index) {
        return records.get(index);
    }

}
