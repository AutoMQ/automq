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

package org.apache.kafka.common.record;

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;

/**
 * A wrapper of {@link Records} which will run the specified release hook when {@link #release()} is called.
 */
public class PooledRecords extends AbstractRecords implements PooledResource, Records {

    private final Records records;
    private final Runnable releaseHook;

    public PooledRecords(Records records, Runnable releaseHook) {
        this.records = records;
        this.releaseHook = releaseHook;
    }

    @Override
    public void release() {
        releaseHook.run();
        if (records instanceof PooledResource) {
            ((PooledResource) records).release();
        }
    }

    @Override
    public int sizeInBytes() {
        return records.sizeInBytes();
    }

    @Override
    public Iterable<? extends RecordBatch> batches() {
        return records.batches();
    }

    @Override
    public AbstractIterator<? extends RecordBatch> batchIterator() {
        return records.batchIterator();
    }

    @Override
    public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
        return records.downConvert(toMagic, firstOffset, time);
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        return records.writeTo(channel, position, length);
    }
}
