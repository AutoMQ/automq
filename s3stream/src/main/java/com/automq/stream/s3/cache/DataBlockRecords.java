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

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBlockRecords {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataBlockRecords.class);
    final AtomicInteger refCount = new AtomicInteger(1);
    private final List<BiConsumer<DataBlockRecords, Throwable>> listeners = new LinkedList<>();
    private List<StreamRecordBatch> records = Collections.emptyList();

    public void registerListener(BiConsumer<DataBlockRecords, Throwable> listener) {
        retain();
        listeners.add(listener);
    }

    public void complete(ObjectReader.DataBlockGroup dataBlockGroup, Throwable ex) {
        if (ex == null) {
            records = new ArrayList<>(dataBlockGroup.recordCount());
            try (CloseableIterator<StreamRecordBatch> it = dataBlockGroup.iterator()) {
                while (it.hasNext()) {
                    records.add(it.next());
                }
            } catch (Throwable e) {
                LOGGER.error("parse data block fail", e);
                records.forEach(StreamRecordBatch::release);
                ex = e;
            }
        }
        Throwable finalEx = ex;
        listeners.forEach(listener -> {
            try {
                listener.accept(this, finalEx);
            } catch (Throwable e) {
                release();
                LOGGER.error("DataBlockRecords fail to notify listener {}", listener, e);
            }
        });
    }

    public List<StreamRecordBatch> records() {
        return Collections.unmodifiableList(records);
    }

    void retain() {
        refCount.incrementAndGet();
    }

    void release() {
        if (refCount.decrementAndGet() == 0) {
            records.forEach(StreamRecordBatch::release);
        }
    }
}
