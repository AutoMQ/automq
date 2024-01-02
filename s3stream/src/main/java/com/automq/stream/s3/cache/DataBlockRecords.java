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

    public void complete(ObjectReader.DataBlock dataBlock, Throwable ex) {
        if (ex == null) {
            records = new ArrayList<>(dataBlock.recordCount());
            try (CloseableIterator<StreamRecordBatch> it = dataBlock.iterator()) {
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
