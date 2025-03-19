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

package com.automq.stream.s3.cache.blockcache;

import com.automq.stream.s3.ObjectReader;
import com.automq.stream.s3.ObjectWriter;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.metadata.S3ObjectType;
import com.automq.stream.s3.metadata.StreamOffsetRange;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.MemoryObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage;

import java.util.LinkedList;
import java.util.List;

import io.netty.buffer.ByteBuf;

public class MockObject {
    final S3ObjectMetadata metadata;
    final MemoryObjectStorage objectStorage;

    private MockObject(S3ObjectMetadata metadata, MemoryObjectStorage objectStorage) {
        this.metadata = metadata;
        this.objectStorage = objectStorage;
    }

    public S3ObjectMetadata metadata() {
        return metadata;
    }

    public ObjectReader objectReader() {
        return ObjectReader.reader(metadata, objectStorage);
    }

    public static Builder builder(long objectId, int blockSizeThreshold) {
        return new Builder(objectId, blockSizeThreshold);
    }

    public static class Builder {
        private final long objectId;
        private final List<StreamOffsetRange> offsetRanges = new LinkedList<>();
        private final MemoryObjectStorage operator = new MemoryObjectStorage();
        private final ObjectWriter writer;

        public Builder(long objectId, int blockSizeThreshold) {
            this.objectId = objectId;
            this.writer = new ObjectWriter.DefaultObjectWriter(objectId, operator, blockSizeThreshold, Integer.MAX_VALUE, new ObjectStorage.WriteOptions());
        }

        public Builder mockDelay(long delay) {
            operator.setDelay(delay);
            return this;
        }

        public Builder write(long streamId, List<StreamRecordBatch> records) {
            offsetRanges.add(new StreamOffsetRange(streamId, records.get(0).getBaseOffset(), records.get(records.size() - 1).getLastOffset()));
            writer.write(streamId, records);
            return this;
        }

        public MockObject build() {
            try {
                writer.close().get();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            ByteBuf buf = operator.get();
            S3ObjectMetadata metadata = new S3ObjectMetadata(objectId, S3ObjectType.STREAM_SET, offsetRanges,
                System.currentTimeMillis(), System.currentTimeMillis(), buf.readableBytes(), objectId);
            return new MockObject(metadata, operator);
        }

    }

}
