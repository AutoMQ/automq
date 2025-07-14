/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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
