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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.DirectByteBufAlloc;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.utils.FutureUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.util.concurrent.CompletableFuture;

/**
 * If object data size is less than ObjectWriter.MAX_UPLOAD_SIZE, we should use single upload to upload it.
 * Else, we should use multi-part upload to upload it.
 */
class ProxyWriter implements Writer {
    private final S3Operator operator;
    private final String path;
    private final long minPartSize;
    private final ThrottleStrategy throttleStrategy;
    final ObjectWriter objectWriter = new ObjectWriter();
    Writer multiPartWriter = null;

    public ProxyWriter(S3Operator operator, String path, long minPartSize, ThrottleStrategy throttleStrategy) {
        this.operator = operator;
        this.path = path;
        this.minPartSize = minPartSize;
        this.throttleStrategy = throttleStrategy;
    }

    public ProxyWriter(S3Operator operator, String path, ThrottleStrategy throttleStrategy) {
        this(operator, path, MIN_PART_SIZE, throttleStrategy);
    }

    @Override
    public CompletableFuture<Void> write(ByteBuf part) {
        if (multiPartWriter != null) {
            return multiPartWriter.write(part);
        } else {
            if (objectWriter.isFull()) {
                newMultiPartWriter();
                return multiPartWriter.write(part);
            } else {
                return objectWriter.write(part);
            }
        }
    }

    @Override
    public void copyOnWrite() {
        if (multiPartWriter != null) {
            multiPartWriter.copyOnWrite();
        } else {
            objectWriter.copyOnWrite();
        }
    }

    @Override
    public void copyWrite(String sourcePath, long start, long end) {
        if (multiPartWriter == null) {
            newMultiPartWriter();
        }
        multiPartWriter.copyWrite(sourcePath, start, end);
    }

    @Override
    public boolean hasBatchingPart() {
        if (multiPartWriter != null) {
            return multiPartWriter.hasBatchingPart();
        } else {
            return objectWriter.hasBatchingPart();
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        if (multiPartWriter != null) {
            return multiPartWriter.close();
        } else {
            return objectWriter.close();
        }
    }

    @Override
    public CompletableFuture<Void> release() {
        if (multiPartWriter != null) {
            return multiPartWriter.release();
        } else {
            return objectWriter.release();
        }
    }

    private void newMultiPartWriter() {
        this.multiPartWriter = new MultiPartWriter(operator, path, minPartSize, throttleStrategy);
        if (objectWriter.data.readableBytes() > 0) {
            FutureUtil.propagate(multiPartWriter.write(objectWriter.data), objectWriter.cf);
        } else {
            objectWriter.data.release();
            objectWriter.cf.complete(null);
        }
    }

    class ObjectWriter implements Writer {
        // max upload size, when object data size is larger MAX_UPLOAD_SIZE, we should use multi-part upload to upload it.
        static final long MAX_UPLOAD_SIZE = 32L * 1024 * 1024;
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompositeByteBuf data = DirectByteBufAlloc.compositeByteBuffer();

        @Override
        public CompletableFuture<Void> write(ByteBuf part) {
            data.addComponent(true, part);
            return cf;
        }

        @Override
        public void copyOnWrite() {
            int size = data.readableBytes();
            if (size > 0) {
                ByteBuf buf = DirectByteBufAlloc.byteBuffer(size);
                buf.writeBytes(data.duplicate());
                CompositeByteBuf copy = DirectByteBufAlloc.compositeByteBuffer().addComponent(true, buf);
                this.data.release();
                this.data = copy;
            }
        }

        @Override
        public void copyWrite(String sourcePath, long start, long end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasBatchingPart() {
            return true;
        }

        @Override
        public CompletableFuture<Void> close() {
            FutureUtil.propagate(operator.write(path, data, throttleStrategy), cf);
            return cf;
        }

        @Override
        public CompletableFuture<Void> release() {
            data.release();
            return CompletableFuture.completedFuture(null);
        }

        public boolean isFull() {
            return data.readableBytes() > MAX_UPLOAD_SIZE;
        }
    }
}
