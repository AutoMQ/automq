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

package com.automq.stream.api;

import com.automq.stream.s3.model.StreamRecordBatch;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

public interface LinkRecordDecoder {
    LinkRecordDecoder NOOP = new Noop();

    /**
     * Get the decoded record size
     */
    int decodedSize(ByteBuf linkRecordBuf);

    CompletableFuture<StreamRecordBatch> decode(StreamRecordBatch src);


    class Noop implements LinkRecordDecoder {

        @Override
        public int decodedSize(ByteBuf linkRecordBuf) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<StreamRecordBatch> decode(StreamRecordBatch src) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
