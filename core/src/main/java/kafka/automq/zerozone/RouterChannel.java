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

package kafka.automq.zerozone;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

public interface RouterChannel {

    CompletableFuture<AppendResult> append(int targetNodeId, short orderHint, ByteBuf data);

    CompletableFuture<ByteBuf> get(ByteBuf channelOffset);

    void nextEpoch(long epoch);

    void trim(long epoch);

    void close();

    class AppendResult {
        private final long epoch;
        private final ByteBuf channelOffset;

        public AppendResult(long epoch, ByteBuf channelOffset) {
            this.epoch = epoch;
            this.channelOffset = channelOffset;
        }

        public long epoch() {
            return epoch;
        }

        public ByteBuf channelOffset() {
            return channelOffset;
        }
    }
}
