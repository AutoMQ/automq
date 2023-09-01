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

package kafka.log.s3.wal;

import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryWriteAheadLog implements WriteAheadLog {
    private final AtomicLong offsetAlloc = new AtomicLong();

    @Override
    public long startPosition() {
        return 0;
    }

    @Override
    public long endPosition() {
        return 0;
    }

    @Override
    public List<WalRecord> read() {
        return Collections.emptyList();
    }

    @Override
    public AppendResult append(ByteBuf data) {
        AppendResult appendResult = new AppendResult();
        appendResult.endPosition = offsetAlloc.getAndIncrement();
        appendResult.future = CompletableFuture.completedFuture(null);
        return appendResult;
    }

    @Override
    public void trim(long position) {

    }
}
