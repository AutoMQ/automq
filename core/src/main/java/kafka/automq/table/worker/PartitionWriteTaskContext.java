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

package kafka.automq.table.worker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

class PartitionWriteTaskContext {
    boolean requireReset;
    CompletableFuture<Void> lastFlushCf;

    final Writer writer;
    final EventLoops.EventLoopRef eventLoop;
    final ExecutorService flushExecutors;
    final WorkerConfig config;
    final long priority;
    long writeSize = 0;

    public PartitionWriteTaskContext(Writer writer, EventLoops.EventLoopRef eventLoop, ExecutorService flushExecutors, WorkerConfig config, long priority) {
        this.requireReset = false;
        this.lastFlushCf = CompletableFuture.completedFuture(null);

        this.writer = writer;
        this.eventLoop = eventLoop;
        this.flushExecutors = flushExecutors;
        this.config = config;
        this.priority = priority;
    }

    public void recordWriteSize(long size) {
        writeSize += size;
    }
}
