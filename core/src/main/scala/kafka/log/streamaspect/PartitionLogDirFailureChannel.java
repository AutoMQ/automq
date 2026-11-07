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

package kafka.log.streamaspect;

import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

import java.io.IOException;

/**
 * PartitionLogDirFailureChannel will ignore the failed dir and only fail the partition.
 */
public class PartitionLogDirFailureChannel extends LogDirFailureChannel {
    private final LogDirFailureChannel inner;
    private final String path;
    private volatile boolean offline = false;

    public PartitionLogDirFailureChannel(LogDirFailureChannel logDirFailureChannel, String partitionPath) {
        super(1);
        this.inner = logDirFailureChannel;
        this.path = partitionPath;
    }

    @Override
    public boolean hasOfflineLogDir(String logDir) {
        return offline;
    }

    public void maybeAddOfflineLogDir(String logDir, String msg, IOException e) {
        offline = true;
        log.error(path + " " + msg, e);
        inner.maybeAddOfflineLogDir(path, msg, e);
    }

    public String takeNextOfflineLogDir() throws InterruptedException {
        return inner.takeNextOfflineLogDir();
    }

    public void clearOfflineLogDirRecord(String logDir) {
        offline = false;
        inner.clearOfflineLogDirRecord(path);
    }

}
