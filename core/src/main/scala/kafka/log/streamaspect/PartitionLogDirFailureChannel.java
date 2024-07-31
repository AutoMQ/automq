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

package kafka.log.streamaspect;

import java.io.IOException;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

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
