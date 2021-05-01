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
package org.apache.kafka.snapshot;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.ReplicatedLog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public final class FileRawSnapshotWriter implements RawSnapshotWriter {
    private final Path tempSnapshotPath;
    private final FileChannel channel;
    private final OffsetAndEpoch snapshotId;
    private final Optional<ReplicatedLog> replicatedLog;
    private boolean frozen = false;

    private FileRawSnapshotWriter(
        Path tempSnapshotPath,
        FileChannel channel,
        OffsetAndEpoch snapshotId,
        Optional<ReplicatedLog> replicatedLog
    ) {
        this.tempSnapshotPath = tempSnapshotPath;
        this.channel = channel;
        this.snapshotId = snapshotId;
        this.replicatedLog = replicatedLog;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void append(UnalignedMemoryRecords records) {
        try {
            checkIfFrozen("Append");
            Utils.writeFully(channel, records.buffer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void append(MemoryRecords records) {
        try {
            checkIfFrozen("Append");
            Utils.writeFully(channel, records.buffer());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public void freeze() {
        try {
            checkIfFrozen("Freeze");

            channel.close();
            frozen = true;

            if (!tempSnapshotPath.toFile().setReadOnly()) {
                throw new IllegalStateException(String.format("Unable to set file (%s) as read-only", tempSnapshotPath));
            }

            Path destination = Snapshots.moveRename(tempSnapshotPath, snapshotId);
            Utils.atomicMoveWithFallback(tempSnapshotPath, destination);

            replicatedLog.ifPresent(log -> log.onSnapshotFrozen(snapshotId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            // This is a noop if freeze was called before calling close
            Files.deleteIfExists(tempSnapshotPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return String.format(
            "FileRawSnapshotWriter(path=%s, snapshotId=%s, frozen=%s)",
            tempSnapshotPath,
            snapshotId,
            frozen
        );
    }

    void checkIfFrozen(String operation) {
        if (frozen) {
            throw new IllegalStateException(
                String.format(
                    "%s is not supported. Snapshot is already frozen: id = %s; temp path = %s",
                    operation,
                    snapshotId,
                    tempSnapshotPath
                )
            );
        }
    }

    /**
     * Create a snapshot writer for topic partition log dir and snapshot id.
     *
     * @param logDir the directory for the topic partition
     * @param snapshotId the end offset and epoch for the snapshotId
     * @throws IOException for any IO error while creating the snapshot
     */
    public static FileRawSnapshotWriter create(
        Path logDir,
        OffsetAndEpoch snapshotId,
        Optional<ReplicatedLog> replicatedLog
    ) {
        try {
            Path path = Snapshots.createTempFile(logDir, snapshotId);

            return new FileRawSnapshotWriter(
                path,
                FileChannel.open(path, Utils.mkSet(StandardOpenOption.WRITE, StandardOpenOption.APPEND)),
                snapshotId,
                replicatedLog
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
