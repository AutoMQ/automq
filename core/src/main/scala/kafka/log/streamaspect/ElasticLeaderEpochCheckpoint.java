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

import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.log.EpochEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

// TODO: better implementation, limit the partition meta logic in partition
class ElasticLeaderEpochCheckpoint extends LeaderEpochCheckpointFile {
    private final ElasticLeaderEpochCheckpointMeta meta;
    private final Consumer<ElasticLeaderEpochCheckpointMeta> saveFunc;

    public ElasticLeaderEpochCheckpoint(ElasticLeaderEpochCheckpointMeta meta,
        Consumer<ElasticLeaderEpochCheckpointMeta> saveFunc) {
        this.meta = meta;
        this.saveFunc = saveFunc;
    }

    @Override
    public synchronized void write(Collection<EpochEntry> epochs) {
        meta.setEntries(new ArrayList<>(epochs));
        saveFunc.accept(meta);
    }

    @Override
    public synchronized void writeIfDirExists(Collection<EpochEntry> epochs) {
        write(epochs);
    }

    @Override
    public synchronized List<EpochEntry> read() {
        return meta.entries();
    }
}