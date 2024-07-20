/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */


package kafka.log.streamaspect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.log.EpochEntry;

// TODO: better implementation, limit the partition meta logic in partition
class ElasticLeaderEpochCheckpoint implements LeaderEpochCheckpoint {
    private final ElasticLeaderEpochCheckpointMeta meta;
    private final Consumer<ElasticLeaderEpochCheckpointMeta> saveFunc;

    public ElasticLeaderEpochCheckpoint(ElasticLeaderEpochCheckpointMeta meta,
        Consumer<ElasticLeaderEpochCheckpointMeta> saveFunc) {
        this.meta = meta;
        this.saveFunc = saveFunc;
    }

    @Override
    public synchronized void write(Collection<EpochEntry> epochs, boolean sync) {
        meta.setEntries(new ArrayList<>(epochs));
        saveFunc.accept(meta);
    }

    @Override
    public synchronized List<EpochEntry> read() {
        return meta.entries();
    }
}
