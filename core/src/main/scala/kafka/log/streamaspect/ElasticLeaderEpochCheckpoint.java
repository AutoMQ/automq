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
