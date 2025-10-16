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

package kafka.cluster;

import kafka.log.streamaspect.ElasticLogMeta;

import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.TimestampOffset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PartitionSnapshot {
    private final int leaderEpoch;
    private final ElasticLogMeta logMeta;
    private final LogOffsetMetadata firstUnstableOffset;
    private final LogOffsetMetadata logEndOffset;
    private final Map<Long, Long> streamEndOffsets;
    private final TimestampOffset lastTimestampOffset;
    private final CompletableFuture<Void> completeCf;

    public PartitionSnapshot(int leaderEpoch, ElasticLogMeta meta, LogOffsetMetadata firstUnstableOffset, LogOffsetMetadata logEndOffset,
        Map<Long, Long> offsets, TimestampOffset lastTimestampOffset, CompletableFuture<Void> completeCf) {
        this.leaderEpoch = leaderEpoch;
        this.logMeta = meta;
        this.firstUnstableOffset = firstUnstableOffset;
        this.logEndOffset = logEndOffset;
        this.streamEndOffsets = offsets;
        this.lastTimestampOffset = lastTimestampOffset;
        this.completeCf = completeCf;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public ElasticLogMeta logMeta() {
        return logMeta;
    }

    public LogOffsetMetadata firstUnstableOffset() {
        return firstUnstableOffset;
    }

    public LogOffsetMetadata logEndOffset() {
        return logEndOffset;
    }

    public Map<Long, Long> streamEndOffsets() {
        return streamEndOffsets;
    }

    public TimestampOffset lastTimestampOffset() {
        return lastTimestampOffset;
    }

    public CompletableFuture<Void> completeCf() {
        return completeCf;
    }

    @Override
    public String toString() {
        return "PartitionSnapshot{" +
            "leaderEpoch=" + leaderEpoch +
            ", logMeta=" + logMeta +
            ", firstUnstableOffset=" + firstUnstableOffset +
            ", logEndOffset=" + logEndOffset +
            ", streamEndOffsets=" + streamEndOffsets +
            ", lastTimestampOffset=" + lastTimestampOffset +
            '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private static final int DEFAULT_STREAM_COUNT = 4;
        private int leaderEpoch;
        private ElasticLogMeta logMeta;
        private LogOffsetMetadata firstUnstableOffset;
        private LogOffsetMetadata logEndOffset;
        private final Map<Long, Long> streamEndOffsets = new HashMap<>(DEFAULT_STREAM_COUNT);
        private final List<CompletableFuture<?>> streamLastAppendFutures = new ArrayList<>(DEFAULT_STREAM_COUNT);
        private TimestampOffset lastTimestampOffset;

        public Builder leaderEpoch(int leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
            return this;
        }

        public Builder logMeta(ElasticLogMeta meta) {
            this.logMeta = meta;
            return this;
        }

        public Builder firstUnstableOffset(LogOffsetMetadata firstUnstableOffset) {
            this.firstUnstableOffset = firstUnstableOffset;
            return this;
        }

        public Builder logEndOffset(LogOffsetMetadata logEndOffset) {
            this.logEndOffset = logEndOffset;
            return this;
        }

        public Builder streamEndOffset(long streamId, long endOffset) {
            streamEndOffsets.put(streamId, endOffset);
            return this;
        }

        public Builder lastTimestampOffset(TimestampOffset lastTimestampOffset) {
            this.lastTimestampOffset = lastTimestampOffset;
            return this;
        }

        public Builder addStreamLastAppendFuture(CompletableFuture<?> future) {
            if (future != null) {
                streamLastAppendFutures.add(future);
            }
            return this;
        }

        public PartitionSnapshot build() {
            CompletableFuture<Void> doneCf = CompletableFuture.allOf(streamLastAppendFutures.toArray(new CompletableFuture<?>[0])).exceptionally(ex -> null);
            return new PartitionSnapshot(leaderEpoch, logMeta, firstUnstableOffset, logEndOffset, streamEndOffsets, lastTimestampOffset, doneCf);
        }
    }
}
