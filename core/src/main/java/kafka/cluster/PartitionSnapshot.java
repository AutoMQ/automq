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

package kafka.cluster;

import kafka.log.streamaspect.ElasticLogMeta;

import org.apache.kafka.storage.internals.log.LogOffsetMetadata;

import java.util.HashMap;
import java.util.Map;

public class PartitionSnapshot {
    private final ElasticLogMeta logMeta;
    private final LogOffsetMetadata lastUnstableOffset;
    private final LogOffsetMetadata logEndOffset;
    private final Map<Long, Long> streamEndOffsets;

    public PartitionSnapshot(ElasticLogMeta meta, LogOffsetMetadata lastUnstableOffset, LogOffsetMetadata logEndOffset,
        Map<Long, Long> offsets) {
        this.logMeta = meta;
        this.lastUnstableOffset = lastUnstableOffset;
        this.logEndOffset = logEndOffset;
        this.streamEndOffsets = offsets;
    }

    public ElasticLogMeta logMeta() {
        return logMeta;
    }

    public LogOffsetMetadata lastUnstableOffset() {
        return lastUnstableOffset;
    }

    public LogOffsetMetadata logEndOffset() {
        return logEndOffset;
    }

    public Map<Long, Long> streamEndOffsets() {
        return streamEndOffsets;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ElasticLogMeta logMeta;
        private LogOffsetMetadata lastUnstableOffset;
        private LogOffsetMetadata logEndOffset;
        private final Map<Long, Long> streamEndOffsets = new HashMap<>();

        public Builder logMeta(ElasticLogMeta meta) {
            this.logMeta = meta;
            return this;
        }

        public Builder lastUnstableOffset(LogOffsetMetadata lastUnstableOffset) {
            this.lastUnstableOffset = lastUnstableOffset;
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

        public PartitionSnapshot build() {
            return new PartitionSnapshot(logMeta, lastUnstableOffset, logEndOffset, streamEndOffsets);
        }
    }
}
