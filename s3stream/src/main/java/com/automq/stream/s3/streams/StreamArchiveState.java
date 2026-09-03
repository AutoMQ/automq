/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.streams;

/**
 * Complete Archive facts observed by a Broker for one Stream. New Broker-to-Controller mutations use
 * {@link StreamArchiveOperation}; this snapshot remains the read/recovery view. The metadata cleanup offset is
 * Controller-owned and read-only to Broker update operations.
 */
public record StreamArchiveState(
    long streamId,
    long streamEpoch,
    long archiveStartOffset,
    long archiveMetadataEndOffset,
    long archiveEndOffset,
    long archivePreparedEndOffset,
    long archiveSize,
    long archiveCleanupEndOffset,
    long archiveCleanupSize
) {
    /**
     * Broker recovery phase derived from the durable Archive facts.
     */
    public StreamArchivePhase phase() {
        return StreamArchivePhase.from(archiveEndOffset, archivePreparedEndOffset, archiveCleanupSize);
    }

    /**
     * Create an empty Archive state builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a builder initialized with this complete state.
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Builds a complete Broker-observed Archive state using named fields.
     */
    public static final class Builder {
        private long streamId;
        private long streamEpoch;
        private long archiveStartOffset;
        private long archiveMetadataEndOffset;
        private long archiveEndOffset;
        private long archivePreparedEndOffset;
        private long archiveSize;
        private long archiveCleanupEndOffset;
        private long archiveCleanupSize;

        private Builder() {
        }

        private Builder(StreamArchiveState state) {
            streamId = state.streamId;
            streamEpoch = state.streamEpoch;
            archiveStartOffset = state.archiveStartOffset;
            archiveMetadataEndOffset = state.archiveMetadataEndOffset;
            archiveEndOffset = state.archiveEndOffset;
            archivePreparedEndOffset = state.archivePreparedEndOffset;
            archiveSize = state.archiveSize;
            archiveCleanupEndOffset = state.archiveCleanupEndOffset;
            archiveCleanupSize = state.archiveCleanupSize;
        }

        /**
         * Set the Stream identity.
         */
        public Builder streamId(long streamId) {
            this.streamId = streamId;
            return this;
        }

        /**
         * Set the Stream owner epoch.
         */
        public Builder streamEpoch(long streamEpoch) {
            this.streamEpoch = streamEpoch;
            return this;
        }

        /**
         * Set the first retained Archive offset.
         */
        public Builder archiveStartOffset(long archiveStartOffset) {
            this.archiveStartOffset = archiveStartOffset;
            return this;
        }

        /**
         * Set the Controller-owned offset through which online object metadata has been reclaimed.
         */
        public Builder archiveMetadataEndOffset(long archiveMetadataEndOffset) {
            this.archiveMetadataEndOffset = archiveMetadataEndOffset;
            return this;
        }

        /**
         * Set the published Archive end offset.
         */
        public Builder archiveEndOffset(long archiveEndOffset) {
            this.archiveEndOffset = archiveEndOffset;
            return this;
        }

        /**
         * Set the end offset of the prepared Archive copy batch.
         */
        public Builder archivePreparedEndOffset(long archivePreparedEndOffset) {
            this.archivePreparedEndOffset = archivePreparedEndOffset;
            return this;
        }

        /**
         * Set the archived object-size accounting total.
         */
        public Builder archiveSize(long archiveSize) {
            this.archiveSize = archiveSize;
            return this;
        }

        /**
         * Set the end offset of the prepared retention-cleanup batch.
         */
        public Builder archiveCleanupEndOffset(long archiveCleanupEndOffset) {
            this.archiveCleanupEndOffset = archiveCleanupEndOffset;
            return this;
        }

        /**
         * Set the object-size accounting total of the prepared retention-cleanup batch.
         */
        public Builder archiveCleanupSize(long archiveCleanupSize) {
            this.archiveCleanupSize = archiveCleanupSize;
            return this;
        }

        /**
         * Build the complete Archive state.
         */
        public StreamArchiveState build() {
            return new StreamArchiveState(streamId, streamEpoch, archiveStartOffset, archiveMetadataEndOffset,
                archiveEndOffset, archivePreparedEndOffset, archiveSize, archiveCleanupEndOffset,
                archiveCleanupSize);
        }
    }
}
