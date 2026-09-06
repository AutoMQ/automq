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
package org.apache.kafka.metadata.stream;

import org.apache.kafka.common.metadata.S3StreamArchiveRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import com.automq.stream.s3.streams.StreamArchivePhase;

/**
 * The complete durable Archive state for one Stream.
 */
public record S3StreamArchiveMetadata(
    long streamId,
    long archiveStartOffset,
    long archiveMetadataEndOffset,
    long archiveEndOffset,
    long archivePreparedEndOffset,
    long archiveSize,
    long archiveCleanupEndOffset,
    long archiveCleanupSize
) {
    /**
     * Derives the Broker-owned recovery phase from this complete durable state.
     */
    public StreamArchivePhase phase() {
        return StreamArchivePhase.from(archiveEndOffset, archivePreparedEndOffset, archiveCleanupSize);
    }

    /** Returns the state after durably preparing one Archive copy range. */
    public S3StreamArchiveMetadata prepareArchive(long preparedEndOffset) {
        return new S3StreamArchiveMetadata(streamId, archiveStartOffset, archiveMetadataEndOffset,
            archiveEndOffset, preparedEndOffset, archiveSize, archiveCleanupEndOffset, archiveCleanupSize);
    }

    /** Returns the state after publishing a prepared Archive copy range. */
    public S3StreamArchiveMetadata publishArchive(long endOffset, long size) {
        return new S3StreamArchiveMetadata(streamId, archiveStartOffset, archiveMetadataEndOffset,
            endOffset, archivePreparedEndOffset, size, archiveCleanupEndOffset, archiveCleanupSize);
    }

    /** Returns the state after durably preparing one Archive retention-cleanup range. */
    public S3StreamArchiveMetadata prepareCleanup(long cleanupEndOffset, long cleanupSize) {
        return new S3StreamArchiveMetadata(streamId, archiveStartOffset, archiveMetadataEndOffset,
            archiveEndOffset, archivePreparedEndOffset, archiveSize, cleanupEndOffset, cleanupSize);
    }

    /** Returns the state after committing the currently prepared retention cleanup. */
    public S3StreamArchiveMetadata commitCleanup() {
        return new S3StreamArchiveMetadata(streamId, archiveCleanupEndOffset, archiveMetadataEndOffset,
            archiveEndOffset, archivePreparedEndOffset, Math.subtractExact(archiveSize, archiveCleanupSize),
            archiveCleanupEndOffset, 0L);
    }

    /** Returns the fully drained state aligned to a later online object boundary. */
    public S3StreamArchiveMetadata advanceEmptyCursor(long offset) {
        return new S3StreamArchiveMetadata(streamId, offset, archiveMetadataEndOffset, offset, offset, 0L,
            offset, 0L);
    }

    /** Returns the state after Controller-owned online metadata cleanup advances. */
    public S3StreamArchiveMetadata advanceMetadataCleanup(long metadataEndOffset) {
        return new S3StreamArchiveMetadata(streamId, archiveStartOffset, metadataEndOffset,
            archiveEndOffset, archivePreparedEndOffset, archiveSize, archiveCleanupEndOffset, archiveCleanupSize);
    }

    /**
     * Creates the non-materialized default state at a Stream's current start offset.
     */
    public static S3StreamArchiveMetadata defaultAt(long streamId, long streamStartOffset) {
        return new S3StreamArchiveMetadata(
            streamId,
            streamStartOffset,
            streamStartOffset,
            streamStartOffset,
            streamStartOffset,
            0L,
            streamStartOffset,
            0L
        );
    }

    /**
     * Creates immutable Archive metadata from a complete KRaft record.
     */
    public static S3StreamArchiveMetadata fromRecord(S3StreamArchiveRecord record) {
        return new S3StreamArchiveMetadata(
            record.streamId(),
            record.archiveStartOffset(),
            record.archiveMetadataEndOffset(),
            record.archiveEndOffset(),
            record.archivePreparedEndOffset(),
            record.archiveSize(),
            record.archiveCleanupEndOffset(),
            record.archiveCleanupSize()
        );
    }

    /**
     * Serializes this complete state as a version-0 KRaft metadata record.
     */
    public ApiMessageAndVersion toRecord() {
        return new ApiMessageAndVersion(new S3StreamArchiveRecord()
            .setStreamId(streamId)
            .setArchiveStartOffset(archiveStartOffset)
            .setArchiveMetadataEndOffset(archiveMetadataEndOffset)
            .setArchiveEndOffset(archiveEndOffset)
            .setArchivePreparedEndOffset(archivePreparedEndOffset)
            .setArchiveSize(archiveSize)
            .setArchiveCleanupEndOffset(archiveCleanupEndOffset)
            .setArchiveCleanupSize(archiveCleanupSize), (short) 0);
    }

}
