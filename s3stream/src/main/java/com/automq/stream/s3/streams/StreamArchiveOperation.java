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

import java.util.List;

/**
 * One typed Broker command that advances a Stream Archive lifecycle.
 */
public interface StreamArchiveOperation {
    /**
     * Stream identity shared by every operation.
     */
    long streamId();

    /**
     * Stream owner epoch held by the submitting Broker.
     */
    long streamEpoch();

    /**
     * Freeze one consecutive online object range before copying it to Archive storage.
     */
    record ArchivePrepare(long streamId, long streamEpoch, long expectedArchiveEndOffset,
                          long archivePreparedEndOffset, List<Long> archiveObjectIds)
        implements StreamArchiveOperation {
        public ArchivePrepare {
            archiveObjectIds = List.copyOf(archiveObjectIds);
        }
    }

    /**
     * Publish one copied prepared range and its new absolute Archive size.
     */
    record ArchivePublish(long streamId, long streamEpoch, long expectedArchiveEndOffset,
                          long archiveEndOffset, long archiveSize) implements StreamArchiveOperation {
    }

    /**
     * Persist one retention-cleanup intent before deleting Archive objects.
     */
    record CleanupPrepare(long streamId, long streamEpoch, long expectedArchiveStartOffset,
                          long archiveCleanupEndOffset, long archiveCleanupSize) implements StreamArchiveOperation {
    }

    /**
     * Commit deletion of the currently prepared retention-cleanup range.
     */
    record CleanupCommit(long streamId, long streamEpoch, long expectedArchiveStartOffset,
                         long archiveCleanupEndOffset) implements StreamArchiveOperation {
    }

    /**
     * Move a fully drained Archive to a later online cursor after retention overtakes it.
     */
    record AdvanceEmptyCursor(long streamId, long streamEpoch, long expectedArchiveOffset,
                              long newArchiveOffset) implements StreamArchiveOperation {
    }
}
