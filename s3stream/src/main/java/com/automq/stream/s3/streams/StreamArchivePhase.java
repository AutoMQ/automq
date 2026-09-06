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
 * Mutually exclusive Broker-owned Stream Archive recovery phases derived from durable offset and size facts.
 * Controller metadata cleanup is orthogonal to these phases.
 */
public enum StreamArchivePhase {
    IDLE,
    ARCHIVE_PREPARED,
    CLEANUP_PREPARED;

    /**
     * Derive the recovery phase without persisting a redundant state discriminator.
     */
    public static StreamArchivePhase from(long archiveEndOffset, long archivePreparedEndOffset,
        long archiveCleanupSize) {
        if (archivePreparedEndOffset > archiveEndOffset) {
            return ARCHIVE_PREPARED;
        }
        if (archiveCleanupSize > 0) {
            return CLEANUP_PREPARED;
        }
        return IDLE;
    }
}
