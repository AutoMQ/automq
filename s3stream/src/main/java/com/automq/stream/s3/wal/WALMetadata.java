/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal;

public class WALMetadata {
    private final int nodeId;
    private final long epoch;

    public WALMetadata(int nodeId, long epoch) {
        this.nodeId = nodeId;
        this.epoch = epoch;
    }

    public int nodeId() {
        return nodeId;
    }

    public long epoch() {
        return epoch;
    }
}
