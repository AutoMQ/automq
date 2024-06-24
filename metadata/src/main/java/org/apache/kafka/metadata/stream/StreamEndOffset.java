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

package org.apache.kafka.metadata.stream;

import java.util.Objects;

public class StreamEndOffset {
    private final long streamId;
    private final long endOffset;

    public StreamEndOffset(long streamId, long endOffset) {
        this.streamId = streamId;
        this.endOffset = endOffset;
    }

    public long streamId() {
        return streamId;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StreamEndOffset that = (StreamEndOffset) o;
        return streamId == that.streamId && endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, endOffset);
    }
}
