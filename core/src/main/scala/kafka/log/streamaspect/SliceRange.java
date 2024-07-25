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

import com.fasterxml.jackson.annotation.JsonProperty;

public class SliceRange {
    @JsonProperty("s")
    private long start = Offsets.NOOP_OFFSET;
    @JsonProperty("e")
    private long end = Offsets.NOOP_OFFSET;

    public SliceRange() {
    }

    public static SliceRange of(long start, long end) {
        SliceRange sliceRange = new SliceRange();
        sliceRange.start(start);
        sliceRange.end(end);
        return sliceRange;
    }

    public long start() {
        return start;
    }

    public void start(long start) {
        this.start = start;
    }

    public long end() {
        return end;
    }

    public void end(long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "[" + start + ", " + end + ']';
    }
}
