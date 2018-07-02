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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Map;

/**
 * The window specification interface for fixed size windows that is used to define window boundaries and window
 * maintain duration.
 * <p>
 * If not explicitly specified, the default maintain duration is 1 day.
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @param <W> type of the window instance
 * @see TimeWindows
 * @see UnlimitedWindows
 * @see JoinWindows
 * @see SessionWindows
 * @see TimestampExtractor
 */
public abstract class Windows<W extends Window> {

    private long maintainDurationMs = 24 * 60 * 60 * 1000L; // default: one day
    @Deprecated public int segments = 3;

    protected Windows() {}

    /**
     * Set the window maintain duration (retention time) in milliseconds.
     * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
     *
     * @param durationMs the window retention time in milliseconds
     * @return itself
     * @throws IllegalArgumentException if {@code durationMs} is negative
     */
    // This should always get overridden to provide the correct return type and thus to avoid a cast
    public Windows<W> until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < 0) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be negative.");
        }
        maintainDurationMs = durationMs;

        return this;
    }

    /**
     * Return the window maintain duration (retention time) in milliseconds.
     *
     * @return the window maintain duration
     */
    public long maintainMs() {
        return maintainDurationMs;
    }

    /**
     * Return the segment interval in milliseconds.
     *
     * @return the segment interval
     */
    @SuppressWarnings("deprecation") // The deprecation is on the public visibility of segments. We intend to make the field private later.
    public long segmentInterval() {
        // Pinned arbitrarily to a minimum of 60 seconds. Profiling may indicate a different value is more efficient.
        final long minimumSegmentInterval = 60_000L;
        // Scaled to the (possibly overridden) retention period
        return Math.max(maintainMs() / (segments - 1), minimumSegmentInterval);
    }

    /**
     * Set the number of segments to be used for rolling the window store.
     * This function is not exposed to users but can be called by developers that extend this class.
     *
     * @param segments the number of segments to be used
     * @return itself
     * @throws IllegalArgumentException if specified segments is small than 2
     * @deprecated since 2.1 Override segmentInterval() instead.
     */
    @Deprecated
    protected Windows<W> segments(final int segments) throws IllegalArgumentException {
        if (segments < 2) {
            throw new IllegalArgumentException("Number of segments must be at least 2.");
        }
        this.segments = segments;

        return this;
    }

    /**
     * Create all windows that contain the provided timestamp, indexed by non-negative window start timestamps.
     *
     * @param timestamp the timestamp window should get created for
     * @return a map of {@code windowStartTimestamp -> Window} entries
     */
    public abstract Map<Long, W> windowsFor(final long timestamp);

    /**
     * Return the size of the specified windows in milliseconds.
     *
     * @return the size of the specified windows
     */
    public abstract long size();
}
