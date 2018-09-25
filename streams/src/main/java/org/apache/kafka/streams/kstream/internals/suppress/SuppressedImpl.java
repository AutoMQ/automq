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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;
import java.util.Objects;

public class SuppressedImpl<K> implements Suppressed<K> {
    private static final Duration DEFAULT_SUPPRESSION_TIME = Duration.ofMillis(Long.MAX_VALUE);
    private static final StrictBufferConfig DEFAULT_BUFFER_CONFIG = BufferConfig.unbounded();

    private final BufferConfig bufferConfig;
    private final Duration timeToWaitForMoreEvents;
    private final TimeDefinition<K> timeDefinition;
    private final boolean suppressTombstones;

    public SuppressedImpl(final Duration suppressionTime,
                          final BufferConfig bufferConfig,
                          final TimeDefinition<K> timeDefinition,
                          final boolean suppressTombstones) {
        this.timeToWaitForMoreEvents = suppressionTime == null ? DEFAULT_SUPPRESSION_TIME : suppressionTime;
        this.timeDefinition = timeDefinition == null ? (context, anyKey) -> context.timestamp() : timeDefinition;
        this.bufferConfig = bufferConfig == null ? DEFAULT_BUFFER_CONFIG : bufferConfig;
        this.suppressTombstones = suppressTombstones;
    }

    interface TimeDefinition<K> {
        long time(final ProcessorContext context, final K key);
    }

    TimeDefinition<K> getTimeDefinition() {
        return timeDefinition;
    }

    Duration getTimeToWaitForMoreEvents() {
        return timeToWaitForMoreEvents == null ? Duration.ZERO : timeToWaitForMoreEvents;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SuppressedImpl<?> that = (SuppressedImpl<?>) o;
        return Objects.equals(bufferConfig, that.bufferConfig) &&
            Objects.equals(getTimeToWaitForMoreEvents(), that.getTimeToWaitForMoreEvents()) &&
            Objects.equals(getTimeDefinition(), that.getTimeDefinition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferConfig, getTimeToWaitForMoreEvents(), getTimeDefinition());
    }

    @Override
    public String toString() {
        return "SuppressedImpl{" +
            ", bufferConfig=" + bufferConfig +
            ", timeToWaitForMoreEvents=" + timeToWaitForMoreEvents +
            ", timeDefinition=" + timeDefinition +
            '}';
    }

    boolean suppressTombstones() {
        return suppressTombstones;
    }
}
