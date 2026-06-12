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

package kafka.automq.availability.action;

import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Broker-local owner of skipped read ranges. It affects fetch positioning only and never mutates stored log data.
 */
public class SkippedReadRangeRegistry {
    static final int DEFAULT_MAX_ENTRIES = 10000;

    private final int maxEntries;
    private final Map<RangeKey, Long> ranges;

    public SkippedReadRangeRegistry() {
        this(DEFAULT_MAX_ENTRIES);
    }

    public SkippedReadRangeRegistry(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
        this.ranges = new LinkedHashMap<>();
    }

    public synchronized void skip(TopicPartition topicPartition, long startOffset, long exclusiveEndOffset) {
        ranges.put(new RangeKey(topicPartition, startOffset), exclusiveEndOffset);
        while (ranges.size() > maxEntries) {
            ranges.remove(ranges.keySet().iterator().next());
        }
    }

    public synchronized long adjustStartOffset(TopicPartition topicPartition, long startOffset) {
        return ranges.getOrDefault(new RangeKey(topicPartition, startOffset), startOffset);
    }

    private static final class RangeKey {
        private final TopicPartition topicPartition;
        private final long startOffset;

        private RangeKey(TopicPartition topicPartition, long startOffset) {
            this.topicPartition = topicPartition;
            this.startOffset = startOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RangeKey)) {
                return false;
            }
            RangeKey rangeKey = (RangeKey) o;
            return startOffset == rangeKey.startOffset && Objects.equals(topicPartition, rangeKey.topicPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, startOffset);
        }
    }
}
