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

package kafka.log.es;

import sdk.elastic.stream.api.Stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static sdk.elastic.stream.utils.Arguments.check;

/**
 * Elastic log dimension stream segment manager.
 */
public class ElasticStreamSegmentManager {
    private final Map<String, ElasticStreamSegment> lastSegments = new ConcurrentHashMap<>();
    private final ElasticLogStreamManager streamManager;

    public ElasticStreamSegmentManager(ElasticLogStreamManager streamManager) {
        this.streamManager = streamManager;
    }

    public ElasticStreamSegment newSegment(String streamName) {
        // seal last segment with the same stream name when create new segment
        ElasticStreamSegment lastSegment = lastSegments.get(streamName);
        if (lastSegment != null) {
            lastSegment.seal();
        }
        Stream stream = streamManager.getStream(streamName);
        ElasticStreamSegment segment = new DefaultElasticStreamSegment(stream, stream.nextOffset(), -1L);
        lastSegments.put(streamName, segment);
        return segment;
    }

    /**
     * Load or create segment
     * - when startOffset is NOOP_OFFSET, then seal last active segment and create new segment.
     * - when startOffset != NOOP_OFFSET, then load segment.
     */
    public ElasticStreamSegment loadOrCreateSegment(String streamName, long startOffset, long endOffset) {
        check(startOffset >= Offsets.NOOP_OFFSET, "startOffset must be >= 0 or == NOOP_OFFSET -1");
        check(endOffset >= Offsets.NOOP_OFFSET, "endOffset must be >= 0 or == NOOP_OFFSET -1");
        if (startOffset == Offsets.NOOP_OFFSET) {
            return newSegment(streamName);
        }
        return new DefaultElasticStreamSegment(streamManager.getStream(streamName), startOffset, endOffset);
    }
}
