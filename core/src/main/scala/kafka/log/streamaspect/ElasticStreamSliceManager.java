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

package kafka.log.streamaspect;

import com.automq.stream.api.Stream;
import com.automq.stream.utils.Arguments;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Elastic log dimension stream segment manager.
 */
public class ElasticStreamSliceManager {
    private final Map<String, ElasticStreamSlice> lastSlices = new ConcurrentHashMap<>();
    private final ElasticLogStreamManager streamManager;

    public ElasticStreamSliceManager(ElasticLogStreamManager streamManager) {
        this.streamManager = streamManager;
    }

    public ElasticLogStreamManager getStreamManager() {
        return streamManager;
    }

    public ElasticStreamSlice newSlice(String streamName) throws IOException {
        // seal last segment with the same stream name when create new segment
        ElasticStreamSlice lastSegment = lastSlices.get(streamName);
        if (lastSegment != null) {
            lastSegment.seal();
        }
        Stream stream = streamManager.getStream(streamName);
        ElasticStreamSlice streamSlice = new DefaultElasticStreamSlice(stream, SliceRange.of(Offsets.NOOP_OFFSET, Offsets.NOOP_OFFSET));
        lastSlices.put(streamName, streamSlice);
        return streamSlice;
    }

    /**
     * Load or create slice
     * - when startOffset is NOOP_OFFSET, then seal last active slice and create new slice.
     * - when startOffset != NOOP_OFFSET, then load slice.
     */
    public ElasticStreamSlice loadOrCreateSlice(String streamName, SliceRange sliceRange) throws IOException {
        Arguments.check(sliceRange.start() >= Offsets.NOOP_OFFSET, "startOffset must be >= NOOP_OFFSET");
        Arguments.check(sliceRange.end() >= Offsets.NOOP_OFFSET, "endOffset must be >= NOOP_OFFSET");
        if (sliceRange.start() == Offsets.NOOP_OFFSET) {
            return newSlice(streamName);
        }
        return new DefaultElasticStreamSlice(streamManager.getStream(streamName), sliceRange);
    }
}
