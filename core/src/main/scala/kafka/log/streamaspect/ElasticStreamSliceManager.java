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
        // seal last slice with the same stream name when create new segment
        ElasticStreamSlice lastSlice = lastSlices.get(streamName);
        if (lastSlice != null) {
            lastSlice.seal();
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
