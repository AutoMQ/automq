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

import java.io.IOException;

public class DefaultStreamSliceSupplier implements StreamSliceSupplier {
    private final ElasticStreamSliceManager streamSliceManager;
    private final String streamName;
    private final SliceRange sliceRange;

    public DefaultStreamSliceSupplier(ElasticStreamSliceManager streamSliceManager, String streamName, SliceRange sliceRange) {
        this.streamSliceManager = streamSliceManager;
        this.streamName = streamName;
        this.sliceRange = sliceRange;
    }

    @Override
    public ElasticStreamSlice get() throws IOException {
        return streamSliceManager.loadOrCreateSlice(streamName, sliceRange);
    }

    /**
     * reset the slice to an open empty slice. This is used in segment index recovery.
     *
     * @return a new open empty slice
     */
    @Override
    public ElasticStreamSlice reset() throws IOException {
        return streamSliceManager.newSlice(streamName);
    }

}
