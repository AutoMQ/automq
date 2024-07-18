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

class IStreamSliceSupplier implements StreamSliceSupplier {
    final ElasticStreamSlice slice;

    public IStreamSliceSupplier(ElasticStreamSlice slice) {
        this.slice = slice;
    }

    @Override
    public ElasticStreamSlice get() throws IOException {
        return slice;
    }

    @Override
    public ElasticStreamSlice reset() throws IOException {
        return new DefaultElasticStreamSlice(new MemoryClient.StreamImpl(2), SliceRange.of(0, Offsets.NOOP_OFFSET));
    }
}
